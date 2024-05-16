// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import com.digitalasset.canton.crypto.{CryptoPureApi, Fingerprint, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.AuthorizedIdentifierDelegation
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreId,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.TopologyMapping.RequiredAuthAuthorizations
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** common functionality shared between incoming transaction authorization validator and the auth computation */
trait TransactionAuthorizationValidator {

  this: NamedLogging =>

  protected val namespaceCache = new TrieMap[Namespace, AuthorizationGraph]()
  protected val identifierDelegationCache =
    new TrieMap[UniqueIdentifier, Set[AuthorizedIdentifierDelegation]]()
  protected val decentralizedNamespaceCache =
    new TrieMap[
      Namespace,
      (DecentralizedNamespaceDefinition, DecentralizedNamespaceAuthorizationGraph),
    ]()

  protected def store: TopologyStore[TopologyStoreId]

  protected def pureCrypto: CryptoPureApi

  def isCurrentlyAuthorized(
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
  )(implicit
      traceContext: TraceContext
  ): Either[
    TopologyTransactionRejection,
    (GenericSignedTopologyTransaction, RequiredAuthAuthorizations),
  ] = {
    // first determine all possible namespaces and uids that need to sign the transaction
    val requiredAuth = toValidate.mapping.requiredAuth(inStore.map(_.transaction))

    logger.debug(s"Required authorizations: $requiredAuth")

    val required = requiredAuth
      .foldMap(
        namespaceCheck = rns =>
          RequiredAuthAuthorizations(
            namespacesWithRoot =
              if (rns.requireRootDelegation) rns.namespaces else Set.empty[Namespace],
            namespaces = if (rns.requireRootDelegation) Set.empty[Namespace] else rns.namespaces,
          ),
        uidCheck = ruid => RequiredAuthAuthorizations(uids = ruid.uids, extraKeys = ruid.extraKeys),
      )

    val signingKeys = toValidate.signatures.map(_.signedBy)
    val namespaceWithRootAuthorizations =
      required.namespacesWithRoot.map { ns =>
        val check = getAuthorizationCheckForNamespace(ns)
        val keysWithDelegation = check.getValidAuthorizationKeys(
          signingKeys,
          requireRoot = true,
        )
        val keysAuthorizeNamespace =
          check.areValidAuthorizationKeys(signingKeys, requireRoot = true)
        (ns -> (keysAuthorizeNamespace, keysWithDelegation))
      }.toMap

    // Now let's determine which namespaces and uids actually delegated to any of the keys
    val namespaceAuthorizations = required.namespaces.map { ns =>
      val check = getAuthorizationCheckForNamespace(ns)
      val keysWithDelegation = check.getValidAuthorizationKeys(
        signingKeys,
        requireRoot = false,
      )
      val keysAuthorizeNamespace = check.areValidAuthorizationKeys(signingKeys, requireRoot = false)
      (ns -> (keysAuthorizeNamespace, keysWithDelegation))
    }.toMap

    val uidAuthorizations =
      required.uids.map { uid =>
        val check = getAuthorizationCheckForNamespace(uid.namespace)
        val keysWithDelegation = check.getValidAuthorizationKeys(
          signingKeys,
          requireRoot = false,
        )
        val keysAuthorizeNamespace =
          check.areValidAuthorizationKeys(signingKeys, requireRoot = false)

        val keyForUid =
          getAuthorizedIdentifierDelegation(check, uid, toValidate.signatures.map(_.signedBy))
            .map(_.mapping.target)

        (uid -> (keysAuthorizeNamespace || keyForUid.nonEmpty, keysWithDelegation ++ keyForUid))
      }.toMap

    val extraKeyAuthorizations = {
      // assume extra keys are not found
      required.extraKeys.map(k => k -> (false, Set.empty[SigningPublicKey])).toMap ++
        // and replace with those that were actually found
        // we have to dive into the owner to key mapping directly here, because we don't
        // need to keep it around (like we do for namespace delegations) and the OTK is the
        // only place that holds the SigningPublicKey.
        toValidate
          .select[TopologyChangeOp.Replace, OwnerToKeyMapping]
          .toList
          .flatMap { otk =>
            otk.mapping.keys.collect {
              case k: SigningPublicKey
                  // only consider the public key as "found" if:
                  // * it's required and
                  // * actually used to sign the transaction
                  if required.extraKeys(k.fingerprint) && signingKeys(k.fingerprint) =>
                k.fingerprint -> (true, Set(k))
            }
          }
          .toMap
    }

    val allAuthorizingKeys =
      (namespaceWithRootAuthorizations.values ++
        namespaceAuthorizations.values ++
        uidAuthorizations.values ++
        extraKeyAuthorizations.values).flatMap { case (_, keys) =>
        keys.map(k => k.id -> k)
      }.toMap

    logAuthorizations("Authorizations with root for namespaces", namespaceWithRootAuthorizations)
    logAuthorizations("Authorizations for namespaces", namespaceAuthorizations)
    logAuthorizations("Authorizations for UIDs", uidAuthorizations)
    logAuthorizations("Authorizations for extraKeys", extraKeyAuthorizations)

    logger.debug(s"All authorizing keys: ${allAuthorizingKeys.keySet}")

    val superfluousKeys = signingKeys -- allAuthorizingKeys.keys
    for {
      _ <- Either.cond[TopologyTransactionRejection, Unit](
        // there must be at least 1 key used for the signatures for one of the delegation mechanisms
        (signingKeys -- superfluousKeys).nonEmpty,
        (), {
          logger.info(
            s"The keys ${superfluousKeys.mkString(", ")} have no delegation to authorize the transaction $toValidate"
          )
          TopologyTransactionRejection.NoDelegationFoundForKeys(superfluousKeys)
        },
      )

      txWithValidSignatures <- toValidate
        .removeSignatures(superfluousKeys)
        .toRight({
          logger.info(
            "Removing all superfluous keys results in a transaction without any signatures at all."
          )
          TopologyTransactionRejection.NoDelegationFoundForKeys(superfluousKeys)
        })

      _ <- txWithValidSignatures.signatures.forgetNE.toList
        .traverse_(sig =>
          allAuthorizingKeys
            .get(sig.signedBy)
            .toRight({
              val msg =
                s"Key ${sig.signedBy} was delegated to, but no actual key was identified. This should not happen."
              logger.error(msg)
              TopologyTransactionRejection.Other(msg)
            })
            .flatMap(key =>
              pureCrypto
                .verifySignature(
                  txWithValidSignatures.hash.hash,
                  key,
                  sig,
                )
                .leftMap(TopologyTransactionRejection.SignatureCheckFailed)
            )
        )
    } yield {
      // and finally we can check whether the authorizations granted by the keys actually satisfy
      // the authorization requirements
      def onlyFullyAuthorized[A](map: Map[A, (Boolean, ?)]): Set[A] = map.collect {
        case (a, (true, _)) => a
      }.toSet
      val actual = RequiredAuthAuthorizations(
        namespacesWithRoot = onlyFullyAuthorized(namespaceWithRootAuthorizations),
        namespaces = onlyFullyAuthorized(namespaceAuthorizations),
        uids = onlyFullyAuthorized(uidAuthorizations),
        extraKeys = onlyFullyAuthorized(extraKeyAuthorizations),
      )
      (
        txWithValidSignatures,
        requiredAuth
          .satisfiedByActualAuthorizers(actual)
          .fold(identity, _ => RequiredAuthAuthorizations.empty),
      )
    }
  }

  private def logAuthorizations[A](
      msg: String,
      auths: Map[A, (Boolean, Set[SigningPublicKey])],
  )(implicit traceContext: TraceContext): Unit = {
    val authorizingKeys = auths
      .collect {
        case (auth, (fullyAuthorized, keys)) if keys.nonEmpty => (auth, fullyAuthorized, keys)
      }
    if (authorizingKeys.nonEmpty) {
      val report = authorizingKeys
        .map { case (auth, fullyAuthorized, keys) =>
          val status = if (fullyAuthorized) "fully" else "partially"
          s"$auth $status authorized by keys ${keys.map(_.id)}"
        }
        .mkString(", ")
      logger.debug(s"$msg: $report")
    }
  }

  private def getAuthorizedIdentifierDelegation(
      graph: AuthorizationCheck,
      uid: UniqueIdentifier,
      authKeys: Set[Fingerprint],
  ): Option[AuthorizedIdentifierDelegation] = {
    getIdentifierDelegationsForUid(uid)
      .find(aid =>
        authKeys(aid.mapping.target.id) && graph.areValidAuthorizationKeys(
          aid.signingKeys,
          requireRoot = false,
        )
      )
  }

  protected def getIdentifierDelegationsForUid(
      uid: UniqueIdentifier
  ): Set[AuthorizedIdentifierDelegation] = {
    identifierDelegationCache
      .getOrElse(uid, Set())
  }

  protected def getAuthorizationCheckForNamespace(
      namespace: Namespace
  ): AuthorizationCheck = {
    val decentralizedNamespaceCheck = decentralizedNamespaceCache.get(namespace).map(_._2)
    val namespaceCheck = namespaceCache.get(
      namespace
    )
    decentralizedNamespaceCheck
      .orElse(namespaceCheck)
      .getOrElse(AuthorizationCheck.empty)
  }

  protected def getAuthorizationGraphForNamespace(
      namespace: Namespace
  ): AuthorizationGraph = {
    namespaceCache.getOrElseUpdate(
      namespace,
      new AuthorizationGraph(namespace, extraDebugInfo = false, loggerFactory),
    )
  }

  protected def loadAuthorizationGraphs(
      timestamp: CantonTimestamp,
      namespaces: Set[Namespace],
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Future[Unit] = {
    val uncachedNamespaces =
      namespaces -- namespaceCache.keySet -- decentralizedNamespaceCache.keySet // only load the ones we don't already hold in memory

    for {
      // TODO(#12390) this doesn't find fully validated transactions from the same batch
      storedDecentralizedNamespace <- store.findPositiveTransactions(
        timestamp,
        asOfInclusive = false,
        isProposal = false,
        types = Seq(DecentralizedNamespaceDefinition.code),
        filterUid = None,
        filterNamespace = Some(uncachedNamespaces.toSeq),
      )
      decentralizedNamespaces = storedDecentralizedNamespace.result.flatMap(
        _.transaction.selectMapping[DecentralizedNamespaceDefinition]
      )
      decentralizedNamespaceOwnersToLoad = decentralizedNamespaces
        .flatMap(_.mapping.owners)
        .toSet -- namespaceCache.keySet
      namespacesToLoad = uncachedNamespaces ++ decentralizedNamespaceOwnersToLoad

      storedNamespaceDelegations <- store.findPositiveTransactions(
        timestamp,
        asOfInclusive = false,
        isProposal = false,
        types = Seq(NamespaceDelegation.code),
        filterUid = None,
        filterNamespace = Some(namespacesToLoad.toSeq),
      )
      namespaceDelegations = storedNamespaceDelegations.result.flatMap(
        _.transaction.selectMapping[NamespaceDelegation]
      )
    } yield {
      val missingNSDs =
        namespacesToLoad -- namespaceDelegations.map(_.mapping.namespace).toSet
      if (missingNSDs.nonEmpty)
        logger.debug(s"Didn't find a namespace delegations for $missingNSDs at $timestamp")

      val namespaceToTx = namespaceDelegations
        .groupBy(_.mapping.namespace)
      namespaceToTx
        .foreach { case (namespace, transactions) =>
          ErrorUtil.requireArgument(
            !namespaceCache.isDefinedAt(namespace),
            s"graph shouldn't exist before loading ${namespaces} vs ${namespaceCache.keySet}",
          )
          val graph = new AuthorizationGraph(
            namespace,
            extraDebugInfo = false,
            loggerFactory,
          )
          namespaceCache.put(namespace, graph).discard
          // use un-authorized batch load. while we are checking for proper authorization when we
          // add a certificate the first time, we allow for the situation where an intermediate certificate
          // is currently expired, but might be replaced with another cert. in this case,
          // the authorization check would fail.
          // unauthorized certificates are not really an issue as we'll simply exclude them when calculating
          // the connected graph
          graph.unauthorizedAdd(transactions.map(AuthorizedTopologyTransaction(_)))
        }

      decentralizedNamespaces.foreach { dns =>
        val namespace = dns.mapping.namespace
        ErrorUtil.requireArgument(
          !decentralizedNamespaceCache.isDefinedAt(namespace),
          s"decentralized namespace shouldn't already be cached before loading $namespace vs ${decentralizedNamespaceCache.keySet}",
        )
        val graphs = dns.mapping.owners.forgetNE.toSeq.map(ns =>
          namespaceCache.getOrElseUpdate(
            ns,
            new AuthorizationGraph(
              ns,
              extraDebugInfo = false,
              loggerFactory,
            ),
          )
        )
        val directDecentralizedNamespaceGraph = namespaceCache.getOrElseUpdate(
          namespace,
          new AuthorizationGraph(
            namespace,
            extraDebugInfo = false,
            loggerFactory,
          ),
        )
        decentralizedNamespaceCache
          .put(
            namespace,
            (
              dns.mapping,
              DecentralizedNamespaceAuthorizationGraph(
                dns.mapping,
                directDecentralizedNamespaceGraph,
                graphs,
              ),
            ),
          )
          .discard
      }
    }
  }

  protected def loadIdentifierDelegations(
      timestamp: CantonTimestamp,
      namespaces: Seq[Namespace],
      uids: Set[UniqueIdentifier],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Set[UniqueIdentifier]] = {
    val uidFilter = (uids -- identifierDelegationCache.keySet)
    store
      .findPositiveTransactions(
        timestamp,
        asOfInclusive = false,
        isProposal = false,
        types = Seq(IdentifierDelegation.code),
        filterUid = Some(uidFilter.toSeq),
        filterNamespace = None,
      )
      .map { stored =>
        val loaded = stored.result.flatMap(
          _.transaction.selectMapping[IdentifierDelegation].map(AuthorizedTopologyTransaction(_))
        )
        val start =
          identifierDelegationCache.keySet
            .filter(cached => namespaces.contains(cached.namespace))
            .toSet
        loaded.foldLeft(start) { case (acc, item) =>
          mergeLoadedIdentifierDelegation(item)
          val uid = item.mapping.identifier
          if (namespaces.contains(uid.namespace))
            acc + uid
          else acc
        }

      }
  }

  private def mergeLoadedIdentifierDelegation(item: AuthorizedIdentifierDelegation): Unit =
    updateIdentifierDelegationCache(item.mapping.identifier, _ + item)

  protected def updateIdentifierDelegationCache(
      uid: UniqueIdentifier,
      op: Set[AuthorizedIdentifierDelegation] => Set[AuthorizedIdentifierDelegation],
  ): Unit = {
    val cur = identifierDelegationCache.getOrElseUpdate(uid, Set())
    identifierDelegationCache.update(uid, op(cur)).discard
  }
}
