// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.crypto.{CryptoPureApi, Fingerprint, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransactionX.AuthorizedIdentifierDelegationX
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyStoreX,
  TopologyTransactionRejection,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.RequiredAuthXAuthorizations
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** common functionality shared between incoming transaction authorization validator and the auth computation */
trait TransactionAuthorizationValidatorX {

  this: NamedLogging =>

  protected val namespaceCache = new TrieMap[Namespace, AuthorizationGraphX]()
  protected val identifierDelegationCache =
    new TrieMap[UniqueIdentifier, Set[AuthorizedIdentifierDelegationX]]()
  protected val decentralizedNamespaceCache =
    new TrieMap[
      Namespace,
      (DecentralizedNamespaceDefinitionX, DecentralizedNamespaceAuthorizationGraphX),
    ]()

  protected def store: TopologyStoreX[TopologyStoreId]

  protected def pureCrypto: CryptoPureApi

  def isCurrentlyAuthorized(
      toValidate: GenericSignedTopologyTransactionX,
      inStore: Option[GenericSignedTopologyTransactionX],
  )(implicit
      traceContext: TraceContext
  ): Either[TopologyTransactionRejection, RequiredAuthXAuthorizations] = {
    // first determine all possible namespaces and uids that need to sign the transaction
    val requiredAuth = toValidate.transaction.mapping.requiredAuth(inStore.map(_.transaction))

    logger.debug(s"Required authorizations: $requiredAuth")

    val required = requiredAuth
      .foldMap(
        namespaceCheck = rns =>
          RequiredAuthXAuthorizations(
            namespacesWithRoot =
              if (rns.requireRootDelegation) rns.namespaces else Set.empty[Namespace],
            namespaces = if (rns.requireRootDelegation) Set.empty[Namespace] else rns.namespaces,
          ),
        uidCheck = ruid => RequiredAuthXAuthorizations(uids = ruid.uids),
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

    val allAuthorizingKeys =
      namespaceWithRootAuthorizations.values.flatMap { case (_, keys) =>
        keys.map(k => k.id -> k)
      }.toMap
        ++ namespaceAuthorizations.values.flatMap { case (_, keys) =>
          keys.map(k => k.id -> k)
        }.toMap
        ++ uidAuthorizations.values.flatMap { case (_, keys) => keys.map(k => k.id -> k) }.toMap

    def logAuthorizations[A](auths: Map[A, (Boolean, Set[SigningPublicKey])]): String = {
      auths
        .map { case (auth, (fullyAuthorized, keys)) =>
          val status = if (fullyAuthorized) "fully" else "partially"
          s"$auth $status authorized by keys ${keys.map(_.id)}"
        }
        .mkString(", ")
    }

    if (namespaceWithRootAuthorizations.nonEmpty)
      logger.debug(
        s"Authorizations with root for namespaces: ${logAuthorizations(namespaceWithRootAuthorizations)}"
      )

    if (namespaceAuthorizations.nonEmpty)
      logger.debug(
        s"Authorizations for namespaces: ${logAuthorizations(namespaceAuthorizations)}"
      )

    if (uidAuthorizations.nonEmpty)
      logger.debug(
        s"Authorizations for UIDs: ${logAuthorizations(uidAuthorizations)}"
      )

    val superfluousKeys = signingKeys -- allAuthorizingKeys.keys
    for {
      _ <- Either.cond[TopologyTransactionRejection, Unit](
        // the key used for the signature must be a valid key for at least one of the delegation mechanisms
        superfluousKeys.isEmpty,
        (), {
          logger.info(
            s"The keys ${superfluousKeys.mkString(", ")} have no delegation to authorize the transaction $toValidate"
          )
          TopologyTransactionRejection.NoDelegationFoundForKeys(superfluousKeys)
        },
      )

      _ <- toValidate.signatures.forgetNE.toList
        .traverse_(sig =>
          allAuthorizingKeys
            .get(sig.signedBy)
            .toRight({
              val msg =
                s"Key ${sig.signedBy.singleQuoted} was delegated to, but no actual key was identified. This should not happen."
              logger.error(msg)
              TopologyTransactionRejection.Other(msg)
            })
            .flatMap(key =>
              pureCrypto
                .verifySignature(
                  toValidate.transaction.hash.hash,
                  key,
                  sig,
                )
                .leftMap(TopologyTransactionRejection.SignatureCheckFailed)
            )
        )
    } yield {
      // and finally we can check whether the authorizations granted by the keys actually satisfy
      // the authorization requirements
      val actual = RequiredAuthXAuthorizations(
        namespaceWithRootAuthorizations.collect { case (ns, (true, _)) => ns }.toSet,
        namespaceAuthorizations.collect { case (ns, (true, _)) => ns }.toSet,
        uidAuthorizations.collect { case (uid, (true, _)) => uid }.toSet,
      )
      requiredAuth
        .satisfiedByActualAuthorizers(
          namespacesWithRoot = actual.namespacesWithRoot,
          namespaces = actual.namespaces,
          uids = actual.uids,
        )
        .fold(identity, _ => RequiredAuthXAuthorizations.empty)
    }
  }

  private def getAuthorizedIdentifierDelegation(
      graph: AuthorizationCheckX,
      uid: UniqueIdentifier,
      authKeys: Set[Fingerprint],
  ): Option[AuthorizedIdentifierDelegationX] = {
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
  ): Set[AuthorizedIdentifierDelegationX] = {
    identifierDelegationCache
      .getOrElse(uid, Set())
  }

  protected def getAuthorizationCheckForNamespace(
      namespace: Namespace
  ): AuthorizationCheckX = {
    val decentralizedNamespaceCheck = decentralizedNamespaceCache.get(namespace).map(_._2)
    val namespaceCheck = namespaceCache.get(
      namespace
    )
    decentralizedNamespaceCheck
      .orElse(namespaceCheck)
      .getOrElse(AuthorizationCheckX.empty)
  }

  protected def getAuthorizationGraphForNamespace(
      namespace: Namespace
  ): AuthorizationGraphX = {
    namespaceCache.getOrElseUpdate(
      namespace,
      new AuthorizationGraphX(namespace, extraDebugInfo = false, loggerFactory),
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
        types = Seq(DecentralizedNamespaceDefinitionX.code),
        filterUid = None,
        filterNamespace = Some(uncachedNamespaces.toSeq),
      )
      decentralizedNamespaces = storedDecentralizedNamespace.result.flatMap(
        _.transaction.selectMapping[DecentralizedNamespaceDefinitionX]
      )
      decentralizedNamespaceOwnersToLoad = decentralizedNamespaces
        .flatMap(_.transaction.mapping.owners)
        .toSet -- namespaceCache.keySet
      namespacesToLoad = uncachedNamespaces ++ decentralizedNamespaceOwnersToLoad

      storedNamespaceDelegations <- store.findPositiveTransactions(
        timestamp,
        asOfInclusive = false,
        isProposal = false,
        types = Seq(NamespaceDelegationX.code),
        filterUid = None,
        filterNamespace = Some(namespacesToLoad.toSeq),
      )
      namespaceDelegations = storedNamespaceDelegations.result.flatMap(
        _.transaction.selectMapping[NamespaceDelegationX]
      )
    } yield {
      val missingNSDs =
        namespacesToLoad -- namespaceDelegations.map(_.transaction.mapping.namespace).toSet
      if (missingNSDs.nonEmpty)
        logger.debug(s"Didn't find a namespace delegations for $missingNSDs at $timestamp")

      val namespaceToTx = namespaceDelegations
        .groupBy(_.transaction.mapping.namespace)
      namespaceToTx
        .foreach { case (namespace, transactions) =>
          ErrorUtil.requireArgument(
            !namespaceCache.isDefinedAt(namespace),
            s"graph shouldn't exist before loading ${namespaces} vs ${namespaceCache.keySet}",
          )
          val graph = new AuthorizationGraphX(
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
          graph.unauthorizedAdd(transactions.map(AuthorizedTopologyTransactionX(_)))
        }

      decentralizedNamespaces.foreach { dns =>
        import dns.transaction.mapping.namespace
        ErrorUtil.requireArgument(
          !decentralizedNamespaceCache.isDefinedAt(namespace),
          s"decentralized namespace shouldn't already be cached before loading $namespace vs ${decentralizedNamespaceCache.keySet}",
        )
        val graphs = dns.transaction.mapping.owners.forgetNE.toSeq.map(ns =>
          namespaceCache.getOrElseUpdate(
            ns,
            new AuthorizationGraphX(
              ns,
              extraDebugInfo = false,
              loggerFactory,
            ),
          )
        )
        val directDecentralizedNamespaceGraph = namespaceCache.getOrElseUpdate(
          namespace,
          new AuthorizationGraphX(
            namespace,
            extraDebugInfo = false,
            loggerFactory,
          ),
        )
        decentralizedNamespaceCache
          .put(
            namespace,
            (
              dns.transaction.mapping,
              DecentralizedNamespaceAuthorizationGraphX(
                dns.transaction.mapping,
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
        types = Seq(IdentifierDelegationX.code),
        filterUid = Some(uidFilter.toSeq),
        filterNamespace = None,
      )
      .map { stored =>
        val loaded = stored.result.flatMap(
          _.transaction.selectMapping[IdentifierDelegationX].map(AuthorizedTopologyTransactionX(_))
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

  private def mergeLoadedIdentifierDelegation(item: AuthorizedIdentifierDelegationX): Unit =
    updateIdentifierDelegationCache(item.mapping.identifier, _ + item)

  protected def updateIdentifierDelegationCache(
      uid: UniqueIdentifier,
      op: Set[AuthorizedIdentifierDelegationX] => Set[AuthorizedIdentifierDelegationX],
  ): Unit = {
    val cur = identifierDelegationCache.getOrElseUpdate(uid, Set())
    identifierDelegationCache.update(uid, op(cur)).discard
  }
}
