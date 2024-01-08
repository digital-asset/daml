// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.crypto.{CryptoPureApi, Fingerprint}
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
  ): Either[TopologyTransactionRejection, RequiredAuthXAuthorizations] = {
    // first determine all possible namespaces and uids that need to sign the transaction
    val requiredAuth = toValidate.transaction.mapping.requiredAuth(inStore.map(_.transaction))
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

    val actualAuthorizersForSignatures = toValidate.signatures.toSeq.forgetNE.foldMap { sig =>
      // Now let's determine which namespaces and uids actually delegated to any of the keys
      val (actualNamespaceAuthorizationsWithRoot, rootKeys) =
        required.namespacesWithRoot.flatMap { ns =>
          getAuthorizationCheckForNamespace(ns)
            .getValidAuthorizationKey(
              sig.signedBy,
              requireRoot = true,
            )
            .map(ns -> _)
        }.unzip
      val (actualNamespaceAuthorizations, nsKeys) = required.namespaces.flatMap { ns =>
        getAuthorizationCheckForNamespace(ns)
          .getValidAuthorizationKey(
            sig.signedBy,
            requireRoot = false,
          )
          .map(ns -> _)
      }.unzip
      val (actualUidAuthorizations, uidKeys) =
        required.uids.flatMap { uid =>
          val authCheck = getAuthorizationCheckForNamespace(uid.namespace)
          val keyForNamespace = authCheck
            .getValidAuthorizationKey(sig.signedBy, requireRoot = false)
          lazy val keyForUid = getAuthorizedIdentifierDelegation(authCheck, uid, Set(sig.signedBy))
            .map(_.mapping.target)

          keyForNamespace
            .orElse(keyForUid)
            .map(uid -> _)
        }.unzip

      for {
        _ <- Either.cond[TopologyTransactionRejection, Unit](
          // the key used for the signature must be a valid key for at least one of the delegation mechanisms
          actualNamespaceAuthorizationsWithRoot.nonEmpty || actualNamespaceAuthorizations.nonEmpty || actualUidAuthorizations.nonEmpty,
          (),
          TopologyTransactionRejection.NotAuthorized,
        )

        keyForSignature <- (rootKeys ++ nsKeys ++ uidKeys).headOption
          .toRight[TopologyTransactionRejection](
            TopologyTransactionRejection.NotAuthorized
          )
        _ <- pureCrypto
          .verifySignature(toValidate.transaction.hash.hash, keyForSignature, sig)
          .leftMap(TopologyTransactionRejection.SignatureCheckFailed)

      } yield {
        RequiredAuthXAuthorizations(
          actualNamespaceAuthorizationsWithRoot,
          actualNamespaceAuthorizations,
          actualUidAuthorizations,
        )
      }
    }

    // and finally we can check whether the authorizations granted by the keys actually satisfy
    // the authorization requirements
    actualAuthorizersForSignatures.map { actual =>
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
