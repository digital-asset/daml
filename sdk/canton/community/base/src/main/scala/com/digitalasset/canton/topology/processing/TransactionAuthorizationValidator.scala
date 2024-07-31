// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import com.digitalasset.canton.crypto.{CryptoPureApi, SigningPublicKey}
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
import com.digitalasset.canton.topology.transaction.TopologyMapping.ReferencedAuthorizations
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** common functionality shared between incoming transaction authorization validator and the auth computation */
trait TransactionAuthorizationValidator {

  this: NamedLogging =>

  /** Invariants:
    * - If it stores ns -> graph, then graph consists of all active namespace delegations for ns.
    * - If it stores ns -> graph and graph is non-empty, then there is no decentralized namespace delegation active for ns.
    */
  protected val namespaceCache = new TrieMap[Namespace, AuthorizationGraph]()

  /** Invariants:
    * - If it stores ns -> Some(graph), then the graph corresponds to the active decentralized namespace delegation for ns.
    *   Moreover, for each owner o, the owner graph is namespaceCache(o).
    * - If it stores ns -> None, then there is no decentralized namespace delegation active for ns.
    * - If it stores ns -> Some(graph), then there is no direct namespace delegation active for ns.
    */
  protected val decentralizedNamespaceCache =
    new TrieMap[
      Namespace,
      Option[DecentralizedNamespaceAuthorizationGraph],
    ]()

  /** Invariants:
    * - If it stores id -> ids, then ids consists of all active identifier delegations for id.
    */
  protected val identifierDelegationCache =
    new TrieMap[UniqueIdentifier, Set[AuthorizedIdentifierDelegation]]()

  protected def store: TopologyStore[TopologyStoreId]

  protected def pureCrypto: CryptoPureApi

  implicit protected def executionContext: ExecutionContext

  def validateSignaturesAndDetermineMissingAuthorizers(
      toValidate: GenericSignedTopologyTransaction,
      inStore: Option[GenericSignedTopologyTransaction],
  )(implicit
      traceContext: TraceContext
  ): Either[
    TopologyTransactionRejection,
    (GenericSignedTopologyTransaction, ReferencedAuthorizations),
  ] = {
    // first determine all possible namespaces and uids that need to sign the transaction
    val requiredAuth = toValidate.mapping.requiredAuth(inStore.map(_.transaction))

    logger.debug(s"Required authorizations: $requiredAuth")

    val referencedAuth = requiredAuth.referenced

    val signingKeys = toValidate.signatures.map(_.signedBy)
    val namespaceWithRootAuthorizations =
      referencedAuth.namespacesWithRoot.map { ns =>
        // This succeeds because loading of uid is requested in AuthorizationKeys.requiredForCheckingAuthorization
        val check = tryGetAuthorizationCheckForNamespace(ns)
        val keysUsed = check.keysSupportingAuthorization(
          signingKeys,
          requireRoot = true,
        )
        val keysAuthorizeNamespace =
          check.existsAuthorizedKeyIn(signingKeys, requireRoot = true)
        ns -> (keysAuthorizeNamespace, keysUsed)
      }.toMap

    // Now let's determine which namespaces and uids actually delegated to any of the keys
    val namespaceAuthorizations = referencedAuth.namespaces.map { ns =>
      // This succeeds because loading of uid is requested in AuthorizationKeys.requiredForCheckingAuthorization
      val check = tryGetAuthorizationCheckForNamespace(ns)
      val keysUsed = check.keysSupportingAuthorization(
        signingKeys,
        requireRoot = false,
      )
      val keysAuthorizeNamespace = check.existsAuthorizedKeyIn(signingKeys, requireRoot = false)
      ns -> (keysAuthorizeNamespace, keysUsed)
    }.toMap

    val uidAuthorizations =
      referencedAuth.uids.map { uid =>
        // This succeeds because loading of uid.namespace is requested in AuthorizationKeys.requiredForCheckingAuthorization
        val check = tryGetAuthorizationCheckForNamespace(uid.namespace)
        val keysUsed = check.keysSupportingAuthorization(
          signingKeys,
          requireRoot = false,
        )
        val keysAuthorizeNamespace =
          check.existsAuthorizedKeyIn(signingKeys, requireRoot = false)

        val keyForUid =
          // This succeeds because loading of uid is requested in AuthorizationKeys.requiredForCheckingAuthorization
          tryGetIdentifierDelegationsForUid(uid)
            .find(aid =>
              signingKeys.contains(aid.mapping.target.id) &&
                check.existsAuthorizedKeyIn(
                  aid.signingKeys,
                  requireRoot = false,
                )
            )
            .map(_.mapping.target)

        uid -> (keysAuthorizeNamespace || keyForUid.nonEmpty, keysUsed ++ keyForUid)
      }.toMap

    val extraKeyAuthorizations =
      // assume extra keys are not found
      referencedAuth.extraKeys.map(k => k -> (false, Set.empty[SigningPublicKey])).toMap ++
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
                  if referencedAuth.extraKeys(k.fingerprint) && signingKeys(k.fingerprint) =>
                k.fingerprint -> (true, Set(k))
            }
          }
          .toMap

    val allKeysUsedForAuthorization =
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

    logger.debug(s"All keys used for authorization: ${allKeysUsedForAuthorization.keySet}")

    val superfluousKeys = signingKeys -- allKeysUsedForAuthorization.keys
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

      txWithSignaturesToVerify <- toValidate
        .removeSignatures(superfluousKeys)
        .toRight({
          logger.info(
            "Removing all superfluous keys results in a transaction without any signatures at all."
          )
          TopologyTransactionRejection.NoDelegationFoundForKeys(superfluousKeys)
        })

      _ <- txWithSignaturesToVerify.signatures.forgetNE.toList
        .traverse_(sig =>
          allKeysUsedForAuthorization
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
                  txWithSignaturesToVerify.hash.hash,
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
      val actual = ReferencedAuthorizations(
        namespacesWithRoot = onlyFullyAuthorized(namespaceWithRootAuthorizations),
        namespaces = onlyFullyAuthorized(namespaceAuthorizations),
        uids = onlyFullyAuthorized(uidAuthorizations),
        extraKeys = onlyFullyAuthorized(extraKeyAuthorizations),
      )
      (
        txWithSignaturesToVerify,
        requiredAuth
          .satisfiedByActualAuthorizers(actual)
          .fold(identity, _ => ReferencedAuthorizations.empty),
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

  protected def tryGetIdentifierDelegationsForUid(
      uid: UniqueIdentifier
  )(implicit traceContext: TraceContext): Set[AuthorizedIdentifierDelegation] =
    identifierDelegationCache.getOrElse(
      uid,
      ErrorUtil.invalidState(s"Cache miss for identifier $uid"),
    )

  private def tryGetAuthorizationCheckForNamespace(
      namespace: Namespace
  )(implicit traceContext: TraceContext): AuthorizationCheck = {
    val directGraph = tryGetAuthorizationGraphForNamespace(namespace)
    val decentralizedGraphO = decentralizedNamespaceCache.getOrElse(
      namespace,
      ErrorUtil.invalidState(s"Cache miss for decentralized namespace $namespace"),
    )

    decentralizedGraphO match {
      case Some(decentralizedGraph) =>
        val directGraphNodes = directGraph.nodes
        ErrorUtil.requireState(
          directGraphNodes.isEmpty,
          show"Namespace $namespace has both direct and decentralized delegations.\n${decentralizedGraph.dnd}\nDirect delegations for: $directGraphNodes",
        )
        decentralizedGraph
      case None => directGraph
    }
  }

  protected def tryGetAuthorizationGraphForNamespace(
      namespace: Namespace
  )(implicit traceContext: TraceContext): AuthorizationGraph =
    namespaceCache.getOrElse(
      namespace,
      ErrorUtil.invalidState(s"Cache miss for direct namespace $namespace"),
    )

  protected def loadNamespaceCaches(
      effectiveTime: CantonTimestamp,
      namespaces: Set[Namespace],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    // only load the ones we don't already hold in memory
    val decentralizedNamespacesToLoad = namespaces -- decentralizedNamespaceCache.keys

    for {
      storedDecentralizedNamespace <- store.findPositiveTransactions(
        effectiveTime,
        asOfInclusive = false,
        isProposal = false,
        types = Seq(DecentralizedNamespaceDefinition.code),
        filterUid = None,
        filterNamespace = Some(decentralizedNamespacesToLoad.toSeq),
      )
      decentralizedNamespaceDefinitions = storedDecentralizedNamespace
        .collectOfMapping[DecentralizedNamespaceDefinition]
        .collectLatestByUniqueKey
        .result
        .map(_.transaction)

      // We need to add queries for owners here, because the caller cannot know them up front.
      decentralizedNamespaceOwners = decentralizedNamespaceDefinitions
        .flatMap(_.mapping.owners)
        .toSet
      namespacesToLoad = namespaces ++ decentralizedNamespaceOwners -- namespaceCache.keys

      storedNamespaceDelegations <- store.findPositiveTransactions(
        effectiveTime,
        asOfInclusive = false,
        isProposal = false,
        types = Seq(NamespaceDelegation.code),
        filterUid = None,
        filterNamespace = Some(namespacesToLoad.toSeq),
      )
      namespaceDelegations = storedNamespaceDelegations
        .collectOfMapping[NamespaceDelegation]
        .collectLatestByUniqueKey
        .result
        .map(_.transaction)
    } yield {

      namespaceDelegations
        .groupBy(_.mapping.namespace)
        .foreach { case (namespace, transactions) =>
          val graph = new AuthorizationGraph(
            namespace,
            extraDebugInfo = false,
            loggerFactory,
          )
          graph.replace(transactions.map(AuthorizedTopologyTransaction(_)))
          val previous = namespaceCache.put(namespace, graph)
          ErrorUtil.requireState(
            previous.isEmpty,
            s"Unexpected cache hit for namespace $namespace: $previous",
          )
          val conflictingDecentralizedNamespaceDefinition =
            decentralizedNamespaceCache.get(namespace).flatten
          ErrorUtil.requireState(
            conflictingDecentralizedNamespaceDefinition.isEmpty,
            s"Conflicting decentralized namespace definition for namespace $namespace: $conflictingDecentralizedNamespaceDefinition",
          )
        }

      namespacesToLoad.foreach { namespace =>
        namespaceCache
          .putIfAbsent(
            namespace,
            new AuthorizationGraph(
              namespace,
              extraDebugInfo = false,
              loggerFactory,
            ),
          )
          .discard
      }

      decentralizedNamespaceDefinitions.foreach { dns =>
        val namespace = dns.mapping.namespace
        val ownerGraphs =
          dns.mapping.owners.forgetNE.toSeq.map(
            // This will succeed, because owner graphs have been loaded just above.
            tryGetAuthorizationGraphForNamespace(_)
          )
        val decentralizedGraph = DecentralizedNamespaceAuthorizationGraph(
          dns.mapping,
          ownerGraphs,
        )
        val previous = decentralizedNamespaceCache.put(namespace, Some(decentralizedGraph))
        ErrorUtil.requireState(
          previous.isEmpty,
          s"Unexpected cache hit for decentralized namespace $namespace: $previous",
        )
        val conflictingDirectGraphNodes = namespaceCache.get(namespace).toList.flatMap(_.nodes)
        ErrorUtil.requireState(
          conflictingDirectGraphNodes.isEmpty,
          s"Conflicting direct namespace graph for namespace $namespace: $conflictingDirectGraphNodes",
        )
      }

      decentralizedNamespacesToLoad.foreach(
        decentralizedNamespaceCache.putIfAbsent(_, None).discard
      )
    }
  }

  protected def loadIdentifierDelegationCaches(
      effectiveTime: CantonTimestamp,
      uids: Set[UniqueIdentifier],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val identifierDelegationsToLoad = uids -- identifierDelegationCache.keySet
    for {
      stored <- store
        .findPositiveTransactions(
          effectiveTime,
          asOfInclusive = false,
          isProposal = false,
          types = Seq(IdentifierDelegation.code),
          filterUid = Some(identifierDelegationsToLoad.toSeq),
          filterNamespace = None,
        )
    } yield {
      val identifierDelegations = stored
        .collectOfMapping[IdentifierDelegation]
        .collectLatestByUniqueKey
        .result
        .map(identifierDelegation =>
          AuthorizedTopologyTransaction(identifierDelegation.transaction)
        )

      identifierDelegations.groupBy(_.mapping.identifier).foreach { case (uid, delegations) =>
        val previous = identifierDelegationCache.put(uid, delegations.toSet)
        ErrorUtil.requireState(
          previous.isEmpty,
          s"Unexpected cache hit for identiefier $uid: $previous",
        )
      }
      identifierDelegationsToLoad.foreach(identifierDelegationCache.putIfAbsent(_, Set()).discard)
    }
  }
}
