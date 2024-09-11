// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.AuthorizedIdentifierDelegation
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.TopologyTransaction.GenericTopologyTransaction
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** cache for working with topology transaction authorization */
trait TransactionAuthorizationCache[+PureCrypto <: CryptoPureApi] {
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

  protected def pureCrypto: PureCrypto

  implicit protected def executionContext: ExecutionContext

  final def reset(): Unit = {
    namespaceCache.clear()
    identifierDelegationCache.clear()
    decentralizedNamespaceCache.clear()
  }

  final def populateCaches(
      asOfExclusive: CantonTimestamp,
      toProcess: GenericTopologyTransaction,
      inStore: Option[GenericTopologyTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val requiredKeys = AuthorizationKeys.required(toProcess, inStore)
    val loadNsdF = loadNamespaceCaches(asOfExclusive, requiredKeys.namespaces)
    val loadIddF = loadIdentifierDelegationCaches(asOfExclusive, requiredKeys.uids)
    loadNsdF.flatMap(_ => loadIddF)
  }

  protected def tryGetIdentifierDelegationsForUid(
      uid: UniqueIdentifier
  )(implicit traceContext: TraceContext): Set[AuthorizedIdentifierDelegation] =
    identifierDelegationCache.getOrElse(
      uid,
      ErrorUtil.invalidState(s"Cache miss for identifier $uid"),
    )

  protected def tryGetAuthorizationCheckForNamespace(
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
      asOfExclusive: CantonTimestamp,
      namespaces: Set[Namespace],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    // only load the ones we don't already hold in memory
    val decentralizedNamespacesToLoad = namespaces -- decentralizedNamespaceCache.keys

    for {
      storedDecentralizedNamespace <- store.findPositiveTransactions(
        asOfExclusive,
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
        asOfExclusive,
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
      asOfExclusive: CantonTimestamp,
      uids: Set[UniqueIdentifier],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val identifierDelegationsToLoad = uids -- identifierDelegationCache.keySet
    for {
      stored <- store
        .findPositiveTransactions(
          asOfExclusive,
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
