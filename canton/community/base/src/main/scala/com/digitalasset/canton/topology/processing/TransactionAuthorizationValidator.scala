// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.instances.list.*
import cats.instances.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.{
  AuthorizedIdentifierDelegation,
  AuthorizedNamespaceDelegation,
}
import com.digitalasset.canton.topology.processing.TransactionAuthorizationValidator.AuthorizationChain
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** common functionality shared between incoming transaction authorization validator and the auth computation */
trait TransactionAuthorizationValidator {

  this: NamedLogging =>

  protected val namespaceCache = new TrieMap[Namespace, AuthorizationGraph]()
  protected val identifierDelegationCache =
    new TrieMap[UniqueIdentifier, Set[AuthorizedIdentifierDelegation]]()

  protected def store: TopologyStore[TopologyStoreId]

  def isCurrentlyAuthorized(sit: SignedTopologyTransaction[TopologyChangeOp]): Boolean = {
    val authKey = sit.key.fingerprint
    if (NamespaceDelegation.isRootCertificate(sit)) true
    else
      sit.transaction.element.mapping.requiredAuth match {
        case RequiredAuth.Ns(namespace, rootDelegation) =>
          getAuthorizationGraphForNamespace(namespace).isValidAuthorizationKey(
            authKey,
            requireRoot = rootDelegation,
          )
        case RequiredAuth.Uid(uids) => uids.forall(isAuthorizedForUid(_, authKey))
      }
  }

  def authorizationChainFor(
      sit: SignedTopologyTransaction[TopologyChangeOp]
  ): Option[AuthorizationChain] = {
    val authKey = sit.key.fingerprint
    if (NamespaceDelegation.isRootCertificate(sit)) Some(AuthorizationChain(Seq(), Seq()))
    else
      sit.transaction.element.mapping.requiredAuth match {
        case RequiredAuth.Ns(namespace, rootDelegation) =>
          getAuthorizationGraphForNamespace(namespace).authorizationChain(
            authKey,
            requireRoot = rootDelegation,
          )
        case RequiredAuth.Uid(uids) =>
          uids.toList
            .traverse(authorizationChainFor(_, authKey))
            .map(_.foldLeft(AuthorizationChain(Seq(), Seq())) { case (acc, elem) =>
              acc.merge(elem)
            })
      }
  }

  protected def authorizationChainFor(
      uid: UniqueIdentifier,
      authKey: Fingerprint,
  ): Option[AuthorizationChain] = {
    val graph = getAuthorizationGraphForNamespace(uid.namespace)
    graph.authorizationChain(authKey, requireRoot = false).orElse {
      getAuthorizedIdentifierDelegation(graph, uid, authKey).flatMap { aid =>
        graph
          .authorizationChain(aid.signingKey, requireRoot = false)
          .map(_.addIdentifierDelegation(aid))
      }
    }
  }

  private def getAuthorizedIdentifierDelegation(
      graph: AuthorizationGraph,
      uid: UniqueIdentifier,
      authKey: Fingerprint,
  ): Option[AuthorizedIdentifierDelegation] = {
    getIdentifierDelegationsForUid(uid)
      .filter(_.mapping.target.fingerprint == authKey)
      .find(aid => graph.isValidAuthorizationKey(aid.signingKey, requireRoot = false))
  }

  def isAuthorizedForUid(uid: UniqueIdentifier, authKey: Fingerprint): Boolean = {
    val graph = getAuthorizationGraphForNamespace(uid.namespace)
    graph.isValidAuthorizationKey(
      authKey,
      requireRoot = false,
    ) || getAuthorizedIdentifierDelegation(graph, uid, authKey).nonEmpty
  }

  protected def getIdentifierDelegationsForUid(
      uid: UniqueIdentifier
  ): Set[AuthorizedIdentifierDelegation] = {
    identifierDelegationCache
      .getOrElse(uid, Set())
  }

  protected def getAuthorizationGraphForNamespace(
      namespace: Namespace
  ): AuthorizationGraph =
    namespaceCache.getOrElseUpdate(
      namespace,
      new AuthorizationGraph(namespace, extraDebugInfo = false, loggerFactory),
    )

  protected def loadAuthorizationGraphs(
      timestamp: CantonTimestamp,
      namespaces: Set[Namespace],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[Unit] = {
    val loadNamespaces =
      namespaces -- namespaceCache.keySet // only load the ones we don't already hold in memory
    for {
      existing <- store.findPositiveTransactions(
        asOf = timestamp,
        asOfInclusive = false,
        includeSecondary = false,
        types = Seq(DomainTopologyTransactionType.NamespaceDelegation),
        filterUid = None,
        filterNamespace = Some(loadNamespaces.toSeq),
      )
    } yield {
      existing.adds
        .toAuthorizedTopologyTransactions { case x: NamespaceDelegation => x }
        .groupBy(_.mapping.namespace)
        .foreach { case (namespace, transactions) =>
          ErrorUtil.requireArgument(
            !namespaceCache.isDefinedAt(namespace),
            s"graph shouldnt exist before loading ${namespaces} vs ${namespaceCache.keySet}",
          )
          val graph = new AuthorizationGraph(namespace, extraDebugInfo = false, loggerFactory)
          namespaceCache.put(namespace, graph).discard
          // use un-authorized batch load. while we are checking for proper authorization when we
          // add a certificate the first time, we allow for the situation where an intermediate certificate
          // is currently expired, but might be replaced with another cert. in this case,
          // the authorization check would fail.
          // unauthorized certificates are not really an issue as we'll simply exclude them when calculating
          // the connected graph
          graph.unauthorizedAdd(transactions)
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
    store
      .findPositiveTransactions(
        asOf = timestamp,
        asOfInclusive = false,
        includeSecondary = false,
        types = Seq(DomainTopologyTransactionType.IdentifierDelegation),
        filterNamespace = Some(namespaces),
        filterUid = Some((uids -- identifierDelegationCache.keySet).toSeq),
      )
      .map(_.adds.toAuthorizedTopologyTransactions { case x: IdentifierDelegation => x })
      .map { loaded =>
        // include the uids which we already cache
        val start =
          identifierDelegationCache.keySet.filter(x => namespaces.contains(x.namespace)).toSet
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

object TransactionAuthorizationValidator {

  /** authorization data
    *
    * this type is returned by the authorization validator. it contains the series of transactions
    * that authorize a certain topology transaction.
    *
    * note that the order of the namespace delegation is in "authorization order".
    */
  final case class AuthorizationChain(
      identifierDelegation: Seq[AuthorizedIdentifierDelegation],
      namespaceDelegations: Seq[AuthorizedNamespaceDelegation],
  ) {

    def addIdentifierDelegation(aid: AuthorizedIdentifierDelegation): AuthorizationChain =
      copy(identifierDelegation = identifierDelegation :+ aid)

    def merge(other: AuthorizationChain): AuthorizationChain = {
      AuthorizationChain(
        mergeUnique(this.identifierDelegation, other.identifierDelegation),
        mergeUnique(this.namespaceDelegations, other.namespaceDelegations),
      )
    }

    private def mergeUnique[T](left: Seq[T], right: Seq[T]): Seq[T] = {
      mutable.LinkedHashSet.from(left).addAll(right).toSeq
    }

  }

  object AuthorizationChain {
    val empty = AuthorizationChain(Seq(), Seq())
  }

}
