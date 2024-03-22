// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Namespace
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.*
import com.digitalasset.canton.topology.processing.TransactionAuthorizationValidator.AuthorizationChain
import com.digitalasset.canton.topology.transaction.{
  IdentifierDelegation,
  NamespaceDelegation,
  SignedTopologyTransaction,
  TopologyChangeOp,
  TopologyMapping,
  UniquePath,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.math.Ordering.Implicits.*

/** An authorized topology transaction */
final case class AuthorizedTopologyTransaction[T <: TopologyMapping](
    uniquePath: UniquePath,
    mapping: T,
    transaction: SignedTopologyTransaction[TopologyChangeOp],
) {
  def signingKey: Fingerprint = transaction.key.fingerprint
}

object AuthorizedTopologyTransaction {

  type AuthorizedNamespaceDelegation = AuthorizedTopologyTransaction[NamespaceDelegation]
  type AuthorizedIdentifierDelegation = AuthorizedTopologyTransaction[IdentifierDelegation]

  /** Returns true if the namespace delegation is a root certificate
    *
    * A root certificate is defined by the namespace delegation that authorizes the
    * key f to act on namespace spanned by f, authorized by f.
    */
  def isRootCertificate(namespaceDelegation: AuthorizedNamespaceDelegation): Boolean = {
    val mapping = namespaceDelegation.mapping
    (mapping.namespace.fingerprint == mapping.target.fingerprint) && namespaceDelegation.signingKey == mapping.target.fingerprint
  }

}

/** maintain a dependency graph for the namespace delegations
  *
  * namespace delegations are a bit tricky as there can be an arbitrary number of delegations before we reach
  * the actual key that will be used for authorizations. think of it as a certificate chain where we get a
  * series of certificates and we need to figure out a path from one certificate to the root certificate.
  *
  * NOTE: this class is not thread-safe
  *
  * properties of the graph:
  *   - the nodes are the target key fingerprints
  *   - the node with fingerprint of the namespace is the root node
  *   - the edges between the nodes are the authorizations where key A authorizes key B to act on the namespace
  *     in this case, the authorization is outgoing from A and incoming to B.
  *   - the graph SHOULD be directed acyclic graph, but we MIGHT have cycles (i.e. key A authorizing B, B authorizing A).
  *     we don't need to make a fuss about cycles in the graph. we just ignore / report them assuming it was an admin
  *     mistake, but we don't get confused.
  *   - root certificates are edges pointing to the node itself. they are separate such that they don't show up
  *     in the list of incoming / outgoing.
  *   - we track for each node the set of outgoing edges and incoming edges. an outgoing edge is a delegation where
  *     the source node is authorizing a target node. obviously every outgoing edge is also an incoming edge.
  *
  * computation task:
  *   - once we've modified the graph, we compute the nodes that are somehow connected to the root node.
  *
  * purpose:
  *   - once we know which target keys are actually authorized to act on this particular namespace, we can then use
  *     this information to find out which resulting mapping is properly authorized and which one not.
  *
  * authorization checks:
  *   - when adding "single transactions", we do check that the transaction is properly authorized. otherwise we
  *     "ignore" it (returning false). this is used during processing.
  *   - when adding "batch transactions", we don't check that all of them are properly authorized, as we do allow
  *     temporarily "nodes" to be unauthorized (so that errors can be fixed by adding a replacement certificate)
  *   - when removing transactions, we do check that the authorizing key is authorized. but note that the authorizing
  *     key of an edge REMOVAL doesn't need to match the key used to authorized the ADD.
  */
class AuthorizationGraph(
    namespace: Namespace,
    extraDebugInfo: Boolean,
    val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {

  private case class GraphNode(
      target: Fingerprint,
      root: Set[AuthorizedNamespaceDelegation] = Set(),
      outgoing: Set[AuthorizedNamespaceDelegation] = Set(),
      incoming: Set[AuthorizedNamespaceDelegation] = Set(),
  ) {

    def isEmpty: Boolean = root.isEmpty && outgoing.isEmpty && incoming.isEmpty

  }

  private abstract class AuthLevel(val isAuth: Boolean, val isRoot: Boolean)
  private object AuthLevel {

    object NotAuthorized extends AuthLevel(false, false)
    object Standard extends AuthLevel(true, false)
    object RootDelegation extends AuthLevel(true, true)

    implicit val orderingAuthLevel: Ordering[AuthLevel] =
      Ordering.by[AuthLevel, Int](authl => { Seq(authl.isAuth, authl.isRoot).count(identity) })

    def fromDelegationO(delegation: Option[AuthorizedNamespaceDelegation]): AuthLevel =
      delegation match {
        case None => AuthLevel.NotAuthorized
        case Some(item) if item.mapping.isRootDelegation => RootDelegation
        case Some(_) => Standard
      }

  }

  private val nodes = new TrieMap[Fingerprint, GraphNode]()

  /** temporary cache for the current graph authorization check results
    *
    * if a fingerprint is empty, then we haven't yet computed the answer
    */
  private val cache = new TrieMap[Fingerprint, Option[AuthorizedNamespaceDelegation]]()

  def add(item: AuthorizedNamespaceDelegation)(implicit traceContext: TraceContext): Boolean = {
    ErrorUtil.requireArgument(
      item.mapping.namespace == namespace,
      s"added namespace ${item.mapping.namespace} to $namespace",
    )
    if (
      AuthorizedTopologyTransaction.isRootCertificate(item) ||
      this.isValidAuthorizationKey(item.signingKey, requireRoot = true)
    ) {
      doAdd(item)
      recompute()
      true
    } else false
  }

  def unauthorizedAdd(
      items: Seq[AuthorizedNamespaceDelegation]
  )(implicit traceContext: TraceContext): Unit = {
    items.foreach(doAdd)
    recompute()
  }

  private def doAdd(item: AuthorizedNamespaceDelegation): Unit = {
    val targetKey = item.mapping.target.fingerprint
    val curTarget = nodes.getOrElse(targetKey, GraphNode(targetKey))
    // if this is a root certificate, remember it separately
    if (AuthorizedTopologyTransaction.isRootCertificate(item)) {
      nodes.update(targetKey, curTarget.copy(root = curTarget.root + item))
    } else {
      val authKey = item.signingKey
      val curAuth = nodes.getOrElse(authKey, GraphNode(authKey))
      nodes.update(authKey, curAuth.copy(outgoing = curAuth.outgoing + item))
      nodes.update(targetKey, curTarget.copy(incoming = curTarget.incoming + item))
    }
  }

  def remove(item: AuthorizedNamespaceDelegation)(implicit traceContext: TraceContext): Boolean =
    if (isValidAuthorizationKey(item.signingKey, requireRoot = true)) {
      doRemove(item)
      true
    } else false

  def unauthorizedRemove(
      items: Seq[AuthorizedNamespaceDelegation]
  )(implicit traceContext: TraceContext): Unit = {
    items.foreach(doRemove)
  }

  /** remove a namespace delegation
    *
    * note that this one is a bit tricky as the removal might have been authorized
    * by a different key than the addition. this is fine but it complicates the book-keeping,
    * as we need to track for each target key what the "incoming authorizations" were solely for the
    * purpose of being able to clean them up
    */
  private def doRemove(
      item: AuthorizedNamespaceDelegation
  )(implicit traceContext: TraceContext): Unit = {
    ErrorUtil.requireArgument(
      item.mapping.namespace == namespace,
      s"added namespace ${item.mapping.namespace} to $namespace",
    )
    def myFilter(existing: AuthorizedNamespaceDelegation): Boolean = {
      // the auth key doesn't need to match on removals
      existing.uniquePath != item.uniquePath || existing.mapping != item.mapping
    }
    def updateRemove(key: Fingerprint, res: GraphNode): Unit = {
      val _ =
        if (res.isEmpty)
          nodes.remove(key)
        else
          nodes.update(key, res)
    }
    def removeOutgoing(node: GraphNode): Unit = {
      // we need to use the "incoming" edges to figure out the original outgoing keys, as the key that
      // was authorizing this removal might not be the one that authorized the addition
      node.incoming.map(_.signingKey).foreach { fp =>
        nodes.get(fp) match {
          case Some(curIncoming) =>
            // remove for this key the edge that goes to the target node
            updateRemove(fp, curIncoming.copy(outgoing = curIncoming.outgoing.filter(myFilter)))
          case None =>
            logger.error(
              s"Broken authorization graph when removing $item as node ${node.target} says that $fp is incoming, but $fp does not exist as a node"
            )
        }
      }
    }
    val targetKey = item.mapping.target.fingerprint
    nodes.get(targetKey) match {
      case Some(curTarget) =>
        if (AuthorizedTopologyTransaction.isRootCertificate(item)) {
          // if this is a root certificate, then we need to remove the self edge
          updateRemove(targetKey, curTarget.copy(root = curTarget.root.filter(myFilter)))
        } else {
          // we need to remove this "edge" from both nodes.
          // on the target node, this is a simple incoming edge
          // however, on the source node, this is a bit different
          removeOutgoing(curTarget)
          // remove incoming
          updateRemove(targetKey, curTarget.copy(incoming = curTarget.incoming.filter(myFilter)))
        }
        recompute()
      case None =>
        logger.warn(s"Superfluous removal of namespace delegation $item")
    }

  }

  protected def recompute()(implicit traceContext: TraceContext): Unit = {
    cache.clear()
    // recompute authorization graph starting from the root certificate
    // this is a graph potentially with cycles, as users might accidentally (or maliciously)
    // create useless certificates chain cycles.
    // however, the actual computation is simple: we start at the root certificate
    // and we let the "trust" (auth-level) flow from there downwards.
    // as a result, every key that doesn't end up in the cache is not connected
    // to the root certificate and therefore useless.
    // some nodes might be visited more than once, but only if the auth-level is increasing.
    // this will guarantee that we eventually terminate
    def go(fingerprint: Fingerprint, incoming: AuthorizedNamespaceDelegation): Unit = {
      val current = cache.getOrElseUpdate(fingerprint, None)
      val currentLevel = AuthLevel.fromDelegationO(current)
      val incomingLevel = AuthLevel.fromDelegationO(Some(incoming))
      // this inherited level is higher than current, propagate it
      if (incomingLevel > currentLevel) {
        cache.update(fingerprint, Some(incoming))
        // get the graph node of this fingerprint
        nodes.get(fingerprint).foreach { tracker =>
          // iterate through all edges that depart from this node
          tracker.outgoing
            .map(x => (AuthLevel.fromDelegationO(Some(x)), x))
            // only propagate edges that require lower or equal authorization level than what we have from incoming
            // so an outgoing root delegation can not be authorized by an incoming non-root delegation
            .filter { case (outgoingLevel, _) => outgoingLevel <= incomingLevel }
            .foreach {
              // iterate through all target fingerprint, taking the edge outgoing from this node as the incoming
              case (_, outgoing) => go(outgoing.mapping.target.fingerprint, incoming = outgoing)
            }
        }
      }
    }
    // start iterating from root certificate (if there is one)
    nodes.get(namespace.fingerprint).flatMap(_.root.headOption).foreach { root =>
      go(namespace.fingerprint, root)
    }
    report()
  }

  def report()(implicit traceContext: TraceContext): Unit =
    if (nodes.get(namespace.fingerprint).flatMap(_.root.headOption).isDefined) {
      val dangling = nodes.keySet.diff(cache.keySet)
      if (dangling.nonEmpty) {
        logger.warn(s"The following target keys of namespace $namespace are dangling: $dangling")
      }
      if (extraDebugInfo && logger.underlying.isDebugEnabled) {
        val str =
          authorizedDelegations()
            .map(aud =>
              show"auth=${aud.signingKey}, target=${aud.mapping.target.fingerprint}, root=${isRootCertificate(aud)}, elementId=${aud.uniquePath.maybeElementId}"
            )
            .mkString("\n  ")
        logger.debug(s"The authorization graph is given by:\n  $str")
      }
    } else
      logger.debug(
        s"Namespace ${namespace} has no root certificate, making all ${nodes.size} un-authorized"
      )

  def isValidAuthorizationKey(authKey: Fingerprint, requireRoot: Boolean): Boolean = {
    val authLevel = AuthLevel.fromDelegationO(cache.getOrElse(authKey, None))
    authLevel.isRoot || (authLevel.isAuth && !requireRoot)
  }

  def authorizationChain(
      startAuthKey: Fingerprint,
      requireRoot: Boolean,
  ): Option[AuthorizationChain] = {
    @tailrec
    def go(
        authKey: Fingerprint,
        requireRoot: Boolean,
        acc: List[AuthorizedNamespaceDelegation],
    ): List[AuthorizedNamespaceDelegation] = {
      cache.getOrElse(authKey, None) match {
        // we've terminated with the root certificate
        case Some(delegation) if isRootCertificate(delegation) =>
          delegation :: acc
        // cert is valid, append it
        case Some(delegation) if delegation.mapping.isRootDelegation || !requireRoot =>
          go(delegation.signingKey, delegation.mapping.isRootDelegation, delegation :: acc)
        // return empty to indicate failure
        case _ => List.empty
      }
    }
    go(startAuthKey, requireRoot, List.empty) match {
      case Nil => None
      case rest =>
        Some(
          AuthorizationChain(identifierDelegation = Seq.empty, namespaceDelegations = rest)
        )
    }
  }

  def authorizedDelegations(): Seq[AuthorizedNamespaceDelegation] =
    cache.values.flatMap(_.toList).toSeq

}
