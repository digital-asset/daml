// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.instances.order.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Fingerprint, SigningPublicKey}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Namespace
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.AuthorizedNamespaceDelegation
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Remove, Replace}
import com.digitalasset.canton.topology.transaction.TopologyMapping.Code
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.graph.{MutableValueGraph, ValueGraphBuilder}

import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

/** An authorized topology transaction */
final case class AuthorizedTopologyTransaction[T <: TopologyMapping](
    transaction: SignedTopologyTransaction[TopologyChangeOp, T]
) extends DelegatedTopologyTransactionLike[TopologyChangeOp, T] {
  override protected def transactionLikeDelegate: TopologyTransactionLike[TopologyChangeOp, T] =
    transaction
  def signingKeys: NonEmpty[Set[Fingerprint]] = transaction.signatures.map(_.signedBy)
}

object AuthorizedTopologyTransaction {

  type AuthorizedNamespaceDelegation = AuthorizedTopologyTransaction[NamespaceDelegation]
  type AuthorizedIdentifierDelegation = AuthorizedTopologyTransaction[IdentifierDelegation]
  type AuthorizedDecentralizedNamespaceDefinition =
    AuthorizedTopologyTransaction[DecentralizedNamespaceDefinition]
}

/** Stores a set of namespace delegations, tracks dependencies and determines which keys are
  * authorized to sign on behalf of a namespace.
  *
  * Namespace delegations are a bit tricky as there can be an arbitrary number of delegations
  * between the namespace key and the key that will be used for authorizations. Think of it as a
  * certificate chain where we get a series of certificates and we need to figure out a path from
  * one certificate to the root certificate.
  *
  * NOTE: this class is not thread-safe
  *
  * Properties of the graph:
  *   - Each node corresponds to a target key
  *   - The node with key fingerprint of the namespace is the root node
  *   - The edges between nodes are namespace delegations. If key A signs a namespace delegation
  *     with target key B, then key A authorizes key B to act on the namespace. In this case, the
  *     edge is outgoing from node A and incoming into node B.
  *   - The graph may have cycles. The implementation does not get confused by this.
  *
  * Computation task: The graph maintains a set of nodes that are connected to the root node. Those
  * correspond to the keys that are authorized to sign on behalf of the namespace.
  *
  * Limitation: clients need to ensure that the namespace delegations added have valid signatures.
  * If delegations with invalid signatures are added, authorization will break.
  *
  * @param extraDebugInfo
  *   whether to log the authorization graph at debug level on every recomputation
  */
class AuthorizationGraph(
    val namespace: Namespace,
    extraDebugInfo: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
) extends AuthorizationCheck
    with NamedLogging {

  /** Use guava's directed ValueGraph to manage authorizations:
    *   - Nodes are the fingerprints of keys
    *   - Edges `from -NSD-> to` represent NamespaceDelegations
    *     - `from` the signing keys of NamespaceDelegation
    *     - `to` the target key of the NamespaceDelegation
    *   - All edges incoming to `to` are labelled with the same NSD.
    *   - Each node has at least one incoming or outgoing edge.
    */
  private val graph: MutableValueGraph[Fingerprint, AuthorizedNamespaceDelegation] =
    ValueGraphBuilder
      .directed()
      .allowsSelfLoops(true) // we allow self loops for the root certificate
      .build[Fingerprint, AuthorizedNamespaceDelegation]

  /** Authorized namespace delegations for namespace `this.namespace`, grouped by target. The
    * namespace delegations carry the length of the valid certificate chain required to arrive at
    * the root certificate
    */
  private val cache =
    new TrieMap[Fingerprint, (AuthorizedNamespaceDelegation, Int)]()

  def nodes: Set[Fingerprint] = graph.nodes().asScala.toSet

  def replace(item: AuthorizedNamespaceDelegation)(implicit traceContext: TraceContext): Unit =
    replace(Seq(item))

  /** Add the mappings in `items` to this graph and remove any existing mappings with the same
    * target fingerprint. If an unauthorized namespace delegation is added to the graph, the graph
    * will contain nodes that are not connected to the root. The target key of the unauthorized
    * delegation will still be considered unauthorized.
    *
    * @throws java.lang.IllegalArgumentException
    *   if `item` does not refer to `namespace` or the operation is not REPLACE.
    */
  def replace(
      items: Seq[AuthorizedNamespaceDelegation]
  )(implicit traceContext: TraceContext): Unit = {
    items.foreach(doReplace)
    recompute()
  }

  private def doReplace(
      item: AuthorizedNamespaceDelegation
  )(implicit traceContext: TraceContext): Unit = {
    ErrorUtil.requireArgument(
      item.mapping.namespace == namespace,
      s"unable to add namespace delegation for ${item.mapping.namespace} to graph for $namespace",
    )
    ErrorUtil.requireArgument(
      item.operation == Replace,
      s"unable to add namespace delegation with operation ${item.operation} to graph for $namespace",
    )
    val targetFingerprint = item.mapping.target.fingerprint
    // if the node already exists, remove all authorizing edges from item.signingKeys to item.target
    // to not leak previous authorizations
    if (graph.nodes().contains(targetFingerprint))
      doRemove(item)

    // add authorizing edges from item.signingKeys to item.target
    item.signingKeys.foreach { authKey =>
      graph.putEdgeValue(authKey, targetFingerprint, item).discard
    }
  }

  /** Remove all mappings with the same target key from this graph.
    *
    * @throws java.lang.IllegalArgumentException
    *   if `item` does not refer to `namespace` or the operation is not REMOVE.
    */
  def remove(item: AuthorizedNamespaceDelegation)(implicit traceContext: TraceContext): Unit = {
    ErrorUtil.requireArgument(
      item.mapping.namespace == namespace,
      s"unable to remove namespace delegation for ${item.mapping.namespace} from graph for $namespace",
    )
    ErrorUtil.requireArgument(
      item.operation == Remove,
      s"unable to remove namespace delegation with operation ${item.operation} from graph for $namespace",
    )

    doRemove(item)
    recompute()
  }

  /** remove a namespace delegation
    *
    * This is done by removing all incoming edges (predecessors) to item.target, and possibly also
    * removing the node itself if it doesn't have any outgoing authorizations
    */
  private def doRemove(
      item: AuthorizedNamespaceDelegation
  )(implicit traceContext: TraceContext): Unit = {
    val keyToRemove = item.mapping.target.fingerprint
    if (graph.nodes().contains(keyToRemove)) {
      // The java.util.Set returned by predecessors is backed by the graph.
      // Therefore we convert it into an immutable scala Set, so that removeEdge
      // doesn't cause a ConcurrentModificationException
      val predecessors = graph
        .predecessors(keyToRemove)
        .asScala
        .toSet[Fingerprint]

      // remove all edges that have the same target key fingerprint as item
      predecessors.foreach(graph.removeEdge(_, keyToRemove).discard)

      // Remove nodes without edges
      (predecessors + keyToRemove).foreach { node =>
        if (graph.degree(node) == 0) graph.removeNode(node).discard
      }
    } else {
      logger.warn(s"Superfluous removal of namespace delegation $item")
    }
  }

  private def rootNode: Option[AuthorizedNamespaceDelegation] =
    Option
      .when(graph.nodes().contains(namespace.fingerprint))(
        graph.edgeValue(namespace.fingerprint, namespace.fingerprint).toScala
      )
      .flatten

  /** Recompute the authorization graph starting from the root certificate: We start at the root
    * certificate and follow outgoing authorizations for all delegations that can sign
    * NamespaceDelegations. As a result, every key that doesn't end up in the cache is not connected
    * to the root certificate and therefore useless.
    */
  protected def recompute()(implicit traceContext: TraceContext): Unit = {
    cache.clear()
    def go(
        incoming: AuthorizedNamespaceDelegation,
        level: Int,
    ): Unit = {
      val fingerprint = incoming.mapping.target.fingerprint
      // only proceed if we haven't seen this fingerprint yet,
      // so we terminate even if the graph has cycles.
      if (!cache.contains(fingerprint)) {
        cache.update(fingerprint, (incoming, level))
        // only look at outgoing authorizations if item can sign other NamespaceDelegations
        if (incoming.mapping.canSign(Code.NamespaceDelegation)) {
          for {
            // find the keys authorized by item.target
            authorizedKey <- graph.successors(fingerprint).asScala
            // look up the authorization via the edge fingerprint -> authorizedKey
            outgoingAuthorization <- graph.edgeValue(fingerprint, authorizedKey).toScala
          } {
            // descend into all outgoing authorizations
            go(outgoingAuthorization, level + 1)
          }

        }
      }
    }

    // start at the root node, if it exists
    rootNode.foreach(go(_, level = 1))

    report()
  }

  def report()(implicit traceContext: TraceContext): Unit = {
    if (rootNode.isEmpty) {
      logger.debug(
        s"Namespace $namespace has no root node, therefore no namespace delegation is authorized."
      )
    }
    /* Only nodes that have an incoming edge are considered dangling:
    Consider the following:
    t0:
      k1 -- NSD(ns1, target=k1, signed=k1) --> k1
      k1 -- NSD(ns1, target=k2, signed=k1) --> k2
      k2 -- NSD(ns1, target=k3, signed=k2) --> k3

    t1: we remove the namespace delegation for k2:
      k1 -- NSD(ns1, target=k1, signed=k1) --> k1
      k2 -- NSD(ns1, target=k3, signed=k2) --> k3

    We see that we still have the node k2, but only for the purpose of maintaining the edge k2->k3.
    Since there is no more NSD with target=k2, we don't actually consider k2 dangling.
     */
    val dangling = graph.nodes().asScala.diff(cache.keySet).filter(!graph.predecessors(_).isEmpty)
    if (dangling.nonEmpty) {
      logger.warn(
        s"The following target keys of namespace $namespace are dangling: ${dangling.toList.sorted}"
      )
    }
    if (cache.nonEmpty) {
      if (extraDebugInfo && logger.underlying.isDebugEnabled) {
        val str =
          cache.values
            .map { case (nsd, _) =>
              show"auth=${nsd.signingKeys}, target=${nsd.mapping.target.fingerprint}, canSignNSD=${nsd.mapping
                  .canSign(Code.NamespaceDelegation)}"
            }
            .mkString("\n  ")
        logger.debug(s"The authorization graph is given by:\n  $str")
      }
    }
  }

  override def existsAuthorizedKeyIn(
      authKeys: Set[Fingerprint],
      mappingToAuthorize: TopologyMapping.Code,
  ): Boolean = authKeys.exists(getAuthorizedKey(_, mappingToAuthorize).nonEmpty)

  private def getAuthorizedKey(
      authKey: Fingerprint,
      mappingToAuthorize: TopologyMapping.Code,
  ): Option[SigningPublicKey] =
    cache
      .get(authKey)
      .map { case (delegation, _) => delegation }
      .filter(_.mapping.canSign(mappingToAuthorize))
      .map(_.mapping.target)

  override def keysSupportingAuthorization(
      authKeys: Set[Fingerprint],
      mappingToAuthorize: TopologyMapping.Code,
  ): Set[SigningPublicKey] = authKeys.flatMap(getAuthorizedKey(_, mappingToAuthorize))

  def authorizedDelegations(): Map[Namespace, Seq[(AuthorizedNamespaceDelegation, Int)]] =
    Map(namespace -> cache.values.toSeq)

  override def toString: String = s"AuthorizationGraph($namespace)"
}

trait AuthorizationCheck {

  /** Determines if a subset of the given keys is authorized to sign a given mapping type on behalf
    * of the (possibly decentralized) namespace.
    *
    * @param mappingToAuthorize
    *   the Code of the mapping that needs to be authorized.
    */
  def existsAuthorizedKeyIn(
      authKeys: Set[Fingerprint],
      mappingToAuthorize: TopologyMapping.Code,
  ): Boolean

  /** Returns those keys that are useful for signing on behalf of the (possibly decentralized)
    * namespace. Only keys with fingerprint in `authKeys` will be returned. The returned keys are
    * not necessarily sufficient to authorize a transaction on behalf of the namespace; in case of a
    * decentralized namespace, additional signatures may be required. Only returns keys that are
    * permitted to sign the provided mapping type.
    */
  def keysSupportingAuthorization(
      authKeys: Set[Fingerprint],
      mappingToAuthorize: TopologyMapping.Code,
  ): Set[SigningPublicKey]

  /** Per namespace (required for decentralized namespaces), a list of namespace delegations that
    * have a gapless chain to the root certificate together with the length of the chain to the root
    * certificate for each namespace delegation.
    */
  def authorizedDelegations(): Map[Namespace, Seq[(AuthorizedNamespaceDelegation, Int)]]
}

/** Authorization graph for a decentralized namespace.
  *
  * @throws java.lang.IllegalArgumentException
  *   if `dnd` and `ownerGraphs` refer to different namespaces.
  */
final case class DecentralizedNamespaceAuthorizationGraph(
    dnd: DecentralizedNamespaceDefinition,
    ownerGraphs: Seq[AuthorizationGraph],
) extends AuthorizationCheck {

  require(
    dnd.owners.forgetNE == ownerGraphs.map(_.namespace).toSet,
    s"The owner graphs refer to the wrong namespaces (expected: ${dnd.owners}), actual: ${ownerGraphs
        .map(_.namespace)}).",
  )

  override def existsAuthorizedKeyIn(
      authKeys: Set[Fingerprint],
      mappingToAuthorize: TopologyMapping.Code,
  ): Boolean =
    ownerGraphs.count(_.existsAuthorizedKeyIn(authKeys, mappingToAuthorize)) >= dnd.threshold.value

  override def keysSupportingAuthorization(
      authKeys: Set[Fingerprint],
      mappingToAuthorize: TopologyMapping.Code,
  ): Set[SigningPublicKey] =
    ownerGraphs
      .flatMap(_.keysSupportingAuthorization(authKeys, mappingToAuthorize))
      .toSet

  override def authorizedDelegations(): Map[Namespace, Seq[(AuthorizedNamespaceDelegation, Int)]] =
    ownerGraphs.flatMap(graph => graph.authorizedDelegations()).toMap
}
