// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.instances.order.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Fingerprint, SigningPublicKey}
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Namespace
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.{
  AuthorizedNamespaceDelegation,
  isRootDelegation,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Remove, Replace}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.google.common.graph.ValueGraphBuilder

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

  /** Returns true if the namespace delegation is a root certificate
    *
    * A root certificate is defined by a namespace delegation that authorizes the
    * key f to act on the namespace spanned by f, authorized by f.
    */
  def isRootCertificate(namespaceDelegation: AuthorizedNamespaceDelegation): Boolean = {
    NamespaceDelegation.isRootCertificate(namespaceDelegation.transaction)
  }

  /** Returns true if the namespace delegation is a root certificate or a root delegation
    *
    * A root delegation is a namespace delegation whose target key may be used to authorize other namespace delegations.
    */
  def isRootDelegation(namespaceDelegation: AuthorizedNamespaceDelegation): Boolean = {
    NamespaceDelegation.isRootDelegation(namespaceDelegation.transaction)
  }

}

/** Stores a set of namespace delegations, tracks dependencies and
  * determines which keys are authorized to sign on behalf of a namespace.
  *
  * Namespace delegations are a bit tricky as there can be an arbitrary number of delegations between the namespace key
  * and the key that will be used for authorizations. Think of it as a certificate chain where we get a
  * series of certificates and we need to figure out a path from one certificate to the root certificate.
  *
  * NOTE: this class is not thread-safe
  *
  * Properties of the graph:
  *   - Each node corresponds to a target key
  *   - The node with key fingerprint of the namespace is the root node
  *   - The edges between nodes are namespace delegations.
  *     If key A signs a namespace delegation with target key B, then key A authorizes key B to act on the namespace.
  *     In this case, the edge is outgoing from node A and incoming into node B.
  *   - The graph may have cycles. The implementation does not get confused by this.
  *
  * Computation task:
  * The graph maintains a set of nodes that are connected to the root node. Those correspond to the keys that are
  * authorized to sign on behalf of the namespace.
  *
  * Limitation: clients need to ensure that the namespace delegations added have valid signatures.
  * If delegations with invalid signatures are added, authorization will break.
  *
  * @param extraDebugInfo whether to log the authorization graph at debug level on every recomputation
  */
class AuthorizationGraph(
    val namespace: Namespace,
    extraDebugInfo: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
) extends AuthorizationCheck
    with NamedLogging {

  /** Use guava's directed ValueGraph to manage authorizations:
    * <ul>
    *   <li>Nodes are the fingerprints of keys</li>
    *   <li>Edges `from -NSD-> to` represent NamespaceDelegations
    *     <ul>
    *       <li>`from` the signing keys of NamespaceDelegation</li>
    *       <li>`to` the target key of the NamespaceDelegation</li>
    *     </ul>
    *   </li>
    *   <li>All edges incoming to `to` are labelled with the same NSD.</li>
    * </ul>
    */
  private val graph = ValueGraphBuilder
    .directed()
    .allowsSelfLoops(true) // we allow self loops for the root certificate
    .build[Fingerprint, AuthorizedNamespaceDelegation]

  /** Authorized namespace delegations for namespace `this.namespace`, grouped by target */
  private val cache =
    new TrieMap[Fingerprint, AuthorizedNamespaceDelegation]()

  /** Check if `item` is authorized and, if so, add its mapping to this graph.
    *
    * @throws java.lang.IllegalArgumentException if `item` does not refer to `namespace` or the operation is not REPLACE.
    */
  def add(item: AuthorizedNamespaceDelegation)(implicit traceContext: TraceContext): Boolean = {
    assertNamespaceAndOperation(item)
    if (
      AuthorizedTopologyTransaction.isRootCertificate(item) ||
      this.existsAuthorizedKeyIn(item.signingKeys, requireRoot = true)
    ) {
      doAdd(item)
      recompute()
      true
    } else false
  }

  /** Add the mappings in `items` to this graph, regardless if they are authorized or not.
    * If an unauthorized namespace delegation is added to the graph, the graph will contain nodes that are not connected to the root.
    * The target key of the unauthorized delegation will still be considered unauthorized.
    *
    * @throws java.lang.IllegalArgumentException if `item` does not refer to `namespace` or the operation is not REPLACE.
    */
  def unauthorizedAdd(
      items: Seq[AuthorizedNamespaceDelegation]
  )(implicit traceContext: TraceContext): Unit = {
    items.foreach(doAdd)
    recompute()
  }

  private def doAdd(
      item: AuthorizedNamespaceDelegation
  )(implicit traceContext: TraceContext): Unit = {
    assertNamespaceAndOperation(item)
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

  private def assertNamespaceAndOperation(
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
  }

  /** Check if `item` is authorized and, if so, remove all mappings with the same target key from this graph.
    * Note that addition and removal of a namespace delegation can be authorized by different keys.
    *
    * @throws java.lang.IllegalArgumentException if `item` does not refer to `namespace` or the operation is not REMOVE.
    */
  def remove(item: AuthorizedNamespaceDelegation)(implicit traceContext: TraceContext): Boolean = {
    ErrorUtil.requireArgument(
      item.mapping.namespace == namespace,
      s"unable to remove namespace delegation for ${item.mapping.namespace} from graph for $namespace",
    )

    ErrorUtil.requireArgument(
      item.operation == Remove,
      s"unable to remove namespace delegation with operation ${item.operation} from graph for $namespace",
    )

    if (existsAuthorizedKeyIn(item.signingKeys, requireRoot = true)) {
      doRemove(item)
      recompute()
      true
    } else false
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
      // remove all edges labelled with item
      graph
        .predecessors(keyToRemove)
        .asScala
        // The java.util.Set returned by predecessors is backed by the graph.
        // Therefore we convert it into an immutable scala Set, so that removeEdge
        // doesn't cause a ConcurrentModificationException
        .toSet[Fingerprint]
        .foreach {
          graph.removeEdge(_, keyToRemove).discard
        }

      // if item.target has no outgoing authorizations, remove it from the graph altogether
      if (graph.outDegree(keyToRemove) == 0) {
        graph.removeNode(keyToRemove).discard
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

  /** Recompute the authorization graph starting from the root certificate:
    * We start at the root certificate and follow outgoing authorizations for all root delegations.
    * As a result, every key that doesn't end up in the cache is not connected
    * to the root certificate and therefore useless.
    */
  protected def recompute()(implicit traceContext: TraceContext): Unit = {
    cache.clear()
    def go(
        incoming: AuthorizedNamespaceDelegation
    ): Unit = {
      val fingerprint = incoming.mapping.target.fingerprint
      // only proceed if we haven't seen this fingerprint yet,
      // so we terminate even if the graph has cycles.
      if (!cache.contains(fingerprint)) {
        cache.update(fingerprint, incoming)
        // only look at outgoing authorizations if item is a root delegation
        if (isRootDelegation(incoming)) {
          for {
            // find the keys authorized by item.target
            authorizedKey <- graph.successors(fingerprint).asScala
            // look up the authorization via the edge fingerprint -> authorizedKey
            outgoingAuthorization <- graph.edgeValue(fingerprint, authorizedKey).toScala
          } {
            // descend into all outgoing authorizations
            go(outgoingAuthorization)
          }

        }
      }
    }

    // start at the root node, if it exists
    rootNode.foreach(go)

    report()
  }

  def report()(implicit traceContext: TraceContext): Unit = {
    if (rootNode.isEmpty) {
      logger.debug(
        s"Namespace $namespace has no root node, therefore no namespace delegation is authorized."
      )
    }
    val dangling = graph.nodes().asScala.diff(cache.keySet)
    if (dangling.nonEmpty) {
      logger.warn(
        s"The following target keys of namespace $namespace are dangling: ${dangling.toList.sorted}"
      )
    }
    if (cache.nonEmpty) {
      if (extraDebugInfo && logger.underlying.isDebugEnabled) {
        val str =
          cache.values
            .map(nsd =>
              show"auth=${nsd.signingKeys}, target=${nsd.mapping.target.fingerprint}, root=${AuthorizedTopologyTransaction
                  .isRootDelegation(nsd)}"
            )
            .mkString("\n  ")
        logger.debug(s"The authorization graph is given by:\n  $str")
      }
    }
  }

  override def existsAuthorizedKeyIn(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Boolean = authKeys.exists(getAuthorizedKey(_, requireRoot).nonEmpty)

  private def getAuthorizedKey(
      authKey: Fingerprint,
      requireRoot: Boolean,
  ): Option[SigningPublicKey] =
    cache
      .get(authKey)
      .filter { delegation =>
        isRootDelegation(delegation) || !requireRoot
      }
      .map(_.mapping.target)

  override def keysSupportingAuthorization(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Set[SigningPublicKey] = authKeys.flatMap(getAuthorizedKey(_, requireRoot))

  override def toString: String = s"AuthorizationGraph($namespace)"
}

trait AuthorizationCheck {

  /** Determines if a subset of the given keys is authorized to sign on behalf of the (possibly decentralized) namespace.
    *
    * @param requireRoot whether the authorization must be suitable to authorize namespace delegations
    */
  def existsAuthorizedKeyIn(authKeys: Set[Fingerprint], requireRoot: Boolean): Boolean

  /** Returns those keys that are useful for signing on behalf of the (possibly decentralized) namespace.
    * Only keys with fingerprint in `authKeys` will be returned.
    * The returned keys are not necessarily sufficient to authorize a transaction on behalf of the namespace;
    * in case of a decentralized namespace, additional signatures may be required.
    */
  def keysSupportingAuthorization(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Set[SigningPublicKey]
}

object AuthorizationCheck {
  val empty: AuthorizationCheck = new AuthorizationCheck {
    override def existsAuthorizedKeyIn(
        authKeys: Set[Fingerprint],
        requireRoot: Boolean,
    ): Boolean = false

    override def keysSupportingAuthorization(
        authKeys: Set[Fingerprint],
        requireRoot: Boolean,
    ): Set[SigningPublicKey] = Set.empty

    override def toString: String = "AuthorizationCheck.empty"
  }
}

/** Authorization graph for a decentralized namespace.
  *
  * @throws java.lang.IllegalArgumentException if `dnd` and `direct` refer to different namespaces.
  */
final case class DecentralizedNamespaceAuthorizationGraph(
    dnd: DecentralizedNamespaceDefinition,
    ownerGraphs: Seq[AuthorizationGraph],
) extends AuthorizationCheck {

  override def existsAuthorizedKeyIn(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Boolean = {
    ownerGraphs.count(_.existsAuthorizedKeyIn(authKeys, requireRoot)) >= dnd.threshold.value
  }

  override def keysSupportingAuthorization(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Set[SigningPublicKey] = {
    ownerGraphs
      .flatMap(_.keysSupportingAuthorization(authKeys, requireRoot))
      .toSet
  }
}
