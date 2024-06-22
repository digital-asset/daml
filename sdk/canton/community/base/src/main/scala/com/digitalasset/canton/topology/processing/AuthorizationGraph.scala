// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Fingerprint, SigningPublicKey}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Namespace
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransaction.AuthorizedNamespaceDelegation
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Remove, Replace}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.collection.concurrent.TrieMap
import scala.math.Ordering.Implicits.*

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

  /** @param root the last active root certificate for `target`
    * @param outgoing all active namespace delegations (excluding root certificates) authorized by `target`
    * @param incoming all active namespace delegations for the namespace `target`
    *
    * All namespace delegations are for namespace `this.namespace`.
    */
  private case class GraphNode(
      target: Fingerprint,
      root: Option[AuthorizedNamespaceDelegation] = None,
      outgoing: Set[AuthorizedNamespaceDelegation] = Set(),
      incoming: Set[AuthorizedNamespaceDelegation] = Set(),
  ) {

    def isEmpty: Boolean = root.isEmpty && outgoing.isEmpty && incoming.isEmpty

  }

  private abstract class AuthLevel(val isAuth: Boolean, val isRoot: Boolean)
  private object AuthLevel {

    private object NotAuthorized extends AuthLevel(false, false)
    private object Standard extends AuthLevel(true, false)
    private object RootDelegation extends AuthLevel(true, true)

    implicit val orderingAuthLevel: Ordering[AuthLevel] =
      Ordering.by[AuthLevel, Int](authl => Seq(authl.isAuth, authl.isRoot).count(identity))

    def fromDelegationO(delegation: Option[AuthorizedNamespaceDelegation]): AuthLevel =
      delegation match {
        case None => AuthLevel.NotAuthorized
        case Some(item) if item.mapping.isRootDelegation => RootDelegation
        case Some(_) => Standard
      }

  }

  /** GraphNodes by GraphNode.target */
  private val nodes = new TrieMap[Fingerprint, GraphNode]()

  /** Authorized namespace delegations for namespace `this.namespace`, grouped by target */
  private val cache =
    new TrieMap[Fingerprint, AuthorizedNamespaceDelegation]()

  /** Check if `item` is authorized and, if so, add its mapping to this graph.
    *
    * @throws java.lang.IllegalArgumentException if `item` does not refer to `namespace` or the operation is not REPLACE.
    */
  def add(item: AuthorizedNamespaceDelegation)(implicit traceContext: TraceContext): Boolean = {
    ErrorUtil.requireArgument(
      item.mapping.namespace == namespace,
      s"unable to add namespace delegation for ${item.mapping.namespace} to graph for $namespace",
    )
    ErrorUtil.requireArgument(
      item.operation == Replace,
      s"unable to add namespace delegation with operation ${item.operation} to graph for $namespace",
    )

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
    ErrorUtil.requireArgument(
      item.mapping.namespace == namespace,
      s"unable to add namespace delegation for ${item.mapping.namespace} to graph for $namespace",
    )
    ErrorUtil.requireArgument(
      item.operation == Replace,
      s"unable to add namespace delegation with operation ${item.operation} to graph for $namespace",
    )

    val targetKey = item.mapping.target.fingerprint
    val curTarget = nodes.getOrElse(targetKey, GraphNode(targetKey))
    // if this is a root certificate, remember it separately
    if (AuthorizedTopologyTransaction.isRootCertificate(item)) {
      ErrorUtil.requireState(
        curTarget.root.forall(_ == item),
        s"Trying to add a root certificate for $namespace that differs from a previously added root certificate.\nKnown=[${curTarget.root}]\nToAdd=[$item]",
      )
      nodes.update(targetKey, curTarget.copy(root = Some(item)))
    } else {
      item.signingKeys.foreach { authKey =>
        val curAuth = nodes.getOrElse(authKey, GraphNode(authKey))
        nodes.update(authKey, curAuth.copy(outgoing = curAuth.outgoing + item))
      }
      nodes.update(targetKey, curTarget.copy(incoming = curTarget.incoming + item))
    }
  }

  /** Check if `item` is authorized and, if so, remove its mapping from this graph.
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
      true
    } else false
  }

  /** remove a namespace delegation
    *
    * The implementation is a bit tricky as the removal might have been authorized
    * by a different key than the addition. This complicates the book-keeping,
    * as we need to track for each target key what the "incoming authorizations" were solely for the
    * purpose of being able to clean them up.
    */
  private def doRemove(
      item: AuthorizedNamespaceDelegation
  )(implicit traceContext: TraceContext): Unit = {
    def myFilter(existing: AuthorizedNamespaceDelegation): Boolean = {
      // the auth key doesn't need to match on removals
      existing.mapping != item.mapping
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
      node.incoming.flatMap(_.signingKeys).foreach { fp =>
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

      case None => logger.warn(s"Superfluous removal of namespace delegation $item")
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
    def go(
        fingerprint: Fingerprint,
        incoming: AuthorizedNamespaceDelegation,
    ): Unit = {
      val current = cache.get(fingerprint)
      val currentLevel = AuthLevel.fromDelegationO(current)
      val incomingLevel = AuthLevel.fromDelegationO(Some(incoming))
      // this inherited level is higher than current, propagate it
      if (incomingLevel > currentLevel) {
        cache.update(fingerprint, incoming)
        // get the graph node of this fingerprint
        nodes.get(fingerprint).foreach { graphNode =>
          // iterate through all edges that depart from this node
          graphNode.outgoing
            .map(x => (AuthLevel.fromDelegationO(Some(x)), x))
            // only propagate edges that require lower or equal authorization level than what we have from incoming
            // so an outgoing root delegation can not be authorized by an incoming non-root delegation
            .filter { case (outgoingLevel, _) => outgoingLevel <= incomingLevel }
            .foreach {
              // iterate through all target fingerprint, taking the edge outgoing from this node as the incoming
              case (_, outgoing) =>
                go(outgoing.mapping.target.fingerprint, incoming = outgoing)
            }
        }
      }
    }
    for {
      // start iterating from root certificates for this namespace
      graph <- nodes.get(namespace.fingerprint)
      root <- graph.root
    } {
      go(namespace.fingerprint, root)
    }

    report()
  }

  def report()(implicit traceContext: TraceContext): Unit =
    if (nodes.get(namespace.fingerprint).flatMap(_.root).isDefined) {
      val dangling = nodes.keySet.diff(cache.keySet)
      if (dangling.nonEmpty) {
        logger.warn(s"The following target keys of namespace $namespace are dangling: $dangling")
      }
      if (extraDebugInfo && logger.underlying.isDebugEnabled) {
        val str =
          cache.values
            .map(aud =>
              show"auth=${aud.signingKeys}, target=${aud.mapping.target.fingerprint}, root=${AuthorizedTopologyTransaction
                  .isRootCertificate(aud)}"
            )
            .mkString("\n  ")
        logger.debug(s"The authorization graph is given by:\n  $str")
      }
    } else
      logger.debug(
        s"Namespace $namespace has no root certificate, making all ${nodes.size} un-authorized"
      )

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
        val authLevel = AuthLevel.fromDelegationO(Some(delegation))
        authLevel.isRoot || (authLevel.isAuth && !requireRoot)
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
    direct: AuthorizationGraph,
    ownerGraphs: Seq[AuthorizationGraph],
) extends AuthorizationCheck {
  require(
    dnd.namespace == direct.namespace,
    s"The direct graph refers to the wrong namespace (expected: ${dnd.namespace}, actual: ${direct.namespace}).",
  )

  override def existsAuthorizedKeyIn(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Boolean = {
    val viaNamespaceDelegation = direct.existsAuthorizedKeyIn(authKeys, requireRoot)
    val viaCollective =
      ownerGraphs.count(_.existsAuthorizedKeyIn(authKeys, requireRoot)) >= dnd.threshold.value
    viaNamespaceDelegation || viaCollective
  }

  override def keysSupportingAuthorization(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Set[SigningPublicKey] = {
    (direct +: ownerGraphs)
      .flatMap(_.keysSupportingAuthorization(authKeys, requireRoot))
      .toSet
  }
}
