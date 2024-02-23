// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.{Fingerprint, SigningPublicKey}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.Namespace
import com.digitalasset.canton.topology.processing.AuthorizedTopologyTransactionX.AuthorizedNamespaceDelegationX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil.*

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.math.Ordering.Implicits.*

/** An authorized topology transaction */
final case class AuthorizedTopologyTransactionX[T <: TopologyMappingX](
    signedTransaction: SignedTopologyTransactionX[TopologyChangeOpX, T]
) {
  def mapping: T = signedTransaction.transaction.mapping
  def signingKeys: NonEmpty[Set[Fingerprint]] = signedTransaction.signatures.map(_.signedBy)
}

object AuthorizedTopologyTransactionX {

  type AuthorizedNamespaceDelegationX = AuthorizedTopologyTransactionX[NamespaceDelegationX]
  type AuthorizedIdentifierDelegationX = AuthorizedTopologyTransactionX[IdentifierDelegationX]
  type AuthorizedDecentralizedNamespaceDefinitionX =
    AuthorizedTopologyTransactionX[DecentralizedNamespaceDefinitionX]

  /** Returns true if the namespace delegation is a root certificate
    *
    * A root certificate is defined by the namespace delegation that authorizes the
    * key f to act on namespace spanned by f, authorized by f.
    */
  def isRootCertificate(namespaceDelegation: AuthorizedNamespaceDelegationX): Boolean = {
    NamespaceDelegationX.isRootCertificate(namespaceDelegation.signedTransaction)
  }

  /** Returns true if the namespace delegation is a root certificate or a root delegation
    *
    * A root certificate is defined by the namespace delegation that authorizes the
    * key f to act on namespace spanned by f, authorized by f.
    *
    * A root delegation is defined by the namespace delegation the authorizes the
    * key g to act on namespace spanned by f.
    */
  def isRootDelegation(namespaceDelegation: AuthorizedNamespaceDelegationX): Boolean = {
    NamespaceDelegationX.isRootDelegation(namespaceDelegation.signedTransaction)
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
  *   - the graph SHOULD be a directed acyclic graph, but we MIGHT have cycles (i.e. key A authorizing B, B authorizing A).
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
  *     this information to find out which resulting mapping is properly authorized and which one is not.
  *
  * authorization checks:
  *   - when adding "single transactions", we do check that the transaction is properly authorized. otherwise we
  *     "ignore" it (returning false). this is used during processing.
  *   - when adding "batch transactions", we don't check that all of them are properly authorized, as we do allow
  *     temporarily "nodes" to be unauthorized (so that errors can be fixed by adding a replacement certificate)
  *   - when removing transactions, we do check that the authorizing key is authorized. but note that the authorizing
  *     key of an edge REMOVAL doesn't need to match the key used to authorized the ADD.
  */
class AuthorizationGraphX(
    val namespace: Namespace,
    extraDebugInfo: Boolean,
    val loggerFactory: NamedLoggerFactory,
) extends AuthorizationCheckX
    with NamedLogging {

  private case class GraphNode(
      target: Fingerprint,
      root: Option[AuthorizedNamespaceDelegationX] = None,
      outgoing: Set[AuthorizedNamespaceDelegationX] = Set(),
      incoming: Set[AuthorizedNamespaceDelegationX] = Set(),
  ) {

    def isEmpty: Boolean = root.isEmpty && outgoing.isEmpty && incoming.isEmpty

  }

  private abstract class AuthLevel(val isAuth: Boolean, val isRoot: Boolean)
  private object AuthLevel {

    object NotAuthorized extends AuthLevel(false, false)
    object Standard extends AuthLevel(true, false)
    object RootDelegation extends AuthLevel(true, true)

    implicit val orderingAuthLevel: Ordering[AuthLevel] =
      Ordering.by[AuthLevel, Int](authl => Seq(authl.isAuth, authl.isRoot).count(identity))

    def fromDelegationO(delegation: Option[AuthorizedNamespaceDelegationX]): AuthLevel =
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
  private val cache =
    new TrieMap[Fingerprint, Option[AuthorizedNamespaceDelegationX]]()

  def add(item: AuthorizedNamespaceDelegationX)(implicit traceContext: TraceContext): Boolean = {
    ErrorUtil.requireArgument(
      item.mapping.namespace == namespace,
      s"added namespace ${item.mapping.namespace} to $namespace",
    )
    if (
      AuthorizedTopologyTransactionX.isRootCertificate(item) ||
      this.areValidAuthorizationKeys(item.signingKeys, requireRoot = true)
    ) {
      doAdd(item)
      recompute()
      true
    } else false
  }

  def unauthorizedAdd(
      items: Seq[AuthorizedNamespaceDelegationX]
  )(implicit traceContext: TraceContext): Unit = {
    items.foreach(doAdd)
    recompute()
  }

  private def doAdd(
      item: AuthorizedNamespaceDelegationX
  )(implicit traceContext: TraceContext): Unit = {
    val targetKey = item.mapping.target.fingerprint
    val curTarget = nodes.getOrElse(targetKey, GraphNode(targetKey))
    // if this is a root certificate, remember it separately
    if (AuthorizedTopologyTransactionX.isRootCertificate(item)) {
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

  def remove(item: AuthorizedNamespaceDelegationX)(implicit traceContext: TraceContext): Boolean =
    if (areValidAuthorizationKeys(item.signingKeys, requireRoot = true)) {
      doRemove(item)
      true
    } else false

  def unauthorizedRemove(
      items: Seq[AuthorizedNamespaceDelegationX]
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
      item: AuthorizedNamespaceDelegationX
  )(implicit traceContext: TraceContext): Unit = {
    ErrorUtil.requireArgument(
      item.mapping.namespace == namespace,
      s"removing namespace ${item.mapping.namespace} from $namespace",
    )
    def myFilter(existing: AuthorizedNamespaceDelegationX): Boolean = {
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
        if (AuthorizedTopologyTransactionX.isRootCertificate(item)) {
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
    def go(
        fingerprint: Fingerprint,
        incoming: AuthorizedNamespaceDelegationX,
    ): Unit = {
      val current = cache.getOrElseUpdate(fingerprint, None)
      val currentLevel = AuthLevel.fromDelegationO(current)
      val incomingLevel = AuthLevel.fromDelegationO(Some(incoming))
      // this inherited level is higher than current, propagate it
      if (incomingLevel > currentLevel) {
        cache.update(fingerprint, Some(incoming))
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
          authorizedDelegations()
            .map(aud =>
              show"auth=${aud.signingKeys}, target=${aud.mapping.target.fingerprint}, root=${AuthorizedTopologyTransactionX
                  .isRootCertificate(aud)}"
            )
            .mkString("\n  ")
        logger.debug(s"The authorization graph is given by:\n  $str")
      }
    } else
      logger.debug(
        s"Namespace ${namespace} has no root certificate, making all ${nodes.size} un-authorized"
      )

  override def areValidAuthorizationKeys(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Boolean = {
    authKeys.exists { authKey =>
      val authLevel = AuthLevel.fromDelegationO(cache.getOrElse(authKey, None))
      authLevel.isRoot || (authLevel.isAuth && !requireRoot)
    }
  }

  override def getValidAuthorizationKeys(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Set[SigningPublicKey] = authKeys.flatMap(authKey =>
    cache
      .getOrElse(authKey, None)
      .map(_.mapping.target)
      .filter(_ => areValidAuthorizationKeys(Set(authKey), requireRoot))
  )

  def authorizationChain(
      startAuthKey: Fingerprint,
      requireRoot: Boolean,
  ): Option[AuthorizationChainX] = {
    @tailrec
    def go(
        authKey: Fingerprint,
        requireRoot: Boolean,
        acc: List[AuthorizedNamespaceDelegationX],
    ): List[AuthorizedNamespaceDelegationX] = {
      cache.getOrElse(authKey, None) match {
        // we've terminated with the root certificate
        case Some(delegation) if AuthorizedTopologyTransactionX.isRootCertificate(delegation) =>
          delegation :: acc
        // cert is valid, append it
        case Some(delegation) if delegation.mapping.isRootDelegation || !requireRoot =>
          go(delegation.signingKeys.head1, delegation.mapping.isRootDelegation, delegation :: acc)
        // return empty to indicate failure
        case _ => List.empty
      }
    }
    go(startAuthKey, requireRoot, List.empty) match {
      case Nil => None
      case rest =>
        Some(
          AuthorizationChainX(
            identifierDelegation = Seq.empty,
            namespaceDelegations = rest,
            Seq.empty,
          )
        )
    }
  }

  def authorizedDelegations(): Seq[AuthorizedNamespaceDelegationX] =
    cache.values.flatMap(_.toList).toSeq

  override def toString: String = s"AuthorizationGraphX($namespace)"

  def debugInfo() = s"$namespace => ${nodes.mkString("\n")}"
}

trait AuthorizationCheckX {
  def areValidAuthorizationKeys(authKeys: Set[Fingerprint], requireRoot: Boolean): Boolean

  def getValidAuthorizationKeys(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Set[SigningPublicKey]

  def authorizationChain(
      startAuthKey: Fingerprint,
      requireRoot: Boolean,
  ): Option[AuthorizationChainX]

  def authorizedDelegations(): Seq[AuthorizedNamespaceDelegationX]
}

object AuthorizationCheckX {
  val empty = new AuthorizationCheckX {
    override def areValidAuthorizationKeys(
        authKeys: Set[Fingerprint],
        requireRoot: Boolean,
    ): Boolean = false

    override def authorizationChain(
        startAuthKey: Fingerprint,
        requireRoot: Boolean,
    ): Option[AuthorizationChainX] = None

    override def getValidAuthorizationKeys(
        authKeys: Set[Fingerprint],
        requireRoot: Boolean,
    ): Set[SigningPublicKey] = Set.empty

    override def authorizedDelegations(): Seq[AuthorizedNamespaceDelegationX] = Seq.empty

    override def toString: String = "AuthorizationCheckX.empty"
  }
}

final case class DecentralizedNamespaceAuthorizationGraphX(
    dnd: DecentralizedNamespaceDefinitionX,
    direct: AuthorizationGraphX,
    ownerGraphs: Seq[AuthorizationGraphX],
) extends AuthorizationCheckX {
  override def areValidAuthorizationKeys(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Boolean = {
    val viaNamespaceDelegation = direct.areValidAuthorizationKeys(authKeys, requireRoot)
    val viaCollective =
      ownerGraphs.count(_.areValidAuthorizationKeys(authKeys, requireRoot)) >= dnd.threshold.value
    viaNamespaceDelegation || viaCollective
  }

  import cats.syntax.foldable.*

  override def getValidAuthorizationKeys(
      authKeys: Set[Fingerprint],
      requireRoot: Boolean,
  ): Set[SigningPublicKey] = {
    (direct +: ownerGraphs)
      .flatMap(_.getValidAuthorizationKeys(authKeys, requireRoot))
      .toSet
  }

  override def authorizationChain(
      startAuthKey: Fingerprint,
      requireRoot: Boolean,
  ): Option[AuthorizationChainX] =
    direct
      .authorizationChain(startAuthKey, requireRoot)
      .orElse(ownerGraphs.map(_.authorizationChain(startAuthKey, requireRoot)).combineAll)

  override def authorizedDelegations(): Seq[AuthorizedNamespaceDelegationX] =
    direct.authorizedDelegations() ++ ownerGraphs.flatMap(_.authorizedDelegations())
}
