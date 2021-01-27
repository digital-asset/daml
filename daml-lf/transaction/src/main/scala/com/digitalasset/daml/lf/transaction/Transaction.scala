// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.ledger.FailedAuthorization
import com.daml.lf.transaction.Node.GenNode
import com.daml.lf.value.Value
import scalaz.Equal

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

final case class VersionedTransaction[Nid, +Cid] private[lf] (
    version: TransactionVersion,
    nodes: Map[Nid, GenNode[Nid, Cid]],
    override val roots: ImmArray[Nid],
) extends HasTxNodes[Nid, Cid]
    with value.CidContainer[VersionedTransaction[Nid, Cid]]
    with NoCopy {

  override protected def self: this.type = this

  @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
  def mapContractId[Cid2](f: Cid => Cid2): VersionedTransaction[Nid, Cid2] = {
    val versionNode = GenNode.map2(identity[Nid], f)
    VersionedTransaction(
      version,
      nodes = nodes.transform((_, node) => versionNode(node)),
      roots,
    )
  }

  // O(1)
  def transaction: GenTransaction[Nid, Cid] =
    GenTransaction(nodes, roots)

}

object VersionedTransaction extends value.CidContainer2[VersionedTransaction] {

  override private[lf] def map2[A1, B1, A2, B2](
      f1: A1 => A2,
      f2: B1 => B2,
  ): VersionedTransaction[A1, B1] => VersionedTransaction[A2, B2] = {
    case VersionedTransaction(version, versionedNodes, roots) =>
      val mapNode = GenNode.map2(f1, f2)
      VersionedTransaction(
        version,
        versionedNodes.map { case (nid, node) =>
          f1(nid) -> mapNode(node)
        },
        roots.map(f1),
      )
  }

  override private[lf] def foreach2[A, B](
      f1: A => Unit,
      f2: B => Unit,
  ): VersionedTransaction[A, B] => Unit = { case VersionedTransaction(_, versionedNodes, _) =>
    val foreachNode = GenNode.foreach2(f1, f2)
    versionedNodes.foreach { case (nid, node) =>
      f1(nid)
      foreachNode(node)
    }
  }

}

/** General transaction type
  *
  * Abstracts over NodeId type and ContractId type
  * ContractId restricts the occurrence of contractIds
  *
  * @param nodes The nodes of this transaction.
  * @param roots References to the root nodes of the transaction.
  * Users of this class may assume that all instances are well-formed, i.e., `isWellFormed.isEmpty`.
  * For performance reasons, users are not required to call `isWellFormed`.
  * Therefore, it is '''forbidden''' to create ill-formed instances, i.e., instances with `!isWellFormed.isEmpty`.
  */
final case class GenTransaction[Nid, +Cid](
    nodes: Map[Nid, Node.GenNode[Nid, Cid]],
    roots: ImmArray[Nid],
) extends HasTxNodes[Nid, Cid]
    with value.CidContainer[GenTransaction[Nid, Cid]] {

  import GenTransaction._

  override protected def self: this.type = this

  private[lf] def map2[Nid2, Cid2](
      f: Nid => Nid2,
      g: Cid => Cid2,
  ): GenTransaction[Nid2, Cid2] =
    GenTransaction.map2(f, g)(this)

  /** Note: the provided function must be injective, otherwise the transaction will be corrupted. */
  def mapNodeId[Nid2](f: Nid => Nid2): GenTransaction[Nid2, Cid] =
    map2(f, identity)

  /** This function checks the following properties:
    *
    * * No dangling references -- all node ids mentioned in the forest are in the nodes map;
    * * No orphaned references -- all keys of the node map are mentioned in the forest;
    * * No aliasing -- every node id in the node map is mentioned exactly once, in the roots list or as a child of
    *   another node.
    */
  def isWellFormed: Set[NotWellFormedError[Nid]] = {
    // note that we cannot implement this with fold because fold itself crashes on bad
    // transactions
    @tailrec
    def go(
        errors: Set[NotWellFormedError[Nid]],
        visited: Set[Nid],
        toVisit: FrontStack[Nid],
    ): (Set[NotWellFormedError[Nid]], Set[Nid]) =
      toVisit match {
        case FrontStack() => (errors, visited)
        case FrontStackCons(nid, nids) =>
          val alreadyVisited = visited.contains(nid)
          val newVisited = visited + nid
          val newErrors = if (alreadyVisited) {
            errors + NotWellFormedError(nid, AliasedNode)
          } else {
            errors
          }
          nodes.get(nid) match {
            case None =>
              go(newErrors + NotWellFormedError(nid, DanglingNodeId), newVisited, nids)
            case Some(node) =>
              node match {
                case _: Node.LeafOnlyNode[Cid] => go(newErrors, newVisited, nids)
                case ne: Node.NodeExercises[Nid, Cid] =>
                  go(
                    newErrors,
                    newVisited,
                    if (alreadyVisited) {
                      nids
                    } else {
                      ne.children ++: nids
                    },
                  )
              }
          }
      }
    val (errors, visited) = go(Set.empty, Set.empty, FrontStack(roots))
    val orphaned = nodes.keys.toSet.diff(visited).map(nid => NotWellFormedError(nid, OrphanedNode))
    errors ++ orphaned
  }

  /** Compares two Transactions up to renaming of Nids. You most likely want to use this rather than ==, since the
    * Nid is irrelevant to the content of the transaction.
    */
  def equalForest[Cid2 >: Cid](other: GenTransaction[_, Cid2]): Boolean =
    compareForest(other)(_ == _)

  /** Compares two Transactions up to renaming of Nids. with the specified comparision of nodes
    * Nid is irrelevant to the content of the transaction.
    */
  def compareForest[Nid2, Cid2](other: GenTransaction[Nid2, Cid2])(
      compare: (Node.GenNode[Nothing, Cid], Node.GenNode[Nothing, Cid2]) => Boolean
  ): Boolean = {
    @tailrec
    def go(toCompare: FrontStack[(Nid, Nid2)]): Boolean =
      toCompare match {
        case FrontStack() => true
        case FrontStackCons((nid1, nid2), rest) =>
          val node1 = nodes(nid1)
          val node2 = other.nodes(nid2)
          node1 match {
            case nf1: Node.NodeFetch[Cid] =>
              node2 match {
                case nf2: Node.NodeFetch[Cid2] => compare(nf1, nf2) && go(rest)
                case _ => false
              }
            case nc1: Node.NodeCreate[Cid] =>
              node2 match {
                case nc2: Node.NodeCreate[Cid2] =>
                  compare(nc1, nc2) && go(rest)
                case _ => false
              }
            case ne1: Node.NodeExercises[Nid, Cid] =>
              node2 match {
                case ne2: Node.NodeExercises[Nid2, Cid2] =>
                  val blankedNe1: Node.NodeExercises[Nothing, Cid] =
                    ne1.copy(children = ImmArray.empty)
                  val blankedNe2: Node.NodeExercises[Nothing, Cid2] =
                    ne2.copy(children = ImmArray.empty)
                  compare(blankedNe1, blankedNe2) &&
                  ne1.children.length == ne2.children.length &&
                  go(ne1.children.zip(ne2.children) ++: rest)
                case _ => false
              }
            case nl1: Node.NodeLookupByKey[Cid] =>
              node2 match {
                case nl2: Node.NodeLookupByKey[Cid2] =>
                  compare(nl1, nl2) && go(rest)
                case _ => false
              }
          }
      }

    if (roots.length != other.roots.length)
      false
    else
      go(FrontStack(roots.zip(other.roots)))

  }

  /** checks that all the values contained are serializable */
  def serializable(f: Value[Cid] => ImmArray[String]): ImmArray[String] = {
    fold(BackStack.empty[String]) { case (errs, (_, node)) =>
      node match {
        case _: Node.NodeFetch[Cid] => errs
        case nc: Node.NodeCreate[Cid] =>
          errs :++ f(nc.coinst.arg) :++ (nc.key match {
            case None => ImmArray.empty
            case Some(key) => f(key.key)
          })
        case ne: Node.NodeExercises[Nid, Cid] => errs :++ f(ne.chosenValue)
        case nlbk: Node.NodeLookupByKey[Cid] => errs :++ f(nlbk.key.key)
      }
    }.toImmArray
  }

  /** Visit every `Val`. */
  def foldValues[Z](z: Z)(f: (Z, Value[Cid]) => Z): Z =
    fold(z) { case (z, (_, n)) =>
      n match {
        case c: Node.NodeCreate[_] =>
          val z1 = f(z, c.arg)
          val z2 = c.key match {
            case None => z1
            case Some(k) => f(z1, k.key)
          }
          z2
        case nf: Node.NodeFetch[_] => nf.key.fold(z)(k => f(z, k.key))
        case e: Node.NodeExercises[_, _] => f(z, e.chosenValue)
        case lk: Node.NodeLookupByKey[_] => f(z, lk.key.key)
      }
    }

  private[lf] def foreach2(fNid: Nid => Unit, fCid: Cid => Unit): Unit =
    GenTransaction.foreach2(fNid, fCid)(this)
}

sealed abstract class HasTxNodes[Nid, +Cid] {

  def nodes: Map[Nid, Node.GenNode[Nid, Cid]]

  def roots: ImmArray[Nid]

  /** This function traverses the transaction tree in pre-order traversal (i.e. exercise node are traversed before their children).
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  final def foreach(f: (Nid, Node.GenNode[Nid, Cid]) => Unit): Unit = {

    @tailrec
    def go(toVisit: FrontStack[Nid]): Unit = toVisit match {
      case FrontStack() =>
      case FrontStackCons(nodeId, toVisit) =>
        val node = nodes(nodeId)
        f(nodeId, node)
        node match {
          case _: Node.LeafOnlyNode[Cid] => go(toVisit)
          case ne: Node.NodeExercises[Nid, Cid] => go(ne.children ++: toVisit)
        }
    }

    go(FrontStack(roots))
  }

  /** Traverses the transaction tree in pre-order traversal (i.e. exercise nodes are traversed before their children)
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  final def fold[A](z: A)(f: (A, (Nid, Node.GenNode[Nid, Cid])) => A): A = {
    var acc = z
    foreach { (nodeId, node) =>
      // make sure to not tie the knot by mistake by evaluating early
      val acc2 = acc
      acc = f(acc2, (nodeId, node))
    }
    acc
  }

  /** A fold over the transaction that maintains global and path-specific state.
    * Takes constant stack space. Returns the global state.
    *
    * Used to for example compute the roots of per-party projections from the
    * transaction.
    */
  final def foldWithPathState[A, B](globalState0: A, pathState0: B)(
      op: (A, B, Nid, Node.GenNode[Nid, Cid]) => (A, B)
  ): A = {
    var globalState = globalState0

    @tailrec
    def go(toVisit: FrontStack[(Nid, B)]): Unit = toVisit match {
      case FrontStack() =>
      case FrontStackCons((nodeId, pathState), toVisit) =>
        val node = nodes(nodeId)
        val (globalState1, newPathState) = op(globalState, pathState, nodeId, node)
        globalState = globalState1
        node match {
          case _: Node.LeafOnlyNode[Cid] => go(toVisit)
          case ne: Node.NodeExercises[Nid, Cid] =>
            go(ne.children.map(_ -> newPathState) ++: toVisit)
        }
    }

    go(FrontStack(roots.map(_ -> pathState0)))
    globalState
  }

  final def localContracts[Cid2 >: Cid]: Map[Cid2, Nid] =
    fold(Map.empty[Cid2, Nid]) {
      case (acc, (nid, create: Node.NodeCreate[Cid])) =>
        acc.updated(create.coid, nid)
      case (acc, _) => acc
    }

  /** Returns the IDs of all input contracts that are used by this transaction.
    */
  final def inputContracts[Cid2 >: Cid]: Set[Cid2] =
    fold(Set.empty[Cid2]) {
      case (acc, (_, Node.NodeExercises(coid, _, _, _, _, _, _, _, _, _, _, _, _, _, _))) =>
        acc + coid
      case (acc, (_, Node.NodeFetch(coid, _, _, _, _, _, _, _, _))) =>
        acc + coid
      case (acc, (_, Node.NodeLookupByKey(_, _, _, Some(coid), _))) =>
        acc + coid
      case (acc, _) => acc
    } -- localContracts.keySet

  // This method visits to all nodes of the transaction in execution order.
  // Exercise nodes are visited twice: when execution reaches them and when execution leaves their body.
  final def foreachInExecutionOrder(
      exerciseBegin: (Nid, Node.NodeExercises[Nid, Cid]) => Unit,
      leaf: (Nid, Node.LeafOnlyNode[Cid]) => Unit,
      exerciseEnd: (Nid, Node.NodeExercises[Nid, Cid]) => Unit,
  ): Unit = {
    @tailrec
    def loop(
        currNodes: FrontStack[Nid],
        stack: FrontStack[((Nid, Node.NodeExercises[Nid, Cid]), FrontStack[Nid])],
    ): Unit =
      currNodes match {
        case FrontStackCons(nid, rest) =>
          nodes(nid) match {
            case exe: Node.NodeExercises[Nid, Cid] =>
              exerciseBegin(nid, exe)
              loop(FrontStack(exe.children), ((nid, exe), rest) +: stack)
            case node: Node.LeafOnlyNode[Cid] =>
              leaf(nid, node)
              loop(rest, stack)
          }
        case FrontStack() =>
          stack match {
            case FrontStackCons(((nid, exe), brothers), rest) =>
              exerciseEnd(nid, exe)
              loop(brothers, rest)
            case FrontStack() =>
          }
      }

    loop(FrontStack(roots), FrontStack.empty)
  }

  // This method visits to all nodes of the transaction in execution order.
  // Exercise nodes are visited twice: when execution reaches them and when execution leaves their body.
  final def foldInExecutionOrder[A](z: A)(
      exerciseBegin: (A, Nid, Node.NodeExercises[Nid, Cid]) => A,
      leaf: (A, Nid, Node.LeafOnlyNode[Cid]) => A,
      exerciseEnd: (A, Nid, Node.NodeExercises[Nid, Cid]) => A,
  ): A = {
    var acc = z
    foreachInExecutionOrder(
      (nid, node) => acc = exerciseBegin(acc, nid, node),
      (nid, node) => acc = leaf(acc, nid, node),
      (nid, node) => acc = exerciseEnd(acc, nid, node),
    )
    acc
  }

  final def guessSubmitter: Either[String, Party] =
    roots.map(nodes(_).requiredAuthorizers) match {
      case ImmArray() =>
        Left(s"Empty transaction")
      case ImmArrayCons(head, _) if head.size != 1 =>
        Left(s"Transaction's roots do not have exactly one authorizer: $this")
      case ImmArrayCons(head, tail) if tail.toSeq.exists(_ != head) =>
        Left(s"Transaction's roots have different authorizers: $this")
      case ImmArrayCons(head, _) =>
        Right(head.head)
    }

}

object GenTransaction extends value.CidContainer2[GenTransaction] {

  type WithTxValue[Nid, +Cid] = GenTransaction[Nid, Cid]

  private[this] val Empty = GenTransaction[Nothing, Nothing](HashMap.empty, ImmArray.empty)

  private[lf] def empty[A, B, C]: GenTransaction[A, B] = Empty.asInstanceOf[GenTransaction[A, B]]

  private[lf] case class NotWellFormedError[Nid](nid: Nid, reason: NotWellFormedErrorReason)
  private[lf] sealed trait NotWellFormedErrorReason
  private[lf] case object DanglingNodeId extends NotWellFormedErrorReason
  private[lf] case object OrphanedNode extends NotWellFormedErrorReason
  private[lf] case object AliasedNode extends NotWellFormedErrorReason

  override private[lf] def map2[A1, A2, B1, B2](
      f1: A1 => B1,
      f2: A2 => B2,
  ): GenTransaction[A1, A2] => GenTransaction[B1, B2] = { case GenTransaction(nodes, roots) =>
    GenTransaction(
      nodes = nodes.map { case (nodeId, node) =>
        f1(nodeId) -> Node.GenNode.map2(f1, f2)(node)
      },
      roots = roots.map(f1),
    )
  }

  override private[lf] def foreach2[A, B](
      f1: A => Unit,
      f2: B => Unit,
  ): GenTransaction[A, B] => Unit = { case GenTransaction(nodes, _) =>
    nodes.foreach { case (nodeId, node) =>
      f1(nodeId)
      Node.GenNode.foreach2(f1, f2)(node)
    }
  }

  // crashes if transaction's keys contain contract Ids.
  @throws[IllegalArgumentException]
  def duplicatedContractKeys(tx: VersionedTransaction[NodeId, Value.ContractId]): Set[GlobalKey] = {

    import GlobalKey.{assertBuild => globalKey}

    case class State(active: Set[GlobalKey], duplicates: Set[GlobalKey]) {
      def created(key: GlobalKey): State =
        if (active(key)) copy(duplicates = duplicates + key) else copy(active = active + key)
      def consumed(key: GlobalKey): State =
        copy(active = active - key)
      def referenced(key: GlobalKey): State =
        copy(active = active + key)
    }

    tx.fold(State(Set.empty, Set.empty)) { case (state, (_, node)) =>
      node match {
        case Node.NodeCreate(_, tmplId, _, _, _, _, _, Some(key), _) =>
          state.created(globalKey(tmplId, key.key))
        case Node.NodeExercises(_, tmplId, _, _, true, _, _, _, _, _, _, _, Some(key), _, _) =>
          state.consumed(globalKey(tmplId, key.key))
        case Node.NodeExercises(_, tmplId, _, _, false, _, _, _, _, _, _, _, Some(key), _, _) =>
          state.referenced(globalKey(tmplId, key.key))
        case Node.NodeFetch(_, tmplId, _, _, _, _, Some(key), _, _) =>
          state.referenced(globalKey(tmplId, key.key))
        case Node.NodeLookupByKey(tmplId, _, key, Some(_), _) =>
          state.referenced(globalKey(tmplId, key.key))
        case _ =>
          state
      }
    }.duplicates
  }

}

object Transaction {

  type Value[+Cid] = Value.VersionedValue[Cid]

  type ContractInst[+Cid] = Value.ContractInst[Value[Cid]]

  /** Transaction nodes */
  type Node = Node.GenNode[NodeId, Value.ContractId]
  type LeafNode = Node.LeafOnlyNode[Value.ContractId]

  /** (Complete) transactions, which are the result of interpreting a
    * ledger-update. These transactions are consumed by either the
    * scenario-interpreter or the DAML-engine code. Both of these
    * code-paths share the computations for segregating the
    * transaction into party-specific ledgers and for computing
    * divulgence of contracts.
    */
  type Transaction = VersionedTransaction[NodeId, Value.ContractId]
  val Transaction: VersionedTransaction.type = VersionedTransaction

  /** Transaction meta data
    *
    * @param submissionSeed : the submission seed used to derive the contract IDs.
    *                       If undefined no seed has been used (the legacy contract ID scheme
    *                       have been used) or it is unknown (output of partial reinterpretation).
    * @param submissionTime : the submission time
    * @param usedPackages   The set of packages used during command processing.
    *                       This is a hint for what packages are required to validate
    *                       the transaction using the current interpreter.
    *                       If set to `empty` the package dependency have not be computed.
    * @param dependsOnTime  : indicate the transaction computation depends on ledger
    *                       time.
    * @param nodeSeeds      : An association list that maps to each ID of create and exercise
    *                       nodes its seeds.
    */
  final case class Metadata(
      submissionSeed: Option[crypto.Hash],
      submissionTime: Time.Timestamp,
      usedPackages: Set[PackageId],
      dependsOnTime: Boolean,
      nodeSeeds: ImmArray[(NodeId, crypto.Hash)],
  )

  def commitTransaction(submittedTransaction: SubmittedTransaction): CommittedTransaction =
    CommittedTransaction(submittedTransaction)

  def commitTransaction(
      submittedTransaction: SubmittedTransaction,
      f: crypto.Hash => Bytes,
  ): Either[String, CommittedTransaction] =
    submittedTransaction.suffixCid(f).map(CommittedTransaction(_))

  /** Errors that can happen during building transactions. */
  sealed abstract class TransactionError extends Product with Serializable

  /** Signal that a 'endExercise' was called in a root-context; i.e.,
    * without a matching 'beginExercise'.
    */
  case object EndExerciseInRootContext extends TransactionError

  /** Signals that the contract-id `coid` was expected to be active, but
    * is not.
    */
  final case class ContractNotActive(
      coid: Value.ContractId,
      templateId: TypeConName,
      consumedBy: NodeId,
  ) extends TransactionError

  final case class AuthFailureDuringExecution(
      nid: NodeId,
      fa: FailedAuthorization,
  ) extends TransactionError

  @deprecated("use Validation.isRepledBy", since = "1.10.0")
  def isReplayedBy[Nid, Cid](
      recorded: VersionedTransaction[Nid, Cid],
      replayed: VersionedTransaction[Nid, Cid],
  )(implicit ECid: Equal[Cid]): Either[ReplayMismatch[Nid, Cid], Unit] =
    Validation.isReplayedBy(recorded, replayed)
}
