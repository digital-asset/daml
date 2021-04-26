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
                case nr: Node.NodeRollback[Nid] =>
                  go(
                    newErrors,
                    newVisited,
                    if (alreadyVisited) {
                      nids
                    } else {
                      nr.children ++: nids
                    },
                  )
                case _: Node.LeafOnlyActionNode[Cid] => go(newErrors, newVisited, nids)
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
            case nr1: Node.NodeRollback[Nid] => //TODO: can this be NodeRollback[_] ?
              node2 match {
                case nr2: Node.NodeRollback[Nid2] => //TODO: and here
                  val blankedNr1: Node.NodeRollback[Nothing] =
                    nr1.copy(children = ImmArray.empty)
                  val blankedNr2: Node.NodeRollback[Nothing] =
                    nr2.copy(children = ImmArray.empty)
                  compare(blankedNr1, blankedNr2) &&
                  nr1.children.length == nr2.children.length &&
                  go(nr1.children.zip(nr2.children) ++: rest)
                case _ => false
              }
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
        case Node.NodeRollback(_, _) => // reconsider if fields added
          errs
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
        case Node.NodeRollback(_, _) => // reconsider if fields added
          z
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

  /** The union of the informees of a all the action nodes. */
  lazy val informees: Set[Ref.Party] =
    nodes.values.foldLeft(Set.empty[Ref.Party]) {
      case (acc, node: Node.GenActionNode[_, _]) => acc | node.informeesOfNode
      case (acc, _: Node.NodeRollback[_]) => acc
    }

  // TODO: https://github.com/digital-asset/daml/issues/8020
  //  for now we assume that rollback node cannot be a root of a transaction.
  @throws[IllegalArgumentException]
  def rootNodes: ImmArray[Node.GenActionNode[Nid, Cid]] =
    roots.map(nid =>
      nodes(nid) match {
        case action: Node.GenActionNode[Nid, Cid] =>
          action
        case _: Node.NodeRollback[_] =>
          throw new IllegalArgumentException(
            s"invalid transaction, root refers to a Rollback node $nid"
          )
      }
    )

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
          case nr: Node.NodeRollback[Nid] => go(nr.children ++: toVisit)
          case _: Node.LeafOnlyActionNode[Cid] => go(toVisit)
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
    foreach((nodeId, node) => acc = f(acc, (nodeId, node)))
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
          case nr: Node.NodeRollback[Nid] =>
            go(nr.children.map(_ -> newPathState) ++: toVisit)
          case _: Node.LeafOnlyActionNode[Cid] => go(toVisit)
          case ne: Node.NodeExercises[Nid, Cid] =>
            go(ne.children.map(_ -> newPathState) ++: toVisit)
        }
    }

    go(FrontStack(roots.map(_ -> pathState0)))
    globalState
  }

  final def localContracts[Cid2 >: Cid]: Map[Cid2, (Nid, Node.NodeCreate[Cid2])] =
    fold(Map.empty[Cid2, (Nid, Node.NodeCreate[Cid2])]) {
      case (acc, (nid, create: Node.NodeCreate[Cid])) =>
        acc.updated(create.coid, nid -> create)
      case (acc, _) => acc
    }

  /** Returns the IDs of all the consumed contracts.
    *  This includes transient contracts but it does not include contracts
    *  consumed in rollback nodes.
    */
  final def consumedContracts[Cid2 >: Cid]: Set[Cid2] =
    foldInExecutionOrder(Set.empty[Cid2])(
      exerciseBegin = (acc, _, exe) => {
        if (exe.consuming) { (acc + exe.targetCoid, true) }
        else { (acc, true) }
      },
      rollbackBegin = (acc, _, _) => (acc, false),
      leaf = (acc, _, _) => acc,
      exerciseEnd = (acc, _, _) => acc,
      rollbackEnd = (acc, _, _) => acc,
    )

  /** Local and global contracts that are inactive at the end of the transaction.
    * This includes both contracts that have been arachived and local
    * contracts whose create has been rolled back.
    */
  final def inactiveContracts[Cid2 >: Cid]: Set[Cid2] = {
    final case class LedgerState(
        createdCids: Set[Cid2],
        inactiveCids: Set[Cid2],
    ) {
      def create(cid: Cid2): LedgerState =
        copy(
          createdCids = createdCids + cid
        )
      def archive(cid: Cid2): LedgerState =
        copy(
          inactiveCids = inactiveCids + cid
        )
    }
    final case class State(
        currentState: LedgerState,
        rollbackStack: List[LedgerState],
    ) {
      def create(cid: Cid2) = copy(
        currentState = currentState.create(cid)
      )
      def archive(cid: Cid2) = copy(
        currentState = currentState.archive(cid)
      )
      def beginRollback() = copy(
        rollbackStack = currentState :: rollbackStack
      )
      def endRollback() = {
        // In addition to archives we also need to mark contracts
        // created in the rollback as inactive
        val beginState = rollbackStack.head
        copy(
          currentState = beginState.copy(
            inactiveCids =
              beginState.inactiveCids union (currentState.createdCids diff beginState.createdCids)
          ),
          rollbackStack = rollbackStack.tail,
        )
      }
    }
    foldInExecutionOrder[State](State(LedgerState(Set.empty, Set.empty), Nil))(
      exerciseBegin = (acc, _, exe) =>
        if (exe.consuming) {
          (acc.archive(exe.targetCoid), true)
        } else {
          (acc, true)
        },
      exerciseEnd = (acc, _, _) => acc,
      rollbackBegin = (acc, _, _) => (acc.beginRollback(), true),
      rollbackEnd = (acc, _, _) => acc.endRollback(),
      leaf = (acc, _, leaf) =>
        leaf match {
          case c: Node.NodeCreate[Cid2] => acc.create(c.coid)
          case _ => acc
        },
    ).currentState.inactiveCids
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

  /** Return all the contract keys referenced by this transaction.
    * This includes the keys created, exercised, fetched, or looked up, even those
    * that refer transient contracts or that appear under a rollback node.
    */
  final def contractKeys(implicit
      evidence: HasTxNodes[Nid, Cid] <:< HasTxNodes[_, Value.ContractId]
  ): Set[GlobalKey] = {
    evidence(this).fold(Set.empty[GlobalKey]) {
      case (acc, (_, node: Node.NodeCreate[Value.ContractId])) =>
        node.key.fold(acc)(key => acc + GlobalKey.assertBuild(node.templateId, key.key))
      case (acc, (_, node: Node.NodeExercises[_, Value.ContractId])) =>
        node.key.fold(acc)(key => acc + GlobalKey.assertBuild(node.templateId, key.key))
      case (acc, (_, node: Node.NodeFetch[Value.ContractId])) =>
        node.key.fold(acc)(key => acc + GlobalKey.assertBuild(node.templateId, key.key))
      case (acc, (_, node: Node.NodeLookupByKey[Value.ContractId])) =>
        acc + GlobalKey.assertBuild(node.templateId, node.key.key)
      case (acc, (_, _: Node.NodeRollback[_])) =>
        acc
    }
  }

  /** The contract keys created or updated as part of the transaction.
    *  This includes updates to transient contracts (by mapping them to None)
    *  but it does not include any updates under rollback nodes.
    */
  final def updatedContractKeys(implicit
      ev: HasTxNodes[Nid, Cid] <:< HasTxNodes[_, Value.ContractId]
  ): Map[GlobalKey, Option[Value.ContractId]] = {
    ev(this).foldInExecutionOrder(Map.empty[GlobalKey, Option[Value.ContractId]])(
      exerciseBegin = {
        case (acc, _, exec) if exec.consuming =>
          (
            exec.key.fold(acc)(key =>
              acc.updated(GlobalKey.assertBuild(exec.templateId, key.key), None)
            ),
            true,
          )
        case (acc, _, _) => (acc, true)
      },
      rollbackBegin = (acc, _, _) => (acc, false),
      leaf = {
        case (acc, _, create: Node.NodeCreate[Value.ContractId]) =>
          create.key.fold(acc)(key =>
            acc.updated(GlobalKey.assertBuild(create.templateId, key.key), Some(create.coid))
          )
        case (acc, _, _: Node.NodeFetch[_] | _: Node.NodeLookupByKey[_]) => acc
      },
      exerciseEnd = (acc, _, _) => acc,
      rollbackEnd = (acc, _, _) => acc,
    )
  }

  // This method visits to all nodes of the transaction in execution order.
  // Exercise/rollback nodes are visited twice: when execution reaches them and when execution leaves their body.
  // On the first visit of an execution/rollback node, the caller can prevent traversal of the children
  final def foreachInExecutionOrder(
      exerciseBegin: (Nid, Node.NodeExercises[Nid, Cid]) => Boolean,
      rollbackBegin: (Nid, Node.NodeRollback[Nid]) => Boolean,
      leaf: (Nid, Node.LeafOnlyActionNode[Cid]) => Unit,
      exerciseEnd: (Nid, Node.NodeExercises[Nid, Cid]) => Unit,
      rollbackEnd: (Nid, Node.NodeRollback[Nid]) => Unit,
  ): Unit = {
    @tailrec
    def loop(
        currNodes: FrontStack[Nid],
        stack: FrontStack[
          ((Nid, Either[Node.NodeRollback[Nid], Node.NodeExercises[Nid, Cid]]), FrontStack[Nid])
        ],
    ): Unit =
      currNodes match {
        case FrontStackCons(nid, rest) =>
          nodes(nid) match {
            case rb: Node.NodeRollback[_] =>
              if (rollbackBegin(nid, rb)) {
                loop(FrontStack(rb.children), ((nid, Left(rb)), rest) +: stack)
              } else {
                loop(rest, stack)
              }
            case exe: Node.NodeExercises[Nid, Cid] =>
              if (exerciseBegin(nid, exe)) {
                loop(FrontStack(exe.children), ((nid, Right(exe)), rest) +: stack)
              } else {
                loop(rest, stack)
              }
            case node: Node.LeafOnlyActionNode[Cid] =>
              leaf(nid, node)
              loop(rest, stack)
          }
        case FrontStack() =>
          stack match {
            case FrontStackCons(((nid, either), brothers), rest) =>
              either match {
                case Left(rb) =>
                  rollbackEnd(nid, rb)
                  loop(brothers, rest)
                case Right(exe) =>
                  exerciseEnd(nid, exe)
                  loop(brothers, rest)
              }
            case FrontStack() =>
          }
      }

    loop(FrontStack(roots), FrontStack.empty)
  }

  // This method visits to all nodes of the transaction in execution order.
  // Exercise nodes are visited twice: when execution reaches them and when execution leaves their body.
  final def foldInExecutionOrder[A](z: A)(
      exerciseBegin: (A, Nid, Node.NodeExercises[Nid, Cid]) => (A, Boolean),
      rollbackBegin: (A, Nid, Node.NodeRollback[Nid]) => (A, Boolean),
      leaf: (A, Nid, Node.LeafOnlyActionNode[Cid]) => A,
      exerciseEnd: (A, Nid, Node.NodeExercises[Nid, Cid]) => A,
      rollbackEnd: (A, Nid, Node.NodeRollback[Nid]) => A,
  ): A = {
    var acc = z
    foreachInExecutionOrder(
      (nid, node) => {
        val (acc2, bool) = exerciseBegin(acc, nid, node)
        acc = acc2
        bool
      },
      (nid, node) => {
        val (acc2, bool) = rollbackBegin(acc, nid, node)
        acc = acc2
        bool
      },
      (nid, node) => acc = leaf(acc, nid, node),
      (nid, node) => acc = exerciseEnd(acc, nid, node),
      (nid, node) => acc = rollbackEnd(acc, nid, node),
    )
    acc
  }

  // This method returns all node-ids reachable from the roots of a transaction.
  final def reachableNodeIds: Set[Nid] = {
    foldInExecutionOrder[Set[Nid]](Set.empty)(
      (acc, nid, _) => (acc + nid, true),
      (acc, nid, _) => (acc + nid, true),
      (acc, nid, _) => acc + nid,
      (acc, _, _) => acc,
      (acc, _, _) => acc,
    )
  }

  final def guessSubmitter: Either[String, Party] =
    rootNodes.map(_.requiredAuthorizers) match {
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
  type LeafNode = Node.LeafOnlyActionNode[Value.ContractId]

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

  /** Signal that a 'endExercise' was called in a non exercise-context; i.e.,
    * without a matching 'beginExercise'.
    */
  case object NonExerciseContext extends TransactionError

  /** Signal that a 'endCatch' or a 'rollback`` was called in a non catch-context; i.e.,
    * without a matching 'beginCatch'.
    */
  case object NonCatchContext extends TransactionError

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
