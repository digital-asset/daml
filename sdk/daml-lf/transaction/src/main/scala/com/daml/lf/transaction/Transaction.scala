// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.lf.transaction.ContractStateMachine.KeyMapping
import com.daml.lf.transaction.TransactionErrors.KeyInputError

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

final case class VersionedTransaction private[lf] (
    version: TransactionVersion,
    nodes: Map[NodeId, Node],
    override val roots: ImmArray[NodeId],
) extends HasTxNodes
    with value.CidContainer[VersionedTransaction]
    with NoCopy {

  override protected def self: this.type = this

  override def mapCid(f: ContractId => ContractId): VersionedTransaction =
    VersionedTransaction(
      version,
      nodes = nodes.map { case (nodeId, node) => nodeId -> node.mapCid(f) },
      roots,
    )

  def mapNodeId(f: NodeId => NodeId): VersionedTransaction =
    VersionedTransaction(
      version,
      nodes.map { case (nodeId, node) => f(nodeId) -> node.mapNodeId(f) },
      roots.map(f),
    )

  // O(1)
  def transaction: Transaction =
    Transaction(nodes, roots)

}

/** General transaction type
  *
  * Abstracts over NodeId type and ContractId type
  * ContractId restricts the occurrence of contractIds
  *
  * @param nodes The nodes of this transaction.
  * @param roots References to the root nodes of the transaction.
  *
  * Users of this class may assume that all instances are well-formed, i.e., `isWellFormed.isEmpty`.
  * For performance reasons, users are not required to call `isWellFormed`.
  * Therefore, it is '''forbidden''' to create ill-formed instances, i.e., instances with `!isWellFormed.isEmpty`.
  */
final case class Transaction(
    nodes: Map[NodeId, Node],
    roots: ImmArray[NodeId],
) extends HasTxNodes
    with value.CidContainer[Transaction] {

  import Transaction._

  override protected def self: this.type = this
  override def mapCid(f: ContractId => ContractId): Transaction =
    copy(nodes = nodes.map { case (nodeId, node) => nodeId -> node.mapCid(f) })
  def mapNodeId(f: NodeId => NodeId): Transaction =
    copy(
      nodes = nodes.map { case (nodeId, node) => f(nodeId) -> node.mapNodeId(f) },
      roots = roots.map(f),
    )

  /** This function checks the following properties:
    *
    * * No dangling references -- all node ids mentioned in the forest are in the nodes map;
    * * No orphaned references -- all keys of the node map are mentioned in the forest;
    * * No aliasing -- every node id in the node map is mentioned exactly once, in the roots list or as a child of
    *   another node.
    */
  def isWellFormed: Set[NotWellFormedError] = {
    // note that we cannot implement this with fold because fold itself crashes on bad
    // transactions
    @tailrec
    def go(
        errors: Set[NotWellFormedError],
        visited: Set[NodeId],
        toVisit: FrontStack[NodeId],
    ): (Set[NotWellFormedError], Set[NodeId]) =
      toVisit.pop match {
        case None => (errors, visited)
        case Some((nid, nids)) =>
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
                case nr: Node.Rollback =>
                  go(
                    newErrors,
                    newVisited,
                    if (alreadyVisited) {
                      nids
                    } else {
                      nr.children ++: nids
                    },
                  )
                case _: Node.LeafOnlyAction => go(newErrors, newVisited, nids)
                case ne: Node.Exercise =>
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
    val (errors, visited) = go(Set.empty, Set.empty, FrontStack.from(roots))
    val orphaned = nodes.keys.toSet.diff(visited).map(nid => NotWellFormedError(nid, OrphanedNode))
    errors ++ orphaned
  }

  /** Compares two Transactions up to renaming of Nids. You most likely want to use this rather than ==, since the
    * Nid is irrelevant to the content of the transaction.
    */
  def equalForest(other: Transaction): Boolean =
    compareForest(other)(_ == _)

  /** Compares two Transactions up to renaming of Nids. with the specified comparision of nodes
    * Nid is irrelevant to the content of the transaction.
    */
  def compareForest(other: Transaction)(
      compare: (Node, Node) => Boolean
  ): Boolean = {
    @tailrec
    def go(toCompare: FrontStack[(NodeId, NodeId)]): Boolean =
      toCompare.pop match {
        case None => true
        case Some(((nid1, nid2), rest)) =>
          val node1 = nodes(nid1)
          val node2 = other.nodes(nid2)
          node1 match {
            case nr1: Node.Rollback => // TODO: can this be Node.Rollback ?
              node2 match {
                case nr2: Node.Rollback => // TODO: and here
                  val blankedNr1: Node.Rollback =
                    nr1.copy(children = ImmArray.Empty)
                  val blankedNr2: Node.Rollback =
                    nr2.copy(children = ImmArray.Empty)
                  compare(blankedNr1, blankedNr2) &&
                  nr1.children.length == nr2.children.length &&
                  go(nr1.children.zip(nr2.children) ++: rest)
                case _ => false
              }
            case nf1: Node.Fetch =>
              node2 match {
                case nf2: Node.Fetch => compare(nf1, nf2) && go(rest)
                case _ => false
              }
            case nc1: Node.Create =>
              node2 match {
                case nc2: Node.Create =>
                  compare(nc1, nc2) && go(rest)
                case _ => false
              }
            case ne1: Node.Exercise =>
              node2 match {
                case ne2: Node.Exercise =>
                  val blankedNe1: Node.Exercise =
                    ne1.copy(children = ImmArray.Empty)
                  val blankedNe2: Node.Exercise =
                    ne2.copy(children = ImmArray.Empty)
                  compare(blankedNe1, blankedNe2) &&
                  ne1.children.length == ne2.children.length &&
                  go(ne1.children.zip(ne2.children) ++: rest)
                case _ => false
              }
            case nl1: Node.LookupByKey =>
              node2 match {
                case nl2: Node.LookupByKey =>
                  compare(nl1, nl2) && go(rest)
                case _ => false
              }
          }
      }

    if (roots.length != other.roots.length)
      false
    else
      go(roots.zip(other.roots).toFrontStack)

  }

  /** checks that all the values contained are serializable */
  def serializable(f: Value => ImmArray[String]): ImmArray[String] = {
    fold(BackStack.empty[String]) { case (errs, (_, node)) =>
      node match {
        case Node.Rollback(_) =>
          errs
        case _: Node.Fetch => errs
        case nc: Node.Create =>
          errs :++ f(nc.arg) :++ (nc.keyOpt match {
            case None => ImmArray.Empty
            case Some(key) => f(key.globalKey.key)
          })
        case ne: Node.Exercise => errs :++ f(ne.chosenValue)
        case nlbk: Node.LookupByKey => errs :++ f(nlbk.gkey.key)
      }
    }.toImmArray
  }

  /** Visit every `Val`. */
  def foldValues[Z](z: Z)(f: (Z, Value) => Z): Z =
    fold(z) { case (z, (_, n)) =>
      n match {
        case Node.Rollback(_) =>
          z
        case c: Node.Create =>
          val z1 = f(z, c.arg)
          val z2 = c.keyOpt match {
            case None => z1
            case Some(k) => f(z1, k.globalKey.key)
          }
          z2
        case nf: Node.Fetch => nf.keyOpt.fold(z)(k => f(z, k.globalKey.key))
        case e: Node.Exercise => f(z, e.chosenValue)
        case lk: Node.LookupByKey => f(z, lk.gkey.key)
      }
    }

  /*
  private[lf] def foreach2(fNid: Nid => Unit, fCid: ContractI => Unit): Unit =
    GenTransaction.foreach2(fNid, fCid)(this)
   */
}

sealed abstract class HasTxNodes {

  import Transaction.{KeyInput, ChildrenRecursion}

  def nodes: Map[NodeId, Node]

  def roots: ImmArray[NodeId]

  // We assume that rollback node cannot be a root of a transaction.
  // This is correct for an unprojected transaction. For a project transaction,
  // Canton handles rollback nodes itself so this is assumption still holds
  // within the Engine.
  @throws[IllegalArgumentException]
  def rootNodes: ImmArray[Node.Action] =
    roots.map(nid =>
      nodes(nid) match {
        case action: Node.Action =>
          action
        case _: Node.Rollback =>
          throw new IllegalArgumentException(
            s"invalid transaction, root refers to a Rollback node $nid"
          )
      }
    )

  /** This function traverses the transaction tree in pre-order traversal (i.e. exercise node are traversed before their children).
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  final def foreach(f: (NodeId, Node) => Unit): Unit = {

    @tailrec
    def go(toVisit: FrontStack[NodeId]): Unit = toVisit.pop match {
      case None =>
      case Some((nodeId, toVisit)) =>
        val node = nodes(nodeId)
        f(nodeId, node)
        node match {
          case nr: Node.Rollback => go(nr.children ++: toVisit)
          case _: Node.LeafOnlyAction => go(toVisit)
          case ne: Node.Exercise => go(ne.children ++: toVisit)
        }
    }

    go(roots.toFrontStack)
  }

  /** Traverses the transaction tree in pre-order traversal (i.e. exercise nodes are traversed before their children)
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  final def fold[A](z: A)(f: (A, (NodeId, Node)) => A): A = {
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
      op: (A, B, NodeId, Node) => (A, B)
  ): A = {
    var globalState = globalState0

    @tailrec
    def go(toVisit: FrontStack[(NodeId, B)]): Unit = toVisit.pop match {
      case None =>
      case Some(((nodeId, pathState), toVisit)) =>
        val node = nodes(nodeId)
        val (globalState1, newPathState) = op(globalState, pathState, nodeId, node)
        globalState = globalState1
        node match {
          case nr: Node.Rollback =>
            go(nr.children.map(_ -> newPathState) ++: toVisit)
          case _: Node.LeafOnlyAction => go(toVisit)
          case ne: Node.Exercise =>
            go(ne.children.map(_ -> newPathState) ++: toVisit)
        }
    }

    go(roots.map(_ -> pathState0).toFrontStack)
    globalState
  }

  final def localContracts[Cid2 >: ContractId]: Map[Cid2, (NodeId, Node.Create)] =
    fold(Map.empty[Cid2, (NodeId, Node.Create)]) {
      case (acc, (nid, create: Node.Create)) =>
        acc.updated(create.coid, nid -> create)
      case (acc, _) => acc
    }

  /** Returns the IDs of all the consumed contracts.
    * This includes transient contracts but it does not include contracts
    * consumed in rollback nodes.
    */
  final def consumedContracts[Cid2 >: ContractId]: Set[Cid2] =
    foldInExecutionOrder(Set.empty[Cid2])(
      exerciseBegin = (acc, _, exe) => {
        if (exe.consuming) { (acc + exe.targetCoid, ChildrenRecursion.DoRecurse) }
        else { (acc, ChildrenRecursion.DoRecurse) }
      },
      rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
      leaf = (acc, _, _) => acc,
      exerciseEnd = (acc, _, _) => acc,
      rollbackEnd = (acc, _, _) => acc,
    )

  /** Local and global contracts that are inactive at the end of the transaction.
    * This includes both contracts that have been arachived and local
    * contracts whose create has been rolled back.
    */
  final def inactiveContracts[Cid2 >: ContractId]: Set[Cid2] = {
    final case class LedgerState(
        // Contracts created up to this point including rolled back contracts.
        createdCids: Set[Cid2],
        // Contracts that have been marked as inactive either by a rollback of a create
        // or an archive.
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
              // Add all contracts created under rollback to inactive contracts.
              // We don’t care if they are nested further below other rollbacks.
              beginState.inactiveCids union (currentState.createdCids diff beginState.createdCids),
            createdCids = currentState.createdCids,
          ),
          rollbackStack = rollbackStack.tail,
        )
      }
    }
    foldInExecutionOrder[State](State(LedgerState(Set.empty, Set.empty), Nil))(
      exerciseBegin = (acc, _, exe) =>
        if (exe.consuming) {
          (acc.archive(exe.targetCoid), ChildrenRecursion.DoRecurse)
        } else {
          (acc, ChildrenRecursion.DoRecurse)
        },
      exerciseEnd = (acc, _, _) => acc,
      rollbackBegin = (acc, _, _) => (acc.beginRollback(), ChildrenRecursion.DoRecurse),
      rollbackEnd = (acc, _, _) => acc.endRollback(),
      leaf = (acc, _, leaf) =>
        leaf match {
          case c: Node.Create => acc.create(c.coid)
          case _ => acc
        },
    ).currentState.inactiveCids
  }

  /** Returns the IDs of all input contracts that are used by this transaction.
    */
  final def inputContracts[Cid2 >: ContractId]: Set[Cid2] =
    fold(Set.empty[Cid2]) {
      case (acc, (_, Node.Exercise(coid, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _))) =>
        acc + coid
      case (acc, (_, Node.Fetch(coid, _, _, _, _, _, _, _, _))) =>
        acc + coid
      case (acc, (_, Node.LookupByKey(_, _, _, Some(coid), _))) =>
        acc + coid
      case (acc, _) => acc
    } -- localContracts.keySet

  /** Return all the contract keys referenced by this transaction.
    * This includes the keys created, exercised, fetched, or looked up, even those
    * that refer transient contracts or that appear under a rollback node.
    */
  final def contractKeys: Set[GlobalKey] = {
    fold(Set.empty[GlobalKey]) {
      case (acc, (_, node: Node.Action)) =>
        node.gkeyOpt.fold(acc)(acc + _)
      case (acc, (_, (_: Node.Rollback))) =>
        acc
    }
  }

  /** Keys are contracts (that have been consumed) and values are the nodes where the contract was consumed.
    * Nodes under rollbacks (both exercises and creates) are ignored (as they have been rolled back).
    * The result includes both local contracts created in the transaction (if they’ve been consumed) as well as global
    * contracts created in previous transactions. It does not include local contracts created under a rollback.
    */
  final def consumedBy: Map[ContractId, NodeId] =
    foldInExecutionOrder[Map[ContractId, NodeId]](HashMap.empty)(
      exerciseBegin = (consumedByMap, nodeId, exerciseNode) => {
        if (exerciseNode.consuming) {
          (consumedByMap + (exerciseNode.targetCoid -> nodeId), ChildrenRecursion.DoRecurse)
        } else {
          (consumedByMap, ChildrenRecursion.DoRecurse)
        }
      },
      rollbackBegin = (consumedByMap, _, _) => {
        (consumedByMap, ChildrenRecursion.DoNotRecurse)
      },
      leaf = (consumedByMap, _, _) => consumedByMap,
      exerciseEnd = (consumedByMap, _, _) => consumedByMap,
      rollbackEnd = (consumedByMap, _, _) => consumedByMap,
    )

  /** Return the expected contract key inputs (i.e. the state before the transaction)
    * for this transaction or an error if the transaction contains a
    * duplicate key error or has an inconsistent mapping for a key. For
    * KeyCreate and NegativeKeyLookup (both corresponding to the key not being active)
    * the first required input in execution order wins. So if a create comes first
    * the input will be set to KeyCreate, if a negative lookup by key comes first
    * the input will be set to NegativeKeyLookup.
    *
    * Because we do not preserve byKey flags across transaction serialization
    * this method will consider all operations with keys for conflicts
    * rather than just by-key operations.
    */
  @throws[IllegalArgumentException](
    "If a contract key contains a contract id"
  )
  def contractKeyInputs: Either[KeyInputError, Map[GlobalKey, KeyInput]] = {
    foldInExecutionOrder[Either[KeyInputError, ContractStateMachine.State[NodeId]]](
      Right(ContractStateMachine.initial[NodeId](ContractKeyUniquenessMode.Strict))
    )(
      exerciseBegin = (acc, nid, exe) =>
        (acc.flatMap(_.handleExercise(nid, exe)), Transaction.ChildrenRecursion.DoRecurse),
      exerciseEnd = (acc, _, _) => acc,
      rollbackBegin =
        (acc, _, _) => (acc.map(_.beginRollback()), Transaction.ChildrenRecursion.DoRecurse),
      rollbackEnd = (acc, _, _) => acc.map(_.endRollback()),
      leaf = (
          acc,
          nid,
          leaf,
      ) =>
        acc.flatMap(
          _.handleNode(nid, leaf, None)
        ), // ok to use None as keyInput, because mode is strict
    ).map(_.globalKeyInputs)
  }

  /** The contract keys created or updated as part of the transaction.
    *  This includes updates to transient contracts (by mapping them to None)
    *  but it does not include any updates under rollback nodes.
    */
  final def updatedContractKeys: Map[GlobalKey, Option[Value.ContractId]] = {
    foldInExecutionOrder(Map.empty[GlobalKey, Option[Value.ContractId]])(
      exerciseBegin = {
        case (acc, _, exec) if exec.consuming =>
          (
            exec.gkeyOpt.fold(acc)(acc.updated(_, None)),
            ChildrenRecursion.DoRecurse,
          )
        case (acc, _, _) => (acc, ChildrenRecursion.DoRecurse)
      },
      rollbackBegin = (acc, _, _) => (acc, ChildrenRecursion.DoNotRecurse),
      leaf = {
        case (acc, _, create: Node.Create) =>
          create.gkeyOpt.fold(acc)(acc.updated(_, Some(create.coid)))
        case (acc, _, _: Node.Fetch | _: Node.LookupByKey) => acc
      },
      exerciseEnd = (acc, _, _) => acc,
      rollbackEnd = (acc, _, _) => acc,
    )
  }

  // This method visits to all nodes of the transaction in execution order.
  // Exercise/rollback nodes are visited twice: when execution reaches them and when execution leaves their body.
  // On the first visit of an execution/rollback node, the caller can prevent traversal of the children
  final def foreachInExecutionOrder(
      exerciseBegin: (NodeId, Node.Exercise) => ChildrenRecursion,
      rollbackBegin: (NodeId, Node.Rollback) => ChildrenRecursion,
      leaf: (NodeId, Node.LeafOnlyAction) => Unit,
      exerciseEnd: (NodeId, Node.Exercise) => Unit,
      rollbackEnd: (NodeId, Node.Rollback) => Unit,
  ): Unit = {
    sealed abstract class StackItem
    object SI {
      final case class Rollback(x: Node.Rollback) extends StackItem
      final case class Exercise(y: Node.Exercise) extends StackItem
    }
    @tailrec
    def loop(
        currNodes: FrontStack[NodeId],
        stack: FrontStack[((NodeId, StackItem), FrontStack[NodeId])],
    ): Unit =
      currNodes.pop match {
        case Some((nid, rest)) =>
          nodes(nid) match {
            case rb: Node.Rollback =>
              rollbackBegin(nid, rb) match {
                case ChildrenRecursion.DoRecurse =>
                  loop(rb.children.toFrontStack, ((nid, SI.Rollback(rb)), rest) +: stack)
                case ChildrenRecursion.DoNotRecurse =>
                  loop(rest, stack)
              }
            case exe: Node.Exercise =>
              exerciseBegin(nid, exe) match {
                case ChildrenRecursion.DoRecurse =>
                  loop(exe.children.toFrontStack, ((nid, SI.Exercise(exe)), rest) +: stack)
                case ChildrenRecursion.DoNotRecurse =>
                  loop(rest, stack)
              }
            case node: Node.LeafOnlyAction =>
              leaf(nid, node)
              loop(rest, stack)
          }
        case None =>
          stack.pop match {
            case Some((((nid, either), brothers), rest)) =>
              either match {
                case SI.Rollback(rb) =>
                  rollbackEnd(nid, rb)
                  loop(brothers, rest)
                case SI.Exercise(exe) =>
                  exerciseEnd(nid, exe)
                  loop(brothers, rest)
              }
            case None =>
          }
      }

    loop(roots.toFrontStack, FrontStack.empty)
  }

  // This method visits to all nodes of the transaction in execution order.
  // Exercise nodes are visited twice: when execution reaches them and when execution leaves their body.
  final def foldInExecutionOrder[A](z: A)(
      exerciseBegin: (A, NodeId, Node.Exercise) => (A, ChildrenRecursion),
      rollbackBegin: (A, NodeId, Node.Rollback) => (A, ChildrenRecursion),
      leaf: (A, NodeId, Node.LeafOnlyAction) => A,
      exerciseEnd: (A, NodeId, Node.Exercise) => A,
      rollbackEnd: (A, NodeId, Node.Rollback) => A,
  ): A = {
    var acc = z
    foreachInExecutionOrder(
      exerciseBegin = (nid, node) => {
        val (acc2, bool) = exerciseBegin(acc, nid, node)
        acc = acc2
        bool
      },
      rollbackBegin = (nid, node) => {
        val (acc2, bool) = rollbackBegin(acc, nid, node)
        acc = acc2
        bool
      },
      leaf = (nid, node) => acc = leaf(acc, nid, node),
      exerciseEnd = (nid, node) => acc = exerciseEnd(acc, nid, node),
      rollbackEnd = (nid, node) => acc = rollbackEnd(acc, nid, node),
    )
    acc
  }

  // This method returns all node-ids reachable from the roots of a transaction.
  final def reachableNodeIds: Set[NodeId] = {
    foldInExecutionOrder[Set[NodeId]](Set.empty)(
      exerciseBegin = (acc, nid, _) => (acc + nid, ChildrenRecursion.DoRecurse),
      rollbackBegin = (acc, nid, _) => (acc + nid, ChildrenRecursion.DoRecurse),
      leaf = (acc, nid, _) => acc + nid,
      exerciseEnd = (acc, _, _) => acc,
      rollbackEnd = (acc, _, _) => acc,
    )
  }

  final def guessSubmitter: Either[String, Party] =
    rootNodes.map(_.requiredAuthorizers).toFrontStack.pop match {
      case None =>
        Left(s"Empty transaction")
      case Some((head, _)) if head.size != 1 =>
        Left(s"Transaction's roots do not have exactly one authorizer: $this")
      case Some((head, tail)) if tail.iterator.exists(_ != head) =>
        Left(s"Transaction's roots have different authorizers: $this")
      case Some((head, _)) =>
        Right(head.head)
    }

  /** The union of the informees of all the action nodes. */
  lazy val informees: Set[Ref.Party] =
    nodes.values.foldLeft(Set.empty[Ref.Party]) {
      case (acc, node: Node.Action) => acc | node.informeesOfNode
      case (acc, _: Node.Rollback) => acc
    }
}

object Transaction {

  private[this] val Empty = Transaction(HashMap.empty, ImmArray.Empty)

  private[lf] def empty: Transaction = Empty

  private[lf] case class NotWellFormedError(nid: NodeId, reason: NotWellFormedErrorReason)
  private[lf] sealed trait NotWellFormedErrorReason
  private[lf] case object DanglingNodeId extends NotWellFormedErrorReason
  private[lf] case object OrphanedNode extends NotWellFormedErrorReason
  private[lf] case object AliasedNode extends NotWellFormedErrorReason

  /** Transaction meta data
    *
    * @param submissionSeed   Populated with the submission seed when returned from [[com.daml.lf.engine.Engine.submit]].
    * @param submissionTime   The submission time
    * @param usedPackages     The set of all packages that are needed for the interpretation of the command. This
    *                         is done by first by establishing all the packages directly associated with action nodes
    *                         in the transaction (by calling [[Node.Action.packageIds]]). The [[usedPackages]] will then
    *                         be this set of packages combined with all packages on which there is a transitive
    *                         dependency (for details see [[com.daml.lf.engine.Engine.deps]]).
    * @param dependsOnTime    Indicates that the transaction computation depends on ledger time.
    * @param nodeSeeds        An association list that maps the node-id of create and exercise
    *                         nodes to their seed.
    * @param globalKeyMapping Input key mappings inferred during interpretation.
    * @param disclosedEvents  Disclosed create events that have been used in this transaction.
    */
  final case class Metadata(
      submissionSeed: Option[crypto.Hash],
      submissionTime: Time.Timestamp,
      usedPackages: Set[PackageId],
      dependsOnTime: Boolean,
      nodeSeeds: ImmArray[(NodeId, crypto.Hash)],
      globalKeyMapping: Map[GlobalKey, Option[Value.ContractId]],
      disclosedEvents: ImmArray[Node.Create],
  )

  def commitTransaction(submittedTransaction: SubmittedTransaction): CommittedTransaction =
    CommittedTransaction(submittedTransaction)

  def commitTransaction(
      submittedTransaction: SubmittedTransaction,
      f: crypto.Hash => Bytes,
  ): Either[String, CommittedTransaction] =
    submittedTransaction.suffixCid(f).map(CommittedTransaction(_))

  /** The state of a key at the beginning of the transaction.
    */
  sealed trait KeyInput extends Product with Serializable {
    def toKeyMapping: ContractStateMachine.KeyMapping
    def isActive: Boolean
  }

  /** No active contract with the given key.
    */
  sealed trait KeyInactive extends KeyInput {
    override def toKeyMapping: KeyMapping = ContractStateMachine.KeyInactive
    override def isActive: Boolean = false
  }

  /** A contract with the key will be created so the key must be inactive.
    */
  final case object KeyCreate extends KeyInactive

  /** Negative key lookup so the key mus tbe inactive.
    */
  final case object NegativeKeyLookup extends KeyInactive

  /** Key must be mapped to this active contract.
    */
  final case class KeyActive(cid: Value.ContractId) extends KeyInput {
    override def toKeyMapping: KeyMapping = ContractStateMachine.KeyActive(cid)
    override def isActive: Boolean = true
  }

  sealed abstract class ChildrenRecursion
  object ChildrenRecursion {
    case object DoRecurse extends ChildrenRecursion
    case object DoNotRecurse extends ChildrenRecursion
  }
}
