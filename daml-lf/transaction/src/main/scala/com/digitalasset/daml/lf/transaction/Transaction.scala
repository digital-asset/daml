// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package transaction

import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.GenTransaction.WithTxValue
import com.daml.lf.transaction.Node._
import com.daml.lf.value.Value
import scalaz.Equal

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.language.higherKinds

final case class VersionedTransaction[Nid, +Cid] private[lf] (
    version: TransactionVersion,
    private[lf] val transaction: GenTransaction.WithTxValue[Nid, Cid],
) extends HasTxNodes[Nid, Cid, Transaction.Value[Cid]]
    with value.CidContainer[VersionedTransaction[Nid, Cid]]
    with NoCopy {

  override protected def self: this.type = this

  @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
  def mapContractId[Cid2](f: Cid => Cid2): VersionedTransaction[Nid, Cid2] =
    VersionedTransaction(
      version,
      transaction = transaction.mapContractIdAndValue(f, _.mapContractId(f))
    )

  /** Increase the `version` if appropriate for `languageVersions`.
    *
    * This does not recur into the values herein; it is safe to apply
    * [[VersionedValue#typedBy]] to any subset of this `languageVersions` for all
    * values herein, unlike most [[value.Value]] operations.
    *
    * {{{
    *   val vt2 = vt.typedBy(someVers:_*)
    *   // safe if and only if vx() yields a subset of someVers
    *   vt2.copy(transaction = vt2.transaction
    *              .mapContractIdAndValue(identity, _.typedBy(vx():_*)))
    * }}}
    *
    * However, applying the ''same'' version set is probably not what you mean,
    * because the set of language versions that types a whole transaction is
    * probably not the same set as those language version[s] that type each
    * value, since each value can be typed by different modules.
    */
  def typedBy(languageVersions: LanguageVersion*): VersionedTransaction[Nid, Cid] = {
    import VersionTimeline._
    import Implicits._
    VersionedTransaction(
      latestWhenAllPresent(version, languageVersions map (a => a: SpecifiedVersion): _*),
      transaction,
    )
  }

  override def nodes: HashMap[Nid, GenNode.WithTxValue[Nid, Cid]] =
    transaction.nodes

  override def roots: ImmArray[Nid] =
    transaction.roots
}

object VersionedTransaction extends value.CidContainer2[VersionedTransaction] {

  override private[lf] def map2[A1, B1, C1, A2, B2, C2](
      f1: A1 => A2,
      f2: B1 => B2,
  ): VersionedTransaction[A1, B1] => VersionedTransaction[A2, B2] = {
    case VersionedTransaction(version, transaction) =>
      VersionedTransaction(version, transaction.map3(f1, f2, Value.VersionedValue.map1(f2)))
  }

  override private[lf] def foreach2[A, B](
      f1: A => Unit,
      f2: B => Unit,
  ): VersionedTransaction[A, B] => Unit = {
    case VersionedTransaction(_, transaction) =>
      transaction.foreach3(f1, f2, Value.VersionedValue.foreach1(f2))
  }

  private[lf] def unapply[Nid, Cid](
      arg: VersionedTransaction[Nid, Cid],
  ): Some[(TransactionVersion, WithTxValue[Nid, Cid])] =
    Some((arg.version, arg.transaction))

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
final private[lf] case class GenTransaction[Nid, +Cid, +Val](
    nodes: HashMap[Nid, GenNode[Nid, Cid, Val]],
    roots: ImmArray[Nid],
) extends HasTxNodes[Nid, Cid, Val]
    with value.CidContainer[GenTransaction[Nid, Cid, Val]] {

  import GenTransaction._

  override protected def self: this.type = this

  private[lf] def map3[Nid2, Cid2, Val2](
      f: Nid => Nid2,
      g: Cid => Cid2,
      h: Val => Val2
  ): GenTransaction[Nid2, Cid2, Val2] =
    GenTransaction.map3(f, g, h)(this)

  @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
  def mapContractIdAndValue[Cid2, Val2](
      f: Cid => Cid2,
      g: Val => Val2,
  ): GenTransaction[Nid, Cid2, Val2] =
    map3(identity, f, g)

  /** Note: the provided function must be injective, otherwise the transaction will be corrupted. */
  def mapNodeId[Nid2](f: Nid => Nid2): GenTransaction[Nid2, Cid, Val] =
    map3(f, identity, identity)

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
                case _: LeafOnlyNode[Cid, Val] => go(newErrors, newVisited, nids)
                case ne: NodeExercises[Nid, Cid, Val] =>
                  go(newErrors, newVisited, if (alreadyVisited) {
                    nids
                  } else {
                    ne.children ++: nids
                  })
              }
          }
      }
    val (errors, visited) = go(Set.empty, Set.empty, FrontStack(roots))
    val orphaned = nodes.keys.toSet.diff(visited).map(nid => NotWellFormedError(nid, OrphanedNode))
    errors ++ orphaned
  }

  /**
    * Compares two Transactions up to renaming of Nids. You most likely want to use this rather than ==, since the
    * Nid is irrelevant to the content of the transaction.
    */
  def equalForest[Cid2 >: Cid, Val2 >: Val](other: GenTransaction[_, Cid2, Val2]): Boolean =
    compareForest(other)(_ == _)

  /**
    * Compares two Transactions up to renaming of Nids. with the specified comparision of nodes
    * Nid is irrelevant to the content of the transaction.
    */
  def compareForest[Nid2, Cid2, Val2](other: GenTransaction[Nid2, Cid2, Val2])(
      compare: (GenNode[Nothing, Cid, Val], GenNode[Nothing, Cid2, Val2]) => Boolean,
  ): Boolean = {
    @tailrec
    def go(toCompare: FrontStack[(Nid, Nid2)]): Boolean =
      toCompare match {
        case FrontStack() => true
        case FrontStackCons((nid1, nid2), rest) =>
          val node1 = nodes(nid1)
          val node2 = other.nodes(nid2)
          node1 match {
            case nf1: NodeFetch[Cid, Val] =>
              node2 match {
                case nf2: NodeFetch[Cid2, Val2] => compare(nf1, nf2) && go(rest)
                case _ => false
              }
            case nc1: NodeCreate[Cid, Val] =>
              node2 match {
                case nc2: NodeCreate[Cid2, Val2] =>
                  compare(nc1, nc2) && go(rest)
                case _ => false
              }
            case ne1: NodeExercises[Nid, Cid, Val] =>
              node2 match {
                case ne2: NodeExercises[Nid2, Cid2, Val2] =>
                  val blankedNe1: NodeExercises[Nothing, Cid, Val] =
                    ne1.copy(children = ImmArray.empty)
                  val blankedNe2: NodeExercises[Nothing, Cid2, Val2] =
                    ne2.copy(children = ImmArray.empty)
                  compare(blankedNe1, blankedNe2) &&
                  ne1.children.length == ne2.children.length &&
                  go(ne1.children.zip(ne2.children) ++: rest)
                case _ => false
              }
            case nl1: NodeLookupByKey[Cid, Val] =>
              node2 match {
                case nl2: NodeLookupByKey[Cid2, Val2] =>
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

  /** Whether `other` is the result of reinterpreting this transaction.
    *
    * @note This function is asymmetric.
    */
  def isReplayedBy[Nid2, Cid2 >: Cid, Val2 >: Val](
      other: GenTransaction[Nid2, Cid2, Val2],
  )(implicit ECid: Equal[Cid2], EVal: Equal[Val2]): Boolean =
    compareForest(other)(Node.isReplayedBy(_, _))

  /** checks that all the values contained are serializable */
  def serializable(f: Val => ImmArray[String]): ImmArray[String] = {
    fold(BackStack.empty[String]) {
      case (errs, (_, node)) =>
        node match {
          case _: NodeFetch[Cid, Val] => errs
          case nc: NodeCreate[Cid, Val] =>
            errs :++ f(nc.coinst.arg) :++ (nc.key match {
              case None => ImmArray.empty
              case Some(key) => f(key.key)
            })
          case ne: NodeExercises[Nid, Cid, Val] => errs :++ f(ne.chosenValue)
          case nlbk: NodeLookupByKey[Cid, Val] => errs :++ f(nlbk.key.key)
        }
    }.toImmArray
  }

  /** Visit every `Val`. */
  def foldValues[Z](z: Z)(f: (Z, Val) => Z): Z =
    fold(z) {
      case (z, (_, n)) =>
        n match {
          case c: Node.NodeCreate[_, Val] =>
            val z1 = f(z, c.coinst.arg)
            val z2 = c.key match {
              case None => z1
              case Some(k) => f(z1, k.key)
            }
            z2
          case nf: Node.NodeFetch[_, Val] => nf.key.fold(z)(k => f(z, k.key))
          case e: Node.NodeExercises[_, _, Val] => f(z, e.chosenValue)
          case lk: Node.NodeLookupByKey[_, Val] => f(z, lk.key.key)
        }
    }

  private[lf] def foreach3(fNid: Nid => Unit, fCid: Cid => Unit, fVal: Val => Unit): Unit =
    GenTransaction.foreach3(fNid, fCid, fVal)(this)
}

sealed abstract class HasTxNodes[Nid, +Cid, +Val] {

  def nodes: HashMap[Nid, GenNode[Nid, Cid, Val]]
  def roots: ImmArray[Nid]

  /**
    * This function traverses the transaction tree in pre-order traversal (i.e. exercise node are traversed before their children).
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  final def foreach(f: (Nid, GenNode[Nid, Cid, Val]) => Unit): Unit = {

    @tailrec
    def go(toVisit: FrontStack[Nid]): Unit = toVisit match {
      case FrontStack() =>
      case FrontStackCons(nodeId, toVisit) =>
        val node = nodes(nodeId)
        f(nodeId, node)
        node match {
          case _: LeafOnlyNode[Cid, Val] => go(toVisit)
          case ne: NodeExercises[Nid, Cid, Val] => go(ne.children ++: toVisit)
        }
    }

    go(FrontStack(roots))
  }

  /**
    * Traverses the transaction tree in pre-order traversal (i.e. exercise nodes are traversed before their children)
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  final def fold[A](z: A)(f: (A, (Nid, GenNode[Nid, Cid, Val])) => A): A = {
    var acc = z
    foreach { (nodeId, node) =>
      // make sure to not tie the knot by mistake by evaluating early
      val acc2 = acc
      acc = f(acc2, (nodeId, node))
    }
    acc
  }

  /**
    * A fold over the transaction that maintains global and path-specific state.
    * Takes constant stack space. Returns the global state.
    *
    * Used to for example compute the roots of per-party projections from the
    * transaction.
    */
  final def foldWithPathState[A, B](globalState0: A, pathState0: B)(
      op: (A, B, Nid, GenNode[Nid, Cid, Val]) => (A, B),
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
          case _: LeafOnlyNode[Cid, Val] => go(toVisit)
          case ne: NodeExercises[Nid, Cid, Val] =>
            go(ne.children.map(_ -> newPathState) ++: toVisit)
        }
    }

    go(FrontStack(roots.map(_ -> pathState0)))
    globalState
  }

  final def localContracts[Cid2 >: Cid]: Map[Cid2, Nid] =
    fold(Map.empty[Cid2, Nid]) {
      case (acc, (nid, create @ Node.NodeCreate(_, _, _, _, _, _))) =>
        acc.updated(create.coid, nid)
      case (acc, _) => acc
    }

  /**
    * Returns the IDs of all input contracts that are used by this transaction.
    */
  final def inputContracts[Cid2 >: Cid]: Set[Cid2] =
    fold(Set.empty[Cid2]) {
      case (acc, (_, Node.NodeExercises(coid, _, _, _, _, _, _, _, _, _, _, _, _))) =>
        acc + coid
      case (acc, (_, Node.NodeFetch(coid, _, _, _, _, _, _))) =>
        acc + coid
      case (acc, (_, Node.NodeLookupByKey(_, _, _, Some(coid)))) =>
        acc + coid
      case (acc, _) => acc
    } -- localContracts.keySet

  // This method visits to all nodes of the transaction in execution order.
  // Exercise nodes are visited twice: when execution reaches them and when execution leaves their body.
  final def foreachInExecutionOrder(
      exerciseBegin: (Nid, Node.NodeExercises[Nid, Cid, Val]) => Unit,
      leaf: (Nid, Node.LeafOnlyNode[Cid, Val]) => Unit,
      exerciseEnd: (Nid, Node.NodeExercises[Nid, Cid, Val]) => Unit,
  ): Unit = {
    @tailrec
    def loop(
        currNodes: FrontStack[Nid],
        stack: FrontStack[((Nid, Node.NodeExercises[Nid, Cid, Val]), FrontStack[Nid])],
    ): Unit =
      currNodes match {
        case FrontStackCons(nid, rest) =>
          nodes(nid) match {
            case exe: NodeExercises[Nid, Cid, Val] =>
              exerciseBegin(nid, exe)
              loop(FrontStack(exe.children), (((nid, exe), rest)) +: stack)
            case node: Node.LeafOnlyNode[Cid, Val] =>
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
      exerciseBegin: (A, Nid, Node.NodeExercises[Nid, Cid, Val]) => A,
      leaf: (A, Nid, Node.LeafOnlyNode[Cid, Val]) => A,
      exerciseEnd: (A, Nid, Node.NodeExercises[Nid, Cid, Val]) => A,
  ): A = {
    var acc = z
    foreachInExecutionOrder(
      (nid, node) => acc = exerciseBegin(acc, nid, node),
      (nid, node) => acc = leaf(acc, nid, node),
      (nid, node) => acc = exerciseEnd(acc, nid, node),
    )
    acc
  }

}

private[lf] object GenTransaction extends value.CidContainer3[GenTransaction] {

  type WithTxValue[Nid, +Cid] = GenTransaction[Nid, Cid, Transaction.Value[Cid]]

  private val Empty =
    GenTransaction[Nothing, Nothing, Nothing](
      HashMap.empty[Nothing, Nothing],
      ImmArray.empty[Nothing])

  def empty[A, B, C]: GenTransaction[A, B, C] = Empty.asInstanceOf[GenTransaction[A, B, C]]

  case class NotWellFormedError[Nid](nid: Nid, reason: NotWellFormedErrorReason)
  sealed trait NotWellFormedErrorReason
  case object DanglingNodeId extends NotWellFormedErrorReason
  case object OrphanedNode extends NotWellFormedErrorReason
  case object AliasedNode extends NotWellFormedErrorReason

  override private[lf] def map3[A1, A2, A3, B1, B2, B3](
      f1: A1 => B1,
      f2: A2 => B2,
      f3: A3 => B3,
  ): GenTransaction[A1, A2, A3] => GenTransaction[B1, B2, B3] = {
    case GenTransaction(nodes, roots) =>
      GenTransaction(
        nodes = nodes.map {
          case (nodeId, node) =>
            f1(nodeId) -> GenNode.map3(f1, f2, f3)(node)
        },
        roots = roots.map(f1)
      )
  }

  override private[lf] def foreach3[A, B, C](
      f1: A => Unit,
      f2: B => Unit,
      f3: C => Unit,
  ): GenTransaction[A, B, C] => Unit = {
    case GenTransaction(nodes, _) =>
      nodes.foreach {
        case (nodeId, node) =>
          f1(nodeId)
          GenNode.foreach3(f1, f2, f3)(node)
      }
  }
}

object Transaction {

  type NodeId = Value.NodeId
  val NodeId = Value.NodeId

  @deprecated("Use daml.lf.value.Value.ContractId directly", since = "1.4.0")
  type TContractId = Value.ContractId

  type Value[+Cid] = Value.VersionedValue[Cid]

  type ContractInst[+Cid] = Value.ContractInst[Value[Cid]]

  /** Transaction nodes */
  type Node = GenNode.WithTxValue[NodeId, Value.ContractId]
  type LeafNode = LeafOnlyNode.WithTxValue[Value.ContractId]

  /** (Complete) transactions, which are the result of interpreting a
    *  ledger-update. These transactions are consumed by either the
    *  scenario-interpreter or the DAML-engine code. Both of these
    *  code-paths share the computations for segregating the
    *  transaction into party-specific ledgers and for computing
    *  divulgence of contracts.
    *
    */
  type Transaction = VersionedTransaction[NodeId, Value.ContractId]
  val Transaction = VersionedTransaction

  /** Transaction meta data
    * @param submissionSeed: the submission seed used to derive the contract IDs.
    *        If undefined no seed has been used (the legacy contract ID scheme
    *        have been used) or it is unknown (output of partial reinterpretation).
    * @param submissionTime: the submission time
    * @param usedPackages The set of packages used during command processing.
    *        This is a hint for what packages are required to validate
    *        the transaction using the current interpreter.
    *        If set to `empty` the package dependency have not be computed.
    * @param dependsOnTime: indicate the transaction computation depends on ledger
    *        time.
    * @param nodeSeeds: An association list that maps to each ID of create and exercise
    *        nodes its seeds.
    * @param byKeyNodes: The list of the IDs of each node that corresponds to a FetchByKey,
    *        LookupByKey, or ExerciseByKey commands. Empty in case of validation or
    *        reinterpretation
    */
  final case class Metadata(
      submissionSeed: Option[crypto.Hash],
      submissionTime: Time.Timestamp,
      usedPackages: Set[PackageId],
      dependsOnTime: Boolean,
      nodeSeeds: ImmArray[(Value.NodeId, crypto.Hash)],
      byKeyNodes: ImmArray[Value.NodeId],
  )

  sealed abstract class DiscriminatedSubtype[X] {
    type T <: X
    def apply(x: X): T
    def subst[F[_]](fx: F[X]): F[T]
  }

  object DiscriminatedSubtype {
    def apply[X]: DiscriminatedSubtype[X] = new DiscriminatedSubtype[X] {
      override type T = X
      override def apply(x: X): T = x
      override def subst[F[_]](fx: F[X]): F[T] = fx
    }
  }

  val SubmittedTransaction = DiscriminatedSubtype[Transaction]
  type SubmittedTransaction = SubmittedTransaction.T

  val CommittedTransaction = DiscriminatedSubtype[Transaction]
  type CommittedTransaction = CommittedTransaction.T

  def commitTransaction(tx: SubmittedTransaction): CommittedTransaction =
    CommittedTransaction(tx)

  def commitTransaction(
      tx: SubmittedTransaction,
      f: crypto.Hash => Bytes,
  ): Either[String, CommittedTransaction] =
    tx.suffixCid(f).map(CommittedTransaction(_))

  /** Errors that can happen during building transactions. */
  sealed abstract class TransactionError extends Product with Serializable

  /** Signal that a 'endExercise' was called in a root-context; i.e.,
    *  without a matching 'beginExercise'.
    */
  case object EndExerciseInRootContext extends TransactionError

  /** Signals that the contract-id `coid` was expected to be active, but
    *  is not.
    */
  final case class ContractNotActive(
      coid: Value.ContractId,
      templateId: TypeConName,
      consumedBy: NodeId)
      extends TransactionError

}
