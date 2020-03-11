// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.value.Value
import scalaz.Equal

import scala.annotation.tailrec
import scala.collection.immutable.HashMap

case class VersionedTransaction[Nid, Cid](
    version: TransactionVersion,
    transaction: GenTransaction.WithTxValue[Nid, Cid],
) {

  @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
  def mapContractId[Cid2](f: Cid => Cid2): VersionedTransaction[Nid, Cid2] =
    copy(transaction = transaction.mapContractIdAndValue(f, _.mapContractId(f)))

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
    copy(
      version = latestWhenAllPresent(version, languageVersions map (a => a: SpecifiedVersion): _*),
    )
  }
}

/** General transaction type
  *
  * Abstracts over NodeId type and ContractId type
  * ContractId restricts the occurrence of contractIds
  * either AbsoluteContractId if only absolute ids occur
  * or ContractId when both absolute and relative ids are allowed
  *
  * The Cid parameter is invariant on purpose, since we do not want
  * to confuse transactions with AbsoluteContractId and ones with ContractId.
  * For example, when enriching the transaction the difference is key.
  *
  * @param nodes The nodes of this transaction.
  * @param roots References to the root nodes of the transaction.
  * Users of this class may assume that all instances are well-formed, i.e., `isWellFormed.isEmpty`.
  * For performance reasons, users are not required to call `isWellFormed`.
  * Therefore, it is '''forbidden''' to create ill-formed instances, i.e., instances with `!isWellFormed.isEmpty`.
  */
final case class GenTransaction[Nid, +Cid, +Val](
    nodes: HashMap[Nid, GenNode[Nid, Cid, Val]],
    roots: ImmArray[Nid],
) extends value.CidContainer[GenTransaction[Nid, Cid, Val]] {

  import GenTransaction._

  override protected val self: this.type = this

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

  /**
    * This function traverses the transaction tree in pre-order traversal (i.e. exercise node are traversed before their children).
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  def foreach(f: (Nid, GenNode[Nid, Cid, Val]) => Unit): Unit = {

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
    * Traverses the transaction tree in pre-order traversal (i.e. exercise node are traversed before their children)
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  def fold[A](z: A)(f: (A, (Nid, GenNode[Nid, Cid, Val])) => A): A = {
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
  def foldWithPathState[A, B](globalState0: A, pathState0: B)(
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

  def localContracts[Cid2 >: Cid]: Map[Cid2, Nid] =
    fold(Map.empty[Cid2, Nid]) {
      case (acc, (nid, create @ Node.NodeCreate(_, _, _, _, _, _, _))) =>
        acc.updated(create.coid, nid)
      case (acc, _) => acc
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
            case nf1: NodeFetch[Cid] =>
              node2 match {
                case nf2: NodeFetch[Cid2] => compare(nf1, nf2) && go(rest)
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
          case _: NodeFetch[Cid] => errs
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
          case _: Node.NodeFetch[_] => z
          case e: Node.NodeExercises[_, _, Val] => f(z, e.chosenValue)
          case lk: Node.NodeLookupByKey[_, Val] => f(z, lk.key.key)
        }
    }
}

object GenTransaction extends value.CidContainer3WithDefaultCidResolver[GenTransaction] {

  type WithTxValue[Nid, +Cid] = GenTransaction[Nid, Cid, Transaction.Value[Cid]]

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
}

object Transaction {

  type NodeId = Value.NodeId
  val NodeId = Value.NodeId

  type TContractId = Value.ContractId

  type Value[+Cid] = Value.VersionedValue[Cid]

  /** Transaction nodes */
  type Node = GenNode.WithTxValue[NodeId, TContractId]
  type LeafNode = LeafOnlyNode.WithTxValue[TContractId]

  /** (Complete) transactions, which are the result of interpreting a
    *  ledger-update. These transactions are consumed by either the
    *  scenario-interpreter or the DAML-engine code. Both of these
    *  code-paths share the computations for segregating the
    *  transaction into party-specific ledgers and for computing
    *  divulgence of contracts.
    *
    */
  type Transaction = GenTransaction.WithTxValue[NodeId, TContractId]

  /* Transaction meta data
   * @param submissionTime: submission time
   * @param usedPackages The set of packages used during command processing.
   *        This is a hint for what packages are required to validate
   *        the transaction using the current interpreter.
   *        The used packages are not serialized using [[TransactionCoder]].
   * @dependsOnTime: indicate the transaction computation depends on ledger
   *        time.
   */
  final case class Metadata(
      submissionTime: Time.Timestamp,
      usedPackages: Set[PackageId],
      dependsOnTime: Boolean
  )

  type AbsTransaction = GenTransaction.WithTxValue[NodeId, Value.AbsoluteContractId]

  type AbsNode = GenNode.WithTxValue[NodeId, Value.AbsoluteContractId]

  /** Errors that can happen during building transactions. */
  sealed abstract class TransactionError extends Product with Serializable

  /** Signal that a 'endExercise' was called in a root-context; i.e.,
    *  without a matching 'beginExercise'.
    */
  case object EndExerciseInRootContext extends TransactionError

  /** Signals that the contract-id `coid` was expected to be active, but
    *  is not.
    */
  final case class ContractNotActive(coid: TContractId, templateId: TypeConName, consumedBy: NodeId)
      extends TransactionError

}
