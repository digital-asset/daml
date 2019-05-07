// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.archive.LanguageVersion
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import Node._
import value.Value
import Value._
import scalaz.Equal

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.breakOut
import scala.util.Try

case class VersionedTransaction[Nid, Cid](
    version: TransactionVersion,
    transaction: GenTransaction.WithTxValue[Nid, Cid]) {
  def mapContractId[Cid2](f: Cid => Cid2): VersionedTransaction[Nid, Cid2] = this.copy(
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
    import VersionTimeline._, Implicits._
    copy(
      version = latestWhenAllPresent(version, languageVersions map (a => a: SpecifiedVersion): _*))
  }
}

/** General transaction type
  *
  * Abstracts over NodeId type and ContractId type
  * ContractId restricts the occurence of contractIds
  * either AbsoluteContractId if only absolute ids occur
  * or ContractId when both absolute and relative ids are allowed
  *
  * The Cid parameter is invariant on purpose, since we do not want
  * to confuse transactions with AbsoluteContractId and ones with ContractId.
  * For example, when enriching the transaction the difference is key.
  *
  * @param nodes The nodes of this transaction.
  * @param roots References to the root nodes of the transaction.
  * @param usedPackages The set of packages used during interpretation.
  *                     This is a hint for what packages are required to validate
  *                     the transaction using the current interpreter. This assumption
  *                     may not hold if new DAML engine implementations are introduced
  *                     as some packages may be only referenced during compilation to
  *                     engine's internal form. The used packages are not serialized
  *                     using [[TransactionCoder]].
  *
  * Users of this class may assume that all instances are well-formed, i.e., `isWellFormed.isEmpty`.
  * For performance reasons, users are not required to call `isWellFormed`.
  * Therefore, it is '''forbidden''' to create ill-formed instances, i.e., instances with `!isWellFormed.isEmpty`.
  */
case class GenTransaction[Nid, Cid, +Val](
    nodes: Map[Nid, GenNode[Nid, Cid, Val]],
    roots: ImmArray[Nid],
    usedPackages: Set[PackageId]) {
  import GenTransaction._

  def mapContractIdAndValue[Cid2, Val2](
      f: Cid => Cid2,
      g: Val => Val2): GenTransaction[Nid, Cid2, Val2] = {
    val nodes2: Map[Nid, GenNode[Nid, Cid2, Val2]] = nodes.mapValues(_.mapContractIdAndValue(f, g))
    this.copy(nodes = nodes2)
  }

  def mapContractId[Cid2](f: Cid => Cid2)(
      implicit ev: Val <:< VersionedValue[Cid]): WithTxValue[Nid, Cid2] = {
    def g(v: Val): VersionedValue[Cid2] = v.mapContractId(f)
    this.mapContractIdAndValue(f, g)
  }

  /** Note: the provided function must be injective, otherwise the transaction will be corrupted. */
  def mapNodeId[Nid2](f: Nid => Nid2): GenTransaction[Nid2, Cid, Val] =
    transaction.GenTransaction(
      roots = roots.map(f),
      nodes = nodes.map {
        case (nid, node) => (f(nid), node.mapNodeId(f))
      },
      usedPackages = usedPackages
    )

  /**
    * This function traverses the roots in order, visiting children based on the traverse order provided.
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  def foreach(order: TraverseOrder, f: (Nid, GenNode[Nid, Cid, Val]) => Unit): Unit = order match {

    case TopDown =>
      @tailrec
      def go(toVisit: FrontStack[Nid]): Unit = toVisit match {
        case FrontStack() => ()
        case FrontStackCons(nodeId, toVisit) =>
          val node = nodes(nodeId)
          f(nodeId, node)
          node match {
            case _: LeafOnlyNode[Cid, Val] => go(toVisit)
            case ne: NodeExercises[Nid, Cid, Val] => go(ne.children ++: toVisit)
          }
      }
      go(FrontStack(roots))

    case BottomUp =>
      sealed trait Step
      case object Done extends Step
      final case class ConsumeNodes(nodes: ImmArray[Nid], next: Step) extends Step
      final case class VisitExercise(nodeId: Nid, node: GenNode[Nid, Cid, Val], next: Step)
          extends Step

      @tailrec
      def go(step: Step): Unit = step match {
        case Done => ()
        case ConsumeNodes(toVisit, next) =>
          toVisit match {
            case ImmArray() => go(next)
            case ImmArrayCons(nodeId, toVisit) =>
              val node = nodes(nodeId)
              node match {
                case _: LeafOnlyNode[Cid, Val] =>
                  f(nodeId, node)
                  go(ConsumeNodes(toVisit, next))
                case ne: NodeExercises[Nid, Cid, Val] =>
                  go(
                    ConsumeNodes(
                      ne.children,
                      VisitExercise(nodeId, node, ConsumeNodes(toVisit, next))))
              }
          }
        case VisitExercise(nodeId, node, next) =>
          f(nodeId, node)
          go(next)
      }
      go(ConsumeNodes(roots, Done))

    case AnyOrder =>
      nodes foreach f.tupled
  }

  /**
    * Traverses in the same order as foreach.
    *
    * Takes constant stack space. Crashes if the transaction is not well formed (see `isWellFormed`)
    */
  def fold[A](order: TraverseOrder, z: A)(f: (A, (Nid, GenNode[Nid, Cid, Val])) => A): A = {
    var acc = z
    foreach(order, (nodeId, node) => {
      // make sure to not tie the knot by mistake by evaluating early
      val acc2 = acc
      acc = f(acc2, (nodeId, node))
    })
    acc
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
        toVisit: FrontStack[Nid]): (Set[NotWellFormedError[Nid]], Set[Nid]) = {
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
                  go(newErrors, newVisited, if (alreadyVisited) { nids } else {
                    ne.children ++: nids
                  })
              }
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
    *
    * @note [[uncheckedVariance]] only to avoid the contains problem
    *       <https://stackoverflow.com/questions/8360413/selectively-disable-subsumption-in-scala-correctly-type-list-contains>.
    *       We can get away with it because we don't admit ''any'' overrides.
    *       However, requiring [[Equal]]`[Val2]` as `isReplayedBy` does would
    *       also solve the contains problem.  Food for thought
    */
  final def equalForest(other: GenTransaction[_, Cid, Val @uncheckedVariance]): Boolean =
    compareForest(other)(_ == _)

  /**
    * Compares two Transactions up to renaming of Nids. with the specified comparision of nodes
    * Nid is irrelevant to the content of the transaction.
    */
  final def compareForest[Nid2, Cid2, Val2](other: GenTransaction[Nid2, Cid2, Val2])(
      compare: (GenNode[Nothing, Cid, Val], GenNode[Nothing, Cid2, Val2]) => Boolean): Boolean = {
    @tailrec
    def go(toCompare: FrontStack[(Nid, Nid2)]): Boolean = toCompare match {
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

    if (roots.length != other.roots.length) {
      false
    } else {
      go(FrontStack(roots.zip(other.roots)))
    }
  }

  /** Whether `other` is the result of reinterpreting this transaction.
    *
    * @note This function is asymmetric.
    */
  private[lf] def isReplayedBy[Nid2, Val2 >: Val](other: GenTransaction[Nid2, Cid, Val2])(
      implicit ECid: Equal[Cid],
      EVal: Equal[Val2]): Boolean =
    compareForest(other)(Node.isReplayedBy(_, _))

  /** checks that all the values contained are serializable */
  def serializable(f: Val => ImmArray[String]): ImmArray[String] = {
    fold(TopDown, BackStack.empty[String]) {
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
    this.fold(GenTransaction.AnyOrder, z) {
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

object GenTransaction {
  type WithTxValue[Nid, Cid] = GenTransaction[Nid, Cid, Transaction.Value[Cid]]

  sealed trait TraverseOrder
  case object BottomUp extends TraverseOrder // visits exercise children before the exercise node itself
  case object TopDown extends TraverseOrder // visits exercise nodes first, then their children
  case object AnyOrder extends TraverseOrder // arbitrary order chosen by implementation

  case class NotWellFormedError[Nid](nid: Nid, reason: NotWellFormedErrorReason)
  sealed trait NotWellFormedErrorReason
  case object DanglingNodeId extends NotWellFormedErrorReason
  case object OrphanedNode extends NotWellFormedErrorReason
  case object AliasedNode extends NotWellFormedErrorReason
}

object Transaction {
  type NodeId = Value.NodeId
  val NodeId = Value.NodeId

  type ContractId = Value.ContractId

  type Value[+Cid] = Value.VersionedValue[Cid]

  /** Transaction nodes */
  type Node = GenNode.WithTxValue[NodeId, ContractId]

  /** (Complete) transactions, which are the result of interpreting a
    *  ledger-update. These transactions are consumed by either the
    *  scenario-interpreter or the DAML-engine code. Both of these
    *  code-paths share the computations for segregating the
    *  transaction into party-specific ledgers and for computing
    *  divulgence of contracts.
    *
    */
  type Transaction = GenTransaction.WithTxValue[NodeId, ContractId]

  /** Errors that can happen during building transactions. */
  sealed abstract class TransactionError extends Product with Serializable

  /** Signal that a 'endExercise' was called in a root-context; i.e.,
    *  without a matching 'beginExercise'.
    */
  case object EndExerciseInRootContext extends TransactionError

  /** Signals that the contract-id `coid` was expected to be active, but
    *  is not.
    */
  final case class ContractNotActive(coid: ContractId, templateId: TypeConName, consumedBy: NodeId)
      extends TransactionError

  /** Contexts of the transaction graph builder, which we use to record
    *  the sub-transaction structure due to 'exercises' statements.
    */
  sealed abstract class Context extends Product with Serializable

  /** The root context, which is what we use when we are not exercising
    *  a choice.
    */
  case object ContextRoot extends Context

  /** Context when creating a sub-transaction due to an exercises. */
  final case class ContextExercises(ctx: ExercisesContext) extends Context

  /** Context information to remember when building a sub-transaction
    *  due to an 'exercises' statement.
    *
    *  @param targetId Contract-id referencing the contract-instance on
    *                  which we are exercising a choice.
    *  @param choiceId Label of the choice that we are exercising.
    *  @param consuming True if the choice consumes the contract.
    *  @param actingParties The parties exercising the choice.
    *  @param chosenValue The chosen value.
    *  @param signatories The signatories of the contract.
    *  @param stakeholders The stakeholders of the contract.
    *  @param controllers The controllers of the choice.
    *  @param exercisesNodeId The node to be inserted once we've
    *                         finished this sub-transaction.
    *  @param parentContext The context in which the exercises is
    *                       happening.
    *  @param parentRoots The root nodes of the parent context.
    */
  case class ExercisesContext(
      targetId: ContractId,
      templateId: TypeConName,
      choiceId: ChoiceName,
      optLocation: Option[Location],
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Value[ContractId],
      signatories: Set[Party],
      stakeholders: Set[Party],
      controllers: Set[Party],
      exercisesNodeId: NodeId,
      parentContext: Context,
      parentRoots: BackStack[NodeId]
  )

  /** A transaction under construction
    *
    *  @param nextNodeId The next free node-id to use.
    *  @param nodes The nodes of the transaction graph being built up.
    *  @param roots Root nodes of the current context.
    *  @param consumedBy 'ContractId's of all contracts that have
    *                    been consumed by nodes up to now.
    *  @param context The context of what sub-transaction is being
    *                 built.
    *  @param aborted The error that lead to aborting the building of
    *                 this transaction. We inline this error to allow
    *                 reporting the error jointly with the state that
    *                 the transaction was in when aborted. It is up to
    *                 the caller to check for 'isAborted' after every
    *                 change to a transaction.
    *  @param keys A local store of the contract keys. Note that this contains
    *              info both about relative and absolute contract ids. We must
    *              do this because absolute contract ids can be archived as
    *              part of execution, and we must record these archivals locally.
    *              Note: it is important for keys that we know to not be present
    *              to be present as [[None]]. The reason for this is that we must
    *              record the "no key" information for absolute contract ids that
    *              we archive. This is not an optimization and is required for
    *              correct semantics, since otherwise lookups for keys for
    *              locally archived absolute contract ids will succeed wrongly.
    * @param usedPackages The set of packages used during interpretation.
    *                     This is a hint for what packages are required to validate
    *                     the transaction using the current interpreter. This assumption
    *                     may not hold if new DAML engine implementations are introduced
    *                     as some packages may be only referenced during compilation to
    *                     engine's internal form.
    */
  case class PartialTransaction(
      nextNodeId: NodeId,
      nodes: Map[NodeId, Node],
      roots: BackStack[NodeId],
      consumedBy: Map[ContractId, NodeId],
      context: Context,
      aborted: Option[TransactionError],
      keys: Map[GlobalKey, Option[ContractId]],
      usedPackages: Set[PackageId]
  ) {

    private def computeRoots: Set[NodeId] = {
      val allChildNodeIds: Set[NodeId] = nodes.values.flatMap {
        case _: LeafOnlyNode[_, _] => Nil
        case NodeExercises(_, _, _, _, _, _, _, _, _, _, children, _) => children.toSeq
      }(breakOut)

      nodes.keySet diff allChildNodeIds
    }

    def nodesToString: String =
      if (nodes.isEmpty) "<empty transaction>"
      else {
        val sb = new StringBuilder()

        def addToStringBuilder(
            nid: NodeId,
            node: GenNode.WithTxValue[NodeId, ContractId],
            rootPrefix: String): Unit = {
          sb.append(rootPrefix)
            .append("node ")
            .append(nid.index)
            .append(": ")
            .append(node.toString)
            .append(", ")
          ()
        }

        def removeTrailingComma(): Unit = {
          if (sb.length >= 2) sb.setLength(sb.length - 2) // remove trailing ", "
        }

        // roots field is not initialized when this method is executed on a failed transaction,
        // so we need to compute them.
        val rootNodes = computeRoots
        val tx = GenTransaction(nodes, ImmArray(rootNodes), usedPackages)

        tx.foreach(GenTransaction.TopDown, { (nid, node) =>
          val rootPrefix = if (rootNodes.contains(nid)) "root " else ""
          addToStringBuilder(nid, node, rootPrefix)
        })
        removeTrailingComma()

        sb.toString
      }

    /** `True` if building the `PartialTransaction` has been aborted due
      *  to a wrong build-step
      */
    def isAborted = aborted.isDefined

    /** Finish building a transaction; i.e., try to extract a complete
      *  transaction from the given 'PartialTransaction'. This fails if
      *  the 'PartialTransaction' is not yet complete or has been
      *  aborted.
      */
    def finish: Either[PartialTransaction, Transaction] =
      if (isAborted) Left(this)
      else
        context match {
          case ContextExercises(_) => Left(this)
          case ContextRoot =>
            Right(
              GenTransaction(
                nodes = nodes,
                roots = roots.toImmArray,
                usedPackages = usedPackages
              ))
          case _ => Left(this)
        }

    /** Lookup the contract associated to 'RelativeContractId'.
      * Return the contract instance and the node in which it was
      * consumed if any.
      */
    def lookupLocalContract(lcoid: RelativeContractId)
      : Option[(ContractInst[Transaction.Value[ContractId]], Option[NodeId])] = {
      def guard(b: Boolean): Option[Unit] = if (b) Some(()) else None
      for {
        _ <- guard(0 <= lcoid.txnid.index)
        node <- nodes.get(lcoid.txnid)
        coinst <- node match {
          case create: NodeCreate.WithTxValue[ContractId] =>
            Some((create.coinst, consumedBy.get(lcoid)))
          case _: NodeExercises[_, _, _] | _: NodeFetch[_] | _: NodeLookupByKey[_, _] => None
        }
      } yield coinst
    }

    /** Extend the 'PartialTransaction' with a node for creating a
      * contract instance.
      */
    def create(
        coinst: ContractInst[Value[ContractId]],
        optLocation: Option[Location],
        signatories: Set[Party],
        stakeholders: Set[Party],
        key: Option[KeyWithMaintainers[Value[ContractId]]])
      : Either[String, (ContractId, PartialTransaction)] = {
      val serializableErrs = serializable(coinst.arg)
      if (serializableErrs.nonEmpty) {
        Left(
          s"""Trying to create a contract with a non-serializable value: ${serializableErrs.iterator
            .mkString(",")}""")
      } else {
        val (nid, ptx) =
          insertFreshNode(
            nid =>
              NodeCreate(
                RelativeContractId(nid),
                coinst,
                optLocation,
                signatories,
                stakeholders,
                key),
            None)
        val cid = nodeIdToContractId(nid)
        // if we have a contract key being added, include it in the list of
        // active keys
        key match {
          case None => Right((cid, ptx))
          case Some(k) =>
            // TODO is there a nicer way of doing this?
            val mbNoRels =
              Try(k.key.mapContractId {
                case abs: AbsoluteContractId => abs
                case rel: RelativeContractId =>
                  throw new RuntimeException(
                    s"Trying to create contract key with relative contract id $rel")
              }).toEither.left.map(_.getMessage)
            mbNoRels.map { noRels =>
              val gk = GlobalKey(coinst.template, noRels)
              (cid, ptx.copy(keys = ptx.keys + (gk -> Some(cid))))
            }
        }
      }
    }

    /** Mark a package as being used in the process of preparing the
      * transaction.
      */
    def markPackage(packageId: PackageId): PartialTransaction =
      this.copy(usedPackages = usedPackages + packageId)

    def serializable(a: Value[ContractId]): ImmArray[String] = a.value.serializable()

    def insertFetch(
        coid: ContractId,
        templateId: TypeConName,
        optLocation: Option[Location],
        actingParties: Set[Party],
        signatories: Set[Party],
        stakeholders: Set[Party]): PartialTransaction =
      mustBeActive(
        coid,
        templateId,
        insertFreshNode(
          _ =>
            NodeFetch(
              coid,
              templateId,
              optLocation,
              Some(actingParties),
              signatories,
              stakeholders),
          None)._2)

    def insertLookup(
        templateId: TypeConName,
        optLocation: Option[Location],
        key: KeyWithMaintainers[Value.VersionedValue[Nothing]],
        result: Option[ContractId]): PartialTransaction =
      insertFreshNode(_ => NodeLookupByKey(templateId, optLocation, key, result), None)._2

    def beginExercises(
        targetId: ContractId,
        templateId: TypeConName,
        choiceId: ChoiceName,
        optLocation: Option[Location],
        consuming: Boolean,
        actingParties: Set[Party],
        signatories: Set[Party],
        stakeholders: Set[Party],
        controllers: Set[Party],
        mbKey: Option[Value[AbsoluteContractId]],
        chosenValue: Value[ContractId]
    ): Either[String, PartialTransaction] = {
      val serializableErrs = serializable(chosenValue)
      if (serializableErrs.nonEmpty) {
        Left(
          s"""Trying to exercise a choice with a non-serializable value: ${serializableErrs.iterator
            .mkString(",")}""")
      } else {
        Right(
          mustBeActive(
            targetId,
            templateId,
            withFreshNodeId {
              case (nodeId, ptx) =>
                ptx
                  .copy(
                    context = ContextExercises(
                      ExercisesContext(
                        targetId = targetId,
                        templateId = templateId,
                        choiceId = choiceId,
                        optLocation = optLocation,
                        consuming = consuming,
                        actingParties = actingParties,
                        chosenValue = chosenValue,
                        signatories = signatories,
                        stakeholders = stakeholders,
                        controllers = controllers,
                        exercisesNodeId = nodeId,
                        parentContext = ptx.context,
                        parentRoots = ptx.roots
                      )),
                    // important: the semantics of DAML dictate that contracts are immediately
                    // inactive as soon as you exercise it. therefore, mark it as consumed now.
                    consumedBy =
                      if (consuming) consumedBy + (targetId -> nodeId)
                      else consumedBy,
                    roots = BackStack.empty,
                    keys = mbKey match {
                      case None => keys
                      case Some(key) =>
                        if (consuming) {
                          keys + (GlobalKey(templateId, key) -> None)
                        } else keys
                    },
                  )
            }
          ))
      }
    }

    def endExercises(value: Value[ContractId]): (Option[NodeId], PartialTransaction) = {
      context match {
        case ContextRoot =>
          (None, noteAbort(EndExerciseInRootContext))
        case ContextExercises(ec) =>
          val exercisesChildren = roots.toImmArray
          val exerciseNode = NodeExercises(
            ec.targetId,
            ec.templateId,
            ec.choiceId,
            ec.optLocation,
            ec.consuming,
            ec.actingParties,
            ec.chosenValue,
            ec.stakeholders,
            ec.signatories,
            ec.controllers,
            exercisesChildren,
            Some(value)
          )
          val nodeId = ec.exercisesNodeId
          val ptx =
            copy(
              context = ec.parentContext,
              roots = ec.parentRoots
            ).insertNode(ec.exercisesNodeId, exerciseNode)
          (Some(nodeId), ptx)
      }
    }

    /** Note that the transaction building failed due to the given error */
    def noteAbort(err: TransactionError): PartialTransaction =
      copy(aborted = Some(err))

    /** `True` iff the given `ContractId` has been consumed already */
    def isConsumed(coid: ContractId): Boolean = consumedBy.contains(coid)

    /** Guard the execution of a step with the unconsumedness of a
      * `ContractId`
      */
    def mustBeActive(
        coid: ContractId,
        templateId: TypeConName,
        f: => PartialTransaction): PartialTransaction =
      consumedBy.get(coid) match {
        case None => f
        case Some(nid) =>
          noteAbort(ContractNotActive(coid, templateId, nid))
      }

    /** Allocate a fresh `NodeId` */
    def withFreshNodeId[A](f: ((NodeId, PartialTransaction)) => A): A =
      f((this.nextNodeId, this.copy(nextNodeId.next)))

    /** Insert the give `Node` under the given `NodeId` */
    def insertNode(i: NodeId, n: Node): PartialTransaction =
      copy(
        roots = roots :+ i,
        nodes = nodes + (i -> n)
      )

    /** Insert the given `Node` under a fresh node-id, and return it */
    def insertFreshNode(
        n: NodeId => Node,
        optConsumedBy: Option[ContractId]): (NodeId, PartialTransaction) =
      withFreshNodeId {
        case (nodeId, ptx) =>
          val ptx2 = ptx.insertNode(nodeId, n(nodeId))
          (
            nodeId,
            optConsumedBy
              .map(coid => ptx2.copy(consumedBy = ptx2.consumedBy + (coid -> nodeId)))
              .getOrElse(ptx2))
      }
  }

  object PartialTransaction {

    /** The initial transaction from which we start building. It does not
      *  contain any nodes and is not marked as aborted.
      */
    def initial = PartialTransaction(
      nextNodeId = NodeId.first,
      nodes = Map.empty,
      roots = BackStack.empty,
      consumedBy = Map.empty,
      context = ContextRoot,
      aborted = None,
      keys = Map.empty,
      usedPackages = Set.empty
    )
  }

  // ---------------
  // pure helpers
  // ---------------
  def nodeIdToContractId(nodeId: NodeId) = RelativeContractId(nodeId)
}
