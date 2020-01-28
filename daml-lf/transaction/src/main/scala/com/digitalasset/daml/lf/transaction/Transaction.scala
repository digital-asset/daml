// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package transaction

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.Node._
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value._
import scalaz.Equal

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.breakOut
import scala.collection.immutable.HashMap

case class VersionedTransaction[Nid, Cid](
    version: TransactionVersion,
    transaction: GenTransaction.WithTxValue[Nid, Cid],
) {

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
  * @param optUsedPackages The set of packages used during command processing.
  *                     This is a hint for what packages are required to validate
  *                     the transaction using the current interpreter.
  *                     The used packages are not serialized using [[TransactionCoder]].
  * @param transactionSeed master hash used to derived node and relative contractId
  *                        discriminators. If it is undefined, the discriminators have not be
  *                        generated and have be let undefined in the nodes and the relative
  *                        contractIds of the transaction.
  *
  * Users of this class may assume that all instances are well-formed, i.e., `isWellFormed.isEmpty`.
  * For performance reasons, users are not required to call `isWellFormed`.
  * Therefore, it is '''forbidden''' to create ill-formed instances, i.e., instances with `!isWellFormed.isEmpty`.
  */
case class GenTransaction[Nid, Cid, +Val](
    nodes: HashMap[Nid, GenNode[Nid, Cid, Val]],
    roots: ImmArray[Nid],
    optUsedPackages: Option[Set[PackageId]],
    transactionSeed: Option[crypto.Hash] = None,
) {

  import GenTransaction._

  def mapContractIdAndValue[Cid2, Val2](
      f: Cid => Cid2,
      g: Val => Val2,
  ): GenTransaction[Nid, Cid2, Val2] =
    copy(
      nodes = // do NOT use `Map#mapValues`! it applies the function lazily on lookup. see #1861
        nodes.transform((_, value) => value.mapContractIdAndValue(f, g)),
    )

  def mapContractId[Cid2](f: Cid => Cid2)(
      implicit ev: Val <:< VersionedValue[Cid],
  ): WithTxValue[Nid, Cid2] =
    mapContractIdAndValue(f, _.mapContractId(f))

  /** Note: the provided function must be injective, otherwise the transaction will be corrupted. */
  def mapNodeId[Nid2](f: Nid => Nid2): GenTransaction[Nid2, Cid, Val] =
    copy(
      roots = roots.map(f),
      nodes = nodes.map { case (nid, node) => (f(nid), node.mapNodeId(f)) },
    )

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
  final def isReplayedBy[Nid2, Val2 >: Val](
      other: GenTransaction[Nid2, Cid, Val2],
  )(implicit ECid: Equal[Cid], EVal: Equal[Val2]): Boolean =
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

object GenTransaction {
  type WithTxValue[Nid, Cid] = GenTransaction[Nid, Cid, Transaction.Value[Cid]]

  case class NotWellFormedError[Nid](nid: Nid, reason: NotWellFormedErrorReason)
  sealed trait NotWellFormedErrorReason
  case object DanglingNodeId extends NotWellFormedErrorReason
  case object OrphanedNode extends NotWellFormedErrorReason
  case object AliasedNode extends NotWellFormedErrorReason
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

  object PartialTransaction {

    type NodeIdx = Value.NodeIdx

    /** Contexts of the transaction graph builder, which we use to record
      * the sub-transaction structure due to 'exercises' statements.
      */
    sealed abstract class Context extends Product with Serializable {
      def discriminator: Option[crypto.Hash]

      def children: BackStack[NodeId]

      def addChild(child: NodeId): Context
    }

    /** The root context, which is what we use when we are not exercising
      * a choice.
      */
    final case class ContextRoot(
        override val discriminator: Option[crypto.Hash],
        children: BackStack[NodeId] = BackStack.empty,
    ) extends Context {
      override def addChild(child: NodeId): ContextRoot = copy(children = children :+ child)
    }

    /** Context when creating a sub-transaction due to an exercises. */
    final case class ContextExercise(
        ctx: ExercisesContext,
        children: BackStack[NodeId] = BackStack.empty,
    ) extends Context {
      override def addChild(child: NodeId): ContextExercise =
        copy(children = children :+ child)

      override def discriminator: Option[crypto.Hash] = ctx.nodeId.discriminator
    }

    /** Context information to remember when building a sub-transaction
      *  due to an 'exercises' statement.
      *
      *  @param targetId Contract-id referencing the contract-instance on
      *                  which we are exercising a choice.
      *  @param templateId Template-id referencing the template of the
      *                    contract on which we are exercising a choice.
      *  @param contractKey Optional contract key, if defined for the
      *                     contract on which we are exercising a choice.
      *  @param choiceId Label of the choice that we are exercising.
      *  @param consuming True if the choice consumes the contract.
      *  @param actingParties The parties exercising the choice.
      *  @param chosenValue The chosen value.
      *  @param signatories The signatories of the contract.
      *  @param stakeholders The stakeholders of the contract.
      *  @param controllers The controllers of the choice.
      *  @param nodeId The node to be inserted once we've
      *                         finished this sub-transaction.
      *  @param parent The context in which the exercises is
      *                       happening.
      */
    case class ExercisesContext(
        targetId: TContractId,
        templateId: TypeConName,
        contractKey: Option[KeyWithMaintainers[Value[Nothing]]],
        choiceId: ChoiceName,
        optLocation: Option[Location],
        consuming: Boolean,
        actingParties: Set[Party],
        chosenValue: Value[TContractId],
        signatories: Set[Party],
        stakeholders: Set[Party],
        controllers: Set[Party],
        nodeId: NodeId,
        parent: Context,
    )

    /** The initial transaction from which we start building. It does not
      *  contain any nodes and is not marked as aborted.
      *  @param transactionSeed is the master hash used to derive nodes and
      *                         contractIds discriminator. If let undefined no
      *                         discriminators should be generate.
      */
    def initial(transactionSeed: Option[crypto.Hash]) = PartialTransaction(
      nextNodeIdx = 0,
      nodes = HashMap.empty,
      consumedBy = Map.empty,
      context = ContextRoot(transactionSeed),
      aborted = None,
      keys = Map.empty,
    )

  }

  /** A transaction under construction
    *
    *  @param nodes The nodes of the transaction graph being built up.
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
    */
  case class PartialTransaction(
      nextNodeIdx: Int,
      nodes: HashMap[NodeId, Node],
      consumedBy: Map[TContractId, NodeId],
      context: PartialTransaction.Context,
      aborted: Option[TransactionError],
      keys: Map[GlobalKey, Option[TContractId]],
  ) {

    import PartialTransaction._

    def nodesToString: String =
      if (nodes.isEmpty) "<empty transaction>"
      else {
        val sb = new StringBuilder()

        def addToStringBuilder(
            nid: NodeId,
            node: GenNode.WithTxValue[NodeId, TContractId],
            rootPrefix: String,
        ): Unit = {
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
        val rootNodes = {
          val allChildNodeIds: Set[NodeId] = nodes.values.flatMap {
            case _: LeafOnlyNode[_, _] => Nil
            case ex: NodeExercises[NodeId, _, _] => ex.children.toSeq
          }(breakOut)

          nodes.keySet diff allChildNodeIds
        }
        val tx = GenTransaction(nodes, ImmArray(rootNodes), None)

        tx.foreach { (nid, node) =>
          val rootPrefix = if (rootNodes.contains(nid)) "root " else ""
          addToStringBuilder(nid, node, rootPrefix)
        }
        removeTrailingComma()

        sb.toString
      }

    /** Finish building a transaction; i.e., try to extract a complete
      *  transaction from the given 'PartialTransaction'. This fails if
      *  the 'PartialTransaction' is not yet complete or has been
      *  aborted.
      */
    def finish: Either[PartialTransaction, Transaction] =
      context match {
        case ContextRoot(transactionSeed, children) if aborted.isEmpty =>
          Right(
            GenTransaction(
              nodes = nodes,
              roots = children.toImmArray,
              optUsedPackages = None,
              transactionSeed = transactionSeed,
            ),
          )
        case _ =>
          Left(this)
      }

    /** Lookup the contract associated to 'RelativeContractId'.
      * Return the contract instance and the node in which it was
      * consumed if any.
      */
    def lookupLocalContract(
        lcoid: RelativeContractId,
    ): Option[(ContractInst[Transaction.Value[TContractId]], Option[NodeId])] =
      for {
        node <- nodes.get(lcoid.txnid)
        coinst <- node match {
          case create: NodeCreate.WithTxValue[TContractId] =>
            Some((create.coinst, consumedBy.get(lcoid)))
          case _: NodeExercises[_, _, _] | _: NodeFetch[_] | _: NodeLookupByKey[_, _] => None
        }
      } yield coinst

    /** Extend the 'PartialTransaction' with a node for creating a
      * contract instance.
      */
    def insertCreate(
        coinst: ContractInst[Value[TContractId]],
        optLocation: Option[Location],
        signatories: Set[Party],
        stakeholders: Set[Party],
        key: Option[KeyWithMaintainers[Value[Nothing]]],
    ): Either[String, (TContractId, PartialTransaction)] = {
      val serializableErrs = serializable(coinst.arg)
      if (serializableErrs.nonEmpty) {
        Left(
          s"""Trying to create a contract with a non-serializable value: ${serializableErrs.iterator
            .mkString(",")}""",
        )
      } else {
        val nodeDiscriminator = deriveChildDiscriminator
        val nodeId = NodeId(nextNodeIdx, nodeDiscriminator)
        val contractDiscriminator =
          nodeDiscriminator.map(crypto.Hash.deriveContractDiscriminator(_, stakeholders))
        val cid = RelativeContractId(nodeId, contractDiscriminator)
        val createNode = NodeCreate(
          cid,
          coinst,
          optLocation,
          signatories,
          stakeholders,
          key,
        )
        val ptx = copy(
          nextNodeIdx = nextNodeIdx + 1,
          context = context.addChild(nodeId),
          nodes = nodes.updated(nodeId, createNode),
        )

        // if we have a contract key being added, include it in the list of
        // active keys
        key match {
          case None => Right((cid, ptx))
          case Some(kWithM) =>
            val ck = GlobalKey(coinst.template, kWithM.key)
            Right((cid, ptx.copy(keys = ptx.keys.updated(ck, Some(cid)))))
        }
      }
    }

    def serializable(a: Value[TContractId]): ImmArray[String] = a.value.serializable()

    def insertFetch(
        coid: TContractId,
        templateId: TypeConName,
        optLocation: Option[Location],
        actingParties: Set[Party],
        signatories: Set[Party],
        stakeholders: Set[Party],
    ): PartialTransaction =
      mustBeActive(
        coid,
        templateId,
        insertLeafNode(
          NodeFetch(coid, templateId, optLocation, Some(actingParties), signatories, stakeholders),
        ),
      )

    def insertLookup(
        templateId: TypeConName,
        optLocation: Option[Location],
        key: KeyWithMaintainers[Value[Nothing]],
        result: Option[TContractId],
    ): PartialTransaction =
      insertLeafNode(NodeLookupByKey(templateId, optLocation, key, result))

    def beginExercises(
        targetId: TContractId,
        templateId: TypeConName,
        choiceId: ChoiceName,
        optLocation: Option[Location],
        consuming: Boolean,
        actingParties: Set[Party],
        signatories: Set[Party],
        stakeholders: Set[Party],
        controllers: Set[Party],
        mbKey: Option[KeyWithMaintainers[Value[Nothing]]],
        chosenValue: Value[TContractId],
    ): Either[String, PartialTransaction] = {
      val serializableErrs = serializable(chosenValue)
      if (serializableErrs.nonEmpty) {
        Left(
          s"""Trying to exercise a choice with a non-serializable value: ${serializableErrs.iterator
            .mkString(",")}""",
        )
      } else {
        val nodeId = NodeId(nextNodeIdx, deriveChildDiscriminator)
        Right(
          mustBeActive(
            targetId,
            templateId,
            copy(
              nextNodeIdx = nextNodeIdx + 1,
              context = ContextExercise(
                ExercisesContext(
                  targetId = targetId,
                  templateId = templateId,
                  contractKey = mbKey,
                  choiceId = choiceId,
                  optLocation = optLocation,
                  consuming = consuming,
                  actingParties = actingParties,
                  chosenValue = chosenValue,
                  signatories = signatories,
                  stakeholders = stakeholders,
                  controllers = controllers,
                  nodeId = nodeId,
                  parent = context,
                ),
              ),
              // important: the semantics of DAML dictate that contracts are immediately
              // inactive as soon as you exercise it. therefore, mark it as consumed now.
              consumedBy = if (consuming) consumedBy.updated(targetId, nodeId) else consumedBy,
              keys = mbKey match {
                case Some(kWithM) if consuming =>
                  keys.updated(GlobalKey(templateId, kWithM.key), None)
                case _ => keys
              },
            ),
          ),
        )
      }
    }

    def endExercises(value: Value[TContractId]): PartialTransaction =
      context match {
        case ContextExercise(ec, children) =>
          val exerciseNode: Transaction.Node = NodeExercises(
            targetCoid = ec.targetId,
            templateId = ec.templateId,
            choiceId = ec.choiceId,
            optLocation = ec.optLocation,
            consuming = ec.consuming,
            actingParties = ec.actingParties,
            chosenValue = ec.chosenValue,
            stakeholders = ec.stakeholders,
            signatories = ec.signatories,
            controllers = ec.controllers,
            children = children.toImmArray,
            exerciseResult = Some(value),
            key = ec.contractKey,
          )
          val nodeId = ec.nodeId
          copy(context = ec.parent.addChild(nodeId), nodes = nodes.updated(nodeId, exerciseNode))
        case ContextRoot(_, _) =>
          noteAbort(EndExerciseInRootContext)
      }

    /** Note that the transaction building failed due to the given error */
    def noteAbort(err: TransactionError): PartialTransaction = copy(aborted = Some(err))

    /** `True` iff the given `ContractId` has been consumed already */
    def isConsumed(coid: TContractId): Boolean = consumedBy.contains(coid)

    /** Guard the execution of a step with the unconsumedness of a
      * `ContractId`
      */
    def mustBeActive(
        coid: TContractId,
        templateId: TypeConName,
        f: => PartialTransaction,
    ): PartialTransaction =
      consumedBy.get(coid) match {
        case None => f
        case Some(nid) => noteAbort(ContractNotActive(coid, templateId, nid))
      }

    /** Insert the given `LeafNode` under a fresh node-id, and return it */
    def insertLeafNode(node: LeafNode): PartialTransaction = {
      val nodeId = NodeId(nextNodeIdx, deriveChildDiscriminator)
      copy(
        nextNodeIdx = nextNodeIdx + 1,
        context = context.addChild(nodeId),
        nodes = nodes.updated(nodeId, node),
      )
    }

    def deriveChildDiscriminator: Option[crypto.Hash] =
      context.discriminator.map(crypto.Hash.deriveNodeDiscriminator(_, nodes.size))

  }

}
