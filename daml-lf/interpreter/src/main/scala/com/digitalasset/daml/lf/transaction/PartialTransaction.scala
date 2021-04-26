// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref.{ChoiceName, Location, Party, TypeConName}
import com.daml.lf.data.{BackStack, ImmArray, Ref, Time}
import com.daml.lf.language.Ast
import com.daml.lf.ledger.{Authorize, FailedAuthorization}
import com.daml.lf.transaction.{
  GenTransaction,
  GlobalKey,
  Node,
  NodeId,
  SubmittedTransaction,
  Transaction => Tx,
  TransactionVersion => TxVersion,
}
import com.daml.lf.value.Value

import scala.annotation.nowarn
import scala.collection.immutable.HashMap

private[lf] object PartialTransaction {

  type NodeIdx = Value.NodeIdx
  type Node = Node.GenNode[NodeId, Value.ContractId]
  type LeafNode = Node.LeafOnlyActionNode[Value.ContractId]

  sealed abstract class ContextInfo {
    val actionChildSeed: Int => crypto.Hash
  }

  sealed abstract class RootContextInfo extends ContextInfo

  private[PartialTransaction] final class SeededTransactionRootContext(seed: crypto.Hash)
      extends RootContextInfo {
    val actionChildSeed = crypto.Hash.deriveNodeSeed(seed, _)
  }

  private[PartialTransaction] final class SeededPartialTransactionRootContext(
      seeds: ImmArray[Option[crypto.Hash]]
  ) extends RootContextInfo {
    override val actionChildSeed: Int => crypto.Hash = { idx =>
      seeds.get(idx) match {
        case Some(Some(value)) =>
          value
        case _ =>
          throw new RuntimeException(s"seed for ${idx}th root node not provided")
      }
    }
  }

  private[PartialTransaction] object NoneSeededTransactionRootContext extends RootContextInfo {
    val actionChildSeed: Any => Nothing = { _ =>
      throw new IllegalStateException(s"the machine is not configure to create transaction")
    }
  }

  /** Contexts of the transaction graph builder, which we use to record
    * the sub-transaction structure due to 'exercises' statements.
    */
  final case class Context(
      info: ContextInfo,
      children: BackStack[NodeId],
      nextActionChildIdx: Int,
  ) {
    def addActionChild(child: NodeId): Context =
      Context(info, children :+ child, nextActionChildIdx + 1)
    def addRollbackChild(child: NodeId, nextActionChildIdx: Int): Context =
      Context(info, children :+ child, nextActionChildIdx)
    // This function may be costly, it must be call at most once for each node.
    def nextActionChildSeed: crypto.Hash = info.actionChildSeed(nextActionChildIdx)
  }

  object Context {

    def apply(initialSeeds: InitialSeeding): Context =
      initialSeeds match {
        case InitialSeeding.TransactionSeed(seed) =>
          Context(new SeededTransactionRootContext(seed), BackStack.empty, 0)
        case InitialSeeding.RootNodeSeeds(seeds) =>
          Context(new SeededPartialTransactionRootContext(seeds), BackStack.empty, 0)
        case InitialSeeding.NoSeed =>
          Context(NoneSeededTransactionRootContext, BackStack.empty, 0)
      }
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
    *  @param nodeId The node to be inserted once we've
    *                         finished this sub-transaction.
    *  @param parent The context in which the exercises is
    *                       happening.
    *  @param byKey True if the exercise is done "by key"
    */
  final case class ExercisesContextInfo(
      targetId: Value.ContractId,
      templateId: TypeConName,
      contractKey: Option[Node.KeyWithMaintainers[Value[Nothing]]],
      choiceId: ChoiceName,
      optLocation: Option[Location],
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Value[Value.ContractId],
      signatories: Set[Party],
      stakeholders: Set[Party],
      choiceObservers: Set[Party],
      nodeId: NodeId,
      parent: Context,
      byKey: Boolean,
  ) extends ContextInfo {
    val actionNodeSeed = parent.nextActionChildSeed
    val actionChildSeed = crypto.Hash.deriveNodeSeed(actionNodeSeed, _)
  }

  final case class ActiveLedgerState(
      consumedBy: Map[Value.ContractId, NodeId],
      keys: Map[GlobalKey, Option[Value.ContractId]],
  )

  final case class TryContextInfo(
      nodeId: NodeId,
      parent: Context,
      // beginState stores the consumed contracts at the beginning of
      // the try so that we can restore them on rollback.
      beginState: ActiveLedgerState,
  ) extends ContextInfo {
    val actionChildSeed: NodeIdx => crypto.Hash = parent.info.actionChildSeed
  }

  def initial(
      pkg2TxVersion: Ref.PackageId => TxVersion,
      submissionTime: Time.Timestamp,
      initialSeeds: InitialSeeding,
  ) = PartialTransaction(
    pkg2TxVersion,
    submissionTime = submissionTime,
    nextNodeIdx = 0,
    nodes = HashMap.empty,
    actionNodeSeeds = BackStack.empty,
    consumedBy = Map.empty,
    context = Context(initialSeeds),
    aborted = None,
    keys = Map.empty,
    localContracts = Set.empty,
  )

  sealed abstract class Result extends Product with Serializable
  final case class CompleteTransaction(tx: SubmittedTransaction) extends Result
  final case class IncompleteTransaction(ptx: PartialTransaction) extends Result

}

/** A transaction under construction
  *
  *  @param nodes The nodes of the transaction graph being built up.
  *  @param actionNodeSeeds The seeds of Create and Exercise nodes
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
  *              info both about relative and contract ids. We must
  *              do this because contract ids can be archived as
  *              part of execution, and we must record these archivals locally.
  *              Note: it is important for keys that we know to not be present
  *              to be present as [[None]]. The reason for this is that we must
  *              record the "no key" information for contract ids that
  *              we archive. This is not an optimization and is required for
  *              correct semantics, since otherwise lookups for keys for
  *              locally archived contract ids will succeed wrongly.
  */
private[lf] case class PartialTransaction(
    packageToTransactionVersion: Ref.PackageId => TxVersion,
    submissionTime: Time.Timestamp,
    nextNodeIdx: Int,
    nodes: HashMap[NodeId, PartialTransaction.Node],
    actionNodeSeeds: BackStack[(NodeId, crypto.Hash)],
    consumedBy: Map[Value.ContractId, NodeId],
    context: PartialTransaction.Context,
    aborted: Option[Tx.TransactionError],
    keys: Map[GlobalKey, Option[Value.ContractId]],
    localContracts: Set[Value.ContractId],
) {

  import PartialTransaction._

  private def activeState: ActiveLedgerState =
    ActiveLedgerState(consumedBy, keys)

  private def resetActiveState(state: ActiveLedgerState): PartialTransaction =
    copy(consumedBy = state.consumedBy, keys = state.keys)

  def nodesToString: String =
    if (nodes.isEmpty) "<empty transaction>"
    else {
      val sb = new StringBuilder()

      def addToStringBuilder(
          nid: NodeId,
          node: Node,
          rootPrefix: String,
      ): Unit = {
        sb.append(rootPrefix)
          .append("node ")
          .append(nid)
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
        val allChildNodeIds: Set[NodeId] = nodes.values.iterator.flatMap {
          case rb: Node.NodeRollback[NodeId] => rb.children.toSeq
          case _: Node.LeafOnlyActionNode[_] => Nil
          case ex: Node.NodeExercises[NodeId, _] => ex.children.toSeq
        }.toSet

        nodes.keySet diff allChildNodeIds
      }
      val tx = GenTransaction(nodes, ImmArray(rootNodes))

      tx.foreach { (nid, node) =>
        val rootPrefix = if (rootNodes.contains(nid)) "root " else ""
        addToStringBuilder(nid, node, rootPrefix)
      }
      removeTrailingComma()

      sb.toString
    }

  /** Finish building a transaction; i.e., try to extract a complete
    *  transaction from the given 'PartialTransaction'. This returns:
    * - a SubmittedTransaction in case of success ;
    * - the 'PartialTransaction' itself if it is not yet complete or
    *   has been aborted ;
    * - an error in case the transaction cannot be serialized using
    *   the `outputTransactionVersions`.
    */
  def finish: PartialTransaction.Result =
    context.info match {
      case _: RootContextInfo if aborted.isEmpty =>
        CompleteTransaction(
          SubmittedTransaction(
            TxVersion.asVersionedTransaction(context.children.toImmArray, nodes)
          )
        )
      case _ =>
        IncompleteTransaction(this)
    }

  /** Extend the 'PartialTransaction' with a node for creating a
    * contract instance.
    */
  def insertCreate(
      auth: Authorize,
      templateId: Ref.Identifier,
      arg: Value[Value.ContractId],
      agreementText: String,
      optLocation: Option[Location],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[Node.KeyWithMaintainers[Value[Nothing]]],
  ): Either[String, (Value.ContractId, PartialTransaction)] = {
    val serializableErrs = serializable(arg)
    if (serializableErrs.nonEmpty) {
      Left(
        s"""Trying to create a contract with a non-serializable value: ${serializableErrs.iterator
          .mkString(",")}"""
      )
    } else {
      val actionNodeSeed = context.nextActionChildSeed
      val discriminator =
        crypto.Hash.deriveContractDiscriminator(actionNodeSeed, submissionTime, stakeholders)
      val cid = Value.ContractId.V1(discriminator)
      val createNode = Node.NodeCreate(
        cid,
        templateId,
        arg,
        agreementText,
        optLocation,
        signatories,
        stakeholders,
        key,
        packageToTransactionVersion(templateId.packageId),
      )
      val nid = NodeId(nextNodeIdx)
      val ptx = copy(
        nextNodeIdx = nextNodeIdx + 1,
        context = context.addActionChild(nid),
        nodes = nodes.updated(nid, createNode),
        actionNodeSeeds = actionNodeSeeds :+ (nid -> actionNodeSeed),
        localContracts = localContracts + cid,
      ).noteAuthFails(nid, CheckAuthorization.authorizeCreate(createNode), auth)

      // if we have a contract key being added, include it in the list of
      // active keys
      key match {
        case None => Right((cid, ptx))
        case Some(kWithM) =>
          val ck = GlobalKey(templateId, kWithM.key)
          Right((cid, ptx.copy(keys = ptx.keys.updated(ck, Some(cid)))))
      }
    }
  }

  private[this] def serializable(a: Value[Value.ContractId]): ImmArray[String] = a.serializable()

  def insertFetch(
      auth: Authorize,
      coid: Value.ContractId,
      templateId: TypeConName,
      optLocation: Option[Location],
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[Node.KeyWithMaintainers[Value[Nothing]]],
      byKey: Boolean,
  ): PartialTransaction = {
    val nid = NodeId(nextNodeIdx)
    val node = Node.NodeFetch(
      coid,
      templateId,
      optLocation,
      actingParties,
      signatories,
      stakeholders,
      key,
      byKey,
      packageToTransactionVersion(templateId.packageId),
    )
    mustBeActive(
      coid,
      templateId,
      insertLeafNode(node),
    ).noteAuthFails(nid, CheckAuthorization.authorizeFetch(node), auth)
  }

  def insertLookup(
      auth: Authorize,
      templateId: TypeConName,
      optLocation: Option[Location],
      key: Node.KeyWithMaintainers[Value[Nothing]],
      result: Option[Value.ContractId],
  ): PartialTransaction = {
    val nid = NodeId(nextNodeIdx)
    val node = Node.NodeLookupByKey(
      templateId,
      optLocation,
      key,
      result,
      packageToTransactionVersion(templateId.packageId),
    )
    insertLeafNode(node)
      .noteAuthFails(nid, CheckAuthorization.authorizeLookupByKey(node), auth)
  }

  /** Open an exercises context.
    * Must be closed by a `endExercises` or an `abortExercise`.
    */
  def beginExercises(
      auth: Authorize,
      targetId: Value.ContractId,
      templateId: TypeConName,
      choiceId: ChoiceName,
      optLocation: Option[Location],
      consuming: Boolean,
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      choiceObservers: Set[Party],
      mbKey: Option[Node.KeyWithMaintainers[Value[Nothing]]],
      byKey: Boolean,
      chosenValue: Value[Value.ContractId],
  ): Either[String, PartialTransaction] = {
    val serializableErrs = serializable(chosenValue)
    if (serializableErrs.nonEmpty) {
      Left(
        s"""Trying to exercise a choice with a non-serializable value: ${serializableErrs.iterator
          .mkString(",")}"""
      )
    } else {
      val nid = NodeId(nextNodeIdx)
      val ec =
        ExercisesContextInfo(
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
          choiceObservers = choiceObservers,
          nodeId = nid,
          parent = context,
          byKey = byKey,
        )

      Right(
        mustBeActive(
          targetId,
          templateId,
          copy(
            nextNodeIdx = nextNodeIdx + 1,
            context = Context(ec, BackStack.empty, 0),
            // important: the semantics of DAML dictate that contracts are immediately
            // inactive as soon as you exercise it. therefore, mark it as consumed now.
            consumedBy = if (consuming) consumedBy.updated(targetId, nid) else consumedBy,
            keys = mbKey match {
              case Some(kWithM) if consuming =>
                keys.updated(GlobalKey(templateId, kWithM.key), None)
              case _ => keys
            },
          ),
        ).noteAuthFails(nid, CheckAuthorization.authorizeExercise(ec), auth)
      )
    }
  }

  /** Close normally an exercise context.
    * Must match a `beginExercises`.
    */
  def endExercises(value: Value[Value.ContractId]): PartialTransaction =
    context.info match {
      case ec: ExercisesContextInfo =>
        val exerciseNode = Node.NodeExercises(
          targetCoid = ec.targetId,
          templateId = ec.templateId,
          choiceId = ec.choiceId,
          optLocation = ec.optLocation,
          consuming = ec.consuming,
          actingParties = ec.actingParties,
          chosenValue = ec.chosenValue,
          stakeholders = ec.stakeholders,
          signatories = ec.signatories,
          choiceObservers = ec.choiceObservers,
          children = context.children.toImmArray,
          exerciseResult = Some(value),
          key = ec.contractKey,
          byKey = ec.byKey,
          version = packageToTransactionVersion(ec.templateId.packageId),
        )
        val nodeId = ec.nodeId
        copy(
          context = ec.parent.addActionChild(nodeId),
          nodes = nodes.updated(nodeId, exerciseNode),
          actionNodeSeeds = actionNodeSeeds :+ (nodeId -> ec.actionNodeSeed),
        )
      case _ =>
        noteAbort(Tx.NonExerciseContext)
    }

  /** Close a abruptly an exercise context du to an uncaught exception.
    * Must match a `beginExercises`.
    */
  def abortExercises: PartialTransaction =
    context.info match {
      case ec: ExercisesContextInfo =>
        val exerciseNode = Node.NodeExercises(
          targetCoid = ec.targetId,
          templateId = ec.templateId,
          choiceId = ec.choiceId,
          optLocation = ec.optLocation,
          consuming = ec.consuming,
          actingParties = ec.actingParties,
          chosenValue = ec.chosenValue,
          stakeholders = ec.stakeholders,
          signatories = ec.signatories,
          choiceObservers = ec.choiceObservers,
          children = context.children.toImmArray,
          exerciseResult = None,
          key = ec.contractKey,
          byKey = ec.byKey,
          version = packageToTransactionVersion(ec.templateId.packageId),
        )
        val nodeId = ec.nodeId
        val actionNodeSeed = context.nextActionChildSeed
        copy(
          context = ec.parent.addActionChild(nodeId),
          nodes = nodes.updated(nodeId, exerciseNode),
          actionNodeSeeds = actionNodeSeeds :+ (nodeId -> actionNodeSeed),
        )
      case _ =>
        noteAbort(Tx.NonExerciseContext)
    }

  /** Open a Try context.
    *  Must be closed by `endTry`, `abortTry`, or `rollbackTry`.
    */
  def beginTry: PartialTransaction = {
    val nid = NodeId(nextNodeIdx)
    val info = TryContextInfo(nid, context, activeState)
    copy(
      nextNodeIdx = nextNodeIdx + 1,
      context = Context(info, BackStack.empty, context.nextActionChildIdx),
    )
  }

  /** Close a try context normally , i.e. no exception occurred.
    * Must match a `beginTry`.
    */
  def endTry: PartialTransaction =
    context.info match {
      case info: TryContextInfo =>
        copy(
          context = info.parent.copy(
            children = info.parent.children :++ context.children.toImmArray,
            nextActionChildIdx = context.nextActionChildIdx,
          )
        )
      case _ =>
        noteAbort(Tx.NonCatchContext)
    }

  /** Close abruptly a try context, due to an uncaught exception,
    * i.e. a exception was thrown inside the context but the catch associated to the try context did not handle it.
    * Must match a `beginTry`.
    */
  def abortTry: PartialTransaction =
    endTry

  /** Close a try context, by catching an exception,
    * i.e. a exception was thrown inside the context, and the catch associated to the try context did handle it.
    */
  @nowarn("msg=parameter value (exceptionType|exception) in method rollbackTry is never used")
  def rollbackTry(
      exceptionType: Ast.Type,
      exception: Value[Value.ContractId],
  ): PartialTransaction =
    context.info match {
      case info: TryContextInfo =>
        // TODO https://github.com/digital-asset/daml/issues/8020
        //  the version of a rollback node should be determined from its children.
        //  in the case of there being no children we can simple drop the entire rollback node.
        val rollbackNode = Node.NodeRollback(context.children.toImmArray, TxVersion.VDev)
        copy(
          context = info.parent.addRollbackChild(info.nodeId, context.nextActionChildIdx),
          nodes = nodes.updated(info.nodeId, rollbackNode),
        ).resetActiveState(info.beginState)
      case _ =>
        noteAbort(Tx.NonCatchContext)
    }

  /** Note that the transaction building failed due to an authorization failure */
  private def noteAuthFails(
      nid: NodeId,
      f: Authorize => List[FailedAuthorization],
      auth: Authorize,
  ): PartialTransaction = {
    f(auth) match {
      case Nil => this
      case fa :: _ => // take just the first failure //TODO: dont compute all!
        noteAbort(Tx.AuthFailureDuringExecution(nid, fa))
    }
  }

  /** Note that the transaction building failed due to the given error */
  private[this] def noteAbort(err: Tx.TransactionError): PartialTransaction =
    copy(aborted = Some(err))

  /** `True` iff the given `ContractId` has been consumed already */
  def isConsumed(coid: Value.ContractId): Boolean = consumedBy.contains(coid)

  /** Guard the execution of a step with the unconsumedness of a
    * `ContractId`
    */
  private[this] def mustBeActive(
      coid: Value.ContractId,
      templateId: TypeConName,
      f: => PartialTransaction,
  ): PartialTransaction =
    consumedBy.get(coid) match {
      case None => f
      case Some(nid) => noteAbort(Tx.ContractNotActive(coid, templateId, nid))
    }

  /** Insert the given `LeafNode` under a fresh node-id, and return it */
  def insertLeafNode(node: LeafNode): PartialTransaction = {
    val nid = NodeId(nextNodeIdx)
    copy(
      nextNodeIdx = nextNodeIdx + 1,
      context = context.addActionChild(nid),
      nodes = nodes.updated(nid, node),
    )
  }

}

private[lf] sealed abstract class InitialSeeding extends Product with Serializable

private[lf] object InitialSeeding {
  // NoSeed may be used to initialize machines that are not intended to create transactions
  // e.g. trigger and script runners, tests
  final case object NoSeed extends InitialSeeding
  final case class TransactionSeed(seed: crypto.Hash) extends InitialSeeding
  final case class RootNodeSeeds(seeds: ImmArray[Option[crypto.Hash]]) extends InitialSeeding
}
