// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref.{ChoiceName, Location, Party, TypeConName}
import com.daml.lf.data.{BackStack, ImmArray, Ref, Time}
import com.daml.lf.language.Ast
import com.daml.lf.ledger.{Authorize, FailedAuthorization}
import com.daml.lf.transaction.{
  ContractKeyUniquenessMode,
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

  sealed trait KeyConflict extends Product with Serializable
  object KeyConflict {
    final case object None extends KeyConflict
    final case object Duplicate extends KeyConflict
  }

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
      keys: Map[GlobalKey, KeyMapping],
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
      contractKeyUniqueness: ContractKeyUniquenessMode,
      submissionTime: Time.Timestamp,
      initialSeeds: InitialSeeding,
  ) = PartialTransaction(
    pkg2TxVersion,
    contractKeyUniqueness = contractKeyUniqueness,
    submissionTime = submissionTime,
    nextNodeIdx = 0,
    nodes = HashMap.empty,
    actionNodeSeeds = BackStack.empty,
    consumedBy = Map.empty,
    context = Context(initialSeeds),
    aborted = None,
    keys = Map.empty,
    globalKeyInputs = Map.empty,
    localContracts = Set.empty,
  )

  sealed abstract class Result extends Product with Serializable
  final case class CompleteTransaction(tx: SubmittedTransaction) extends Result
  final case class IncompleteTransaction(ptx: PartialTransaction) extends Result

  sealed abstract class KeyMapping extends Product with Serializable
  // There is no active contract with the given key.
  final case object KeyInactive extends KeyMapping
  // The contract with the given cid is active and has the given key.
  final case class KeyActive(cid: Value.ContractId) extends KeyMapping
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
  *  @param keys A local store of the contract keys used for lookups and fetches by keys
  *              (including exercise by key). Each of those operations will be resolved
  *              against this map first. Only if there is no entry in here
  *              (but not if there is an entry mapped to None), will we ask the ledger.
  *
  *              This map is mutated by the following operations:
  *              1. fetch-by-key/lookup-by-key/exercise-by-key will insert an
  *                 an entry in the map if there wasn’t already one (i.e., if they queried the ledger).
  *              2. ACS mutating operations if the corresponding contract has a key. Specifically,
  *                 2.1. A create will set the corresponding map entry to KeyActive(cid) if the contract has a key.
  *                 2.2. A consuming choice on cid will set the corresponding map entry to KeyInactive
  *                      iff we had a KeyActive(cid) entry for the same key before. If not, keys
  *                      will not be modified. Later lookups have an activeness check
  *                      that can then set this to KeyInactive if the result of the
  *                      lookup was already archived.
  *
  *              On a rollback, we restore the state at the beginning of the rollback. However,
  *              we preserve globalKeyInputs so we will not ask the ledger again for a key lookup
  *              that we saw in a rollback.
  *
  *              Note that the engine is also used in Canton’s non-uck (unique contract key) mode.
  *              In that mode, duplicate keys should not be an error. We provide no stability
  *              guarantees for this mode at this point so tests can be changed freely.
  *  @param globalKeyInputs A store of fetches and lookups of global keys.
  *   Note that this represents the required state at the beginning of the transaction, i.e., the
  *   transaction inputs.
  *   The contract might no longer be active or a new local contract with the
  *   same key might have been created since. This is updated on creates with keys with KeyInactive
  *   (implying that no key must have been active at the beginning of the transaction)
  *   and on failing and successful lookup and fetch by key.
  */
private[lf] case class PartialTransaction(
    packageToTransactionVersion: Ref.PackageId => TxVersion,
    contractKeyUniqueness: ContractKeyUniquenessMode,
    submissionTime: Time.Timestamp,
    nextNodeIdx: Int,
    nodes: HashMap[NodeId, PartialTransaction.Node],
    actionNodeSeeds: BackStack[(NodeId, crypto.Hash)],
    consumedBy: Map[Value.ContractId, NodeId],
    context: PartialTransaction.Context,
    aborted: Option[Tx.TransactionError],
    keys: Map[GlobalKey, PartialTransaction.KeyMapping],
    globalKeyInputs: Map[GlobalKey, PartialTransaction.KeyMapping],
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

          // Note (MK) Duplicate key checks in Speedy
          // When run in ContractKeyUniquenessMode.On speedy detects duplicate contract keys errors.
          //
          // Just like for modifying `keys` and `globalKeyInputs` we only consider
          // by-key operations, i.e., lookup, exercise and fetch by key as well as creates
          // and archives if the key has been brought into scope before.
          //
          // In the end, those checks mean that ledgers only have to look at inputs and outputs
          // of the transaction and check for conflicts on that while speedy checks for internal
          // conflicts.
          //
          // We have to consider the following cases for conflicts:
          // 1. Create of a new local contract
          //    1.1. KeyInactive in `keys`. This means we saw an archive so the create is valid.
          //    1.2. KeyActive(_) in `keys`. This can either be local contract or a global contract. Both are an error.
          //    1.3. No entry in `keys` and no entry in `globalKeyInputs`. This is valid. Note that the ledger here will then
          //         have to check when committing that there is no active contract with this key before the transaction.
          //    1.4. No entry in `keys` and `KeyInactive` in `globalKeyInputs`. This is valid. Ledgers need the same check
          //         as for 1.3.
          //    1.5. No entry in `keys` and `KeyActive(_)` in `globalKeyInputs`. This is an error. Note that the case where
          //         the global contract has already been archived falls under 1.2.
          // 2. Global key lookups
          //    2.1. Conflicts with other global contracts cannot arise as we query a key at most once.
          //    2.2. Conflicts with local contracts also cannot arise: A successful create will either
          //         2.2.1: Set `globalKeyInputs` to `KeyInactive`.
          //         2.2.2: Not modify `globalKeyInputs` if there already was an entry.
          //         For both of those cases `globalKeyInputs` already had an entry which means
          //         we would use that as a cached result and not query the ledger.
          val conflict = keys.get(ck).orElse(ptx.globalKeyInputs.get(ck)) match {
            case Some(KeyActive(_)) => KeyConflict.Duplicate
            case Some(KeyInactive) | None => KeyConflict.None
          }
          val globalKeyInputs = keys.get(ck).orElse(ptx.globalKeyInputs.get(ck)) match {
            case None => ptx.globalKeyInputs.updated(ck, KeyInactive)
            case Some(_) => ptx.globalKeyInputs
          }
          (conflict, contractKeyUniqueness) match {
            case (KeyConflict.Duplicate, ContractKeyUniquenessMode.On) =>
              Right((cid, ptx.noteAbort(Tx.DuplicateContractKey(ck))))
            case _ =>
              Right(
                (
                  cid,
                  ptx.copy(
                    keys = ptx.keys.updated(ck, KeyActive(cid)),
                    globalKeyInputs = globalKeyInputs,
                  ),
                )
              )
          }
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
                val gkey = GlobalKey(templateId, kWithM.key)
                keys.get(gkey).orElse(globalKeyInputs.get(gkey)) match {
                  // An archive can only mark a key as inactive
                  // if it was brought into scope before.
                  case Some(KeyActive(cid)) if cid == targetId =>
                    keys.updated(gkey, KeyInactive)
                  // If the key was not in scope or mapped to a different cid, we don’t change keys. Instead we will do
                  // an activeness check when looking it up later.
                  case _ => keys
                }
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
  private def noteAbort(err: Tx.TransactionError): PartialTransaction =
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
