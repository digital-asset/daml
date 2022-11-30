// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref.{ChoiceName, Location, Party, TypeConName}
import com.daml.lf.data.{BackStack, ImmArray, Ref, Time}
import com.daml.lf.ledger.Authorize
import com.daml.lf.transaction.ContractKeyUniquenessMode
import com.daml.lf.transaction.{
  ContractStateMachine,
  GlobalKey,
  Node,
  NodeId,
  SubmittedTransaction => SubmittedTx,
  Transaction => Tx,
  TransactionVersion => TxVersion,
}
import com.daml.lf.value.Value
import com.daml.nameof.NameOf
import com.daml.scalautil.Statement.discard

import scala.collection.immutable.HashMap
import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.tailrec

private[lf] object PartialTransaction {

  sealed abstract class KeyConflict extends Product with Serializable
  object KeyConflict {
    final case object None extends KeyConflict
    final case object Duplicate extends KeyConflict
  }

  import Value.NodeIdx

  sealed abstract class ContextInfo {
    val actionChildSeed: Int => crypto.Hash
    private[PartialTransaction] def authorizers: Set[Party]
  }

  sealed abstract class RootContextInfo extends ContextInfo {
    val committers: Set[Party]
    override val authorizers: Set[Party] = committers
  }

  private[PartialTransaction] final class SeededTransactionRootContext(
      seed: crypto.Hash,
      override val committers: Set[Party],
  ) extends RootContextInfo {
    val actionChildSeed = crypto.Hash.deriveNodeSeed(seed, _)
  }

  private[PartialTransaction] final class SeededPartialTransactionRootContext(
      seeds: ImmArray[Option[crypto.Hash]],
      override val committers: Set[Party],
  ) extends RootContextInfo {
    override val actionChildSeed: Int => crypto.Hash = { idx =>
      seeds.get(idx) match {
        case Some(Some(value)) =>
          value
        case _ =>
          InternalError.runtimeException(
            NameOf.qualifiedNameOfCurrentFunc,
            s"seed for ${idx}th root node not provided",
          )
      }
    }
  }

  private[PartialTransaction] final case class NoneSeededTransactionRootContext(
      override val committers: Set[Party]
  ) extends RootContextInfo {
    val actionChildSeed: Any => Nothing = { _ =>
      InternalError.runtimeException(
        NameOf.qualifiedNameOfCurrentFunc,
        s"the machine is not configure to create transaction",
      )
    }
  }

  /** Contexts of the transaction graph builder, which we use to record
    * the sub-transaction structure due to 'exercises' statements.
    */
  final case class Context(
      info: ContextInfo,
      minChildVersion: TxVersion, // tracks the minimum version of any child within `children`
      children: BackStack[NodeId],
      nextActionChildIdx: Int,
  ) {
    // when we add a child node we must pass the minimum-version contained in that child
    def addActionChild(child: NodeId, version: TxVersion): Context = {
      Context(info, minChildVersion min version, children :+ child, nextActionChildIdx + 1)
    }
    def addRollbackChild(child: NodeId, version: TxVersion, nextActionChildIdx: Int): Context =
      Context(info, minChildVersion min version, children :+ child, nextActionChildIdx)
    // This function may be costly, it must be call at most once for each node.
    def nextActionChildSeed: crypto.Hash = info.actionChildSeed(nextActionChildIdx)
  }

  object Context {

    def apply(info: ContextInfo): Context =
      // An empty context, with no children; minChildVersion is set to the max-int.
      Context(info, TxVersion.VDev, BackStack.empty, 0)

    def apply(initialSeeds: InitialSeeding, committers: Set[Party]): Context =
      initialSeeds match {
        case InitialSeeding.TransactionSeed(seed) =>
          Context(new SeededTransactionRootContext(seed, committers))
        case InitialSeeding.RootNodeSeeds(seeds) =>
          Context(new SeededPartialTransactionRootContext(seeds, committers))
        case InitialSeeding.NoSeed =>
          Context(NoneSeededTransactionRootContext(committers))
      }
  }

  /** Context information to remember when building a sub-transaction
    *  due to an 'exercises' statement.
    *
    *  @param targetId Contract-id referencing the contract-instance on
    *                  which we are exercising a choice.
    *  @param templateId Template-id referencing the template of the
    *                    contract on which we are exercising a choice.
    *  @param interfaceId The interface where the choice is defined if inherited.
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
      interfaceId: Option[TypeConName],
      contractKey: Option[Node.KeyWithMaintainers],
      choiceId: ChoiceName,
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Value,
      signatories: Set[Party],
      stakeholders: Set[Party],
      choiceObservers: Set[Party],
      nodeId: NodeId,
      parent: Context,
      byKey: Boolean,
      version: TxVersion,
  ) extends ContextInfo {
    val actionNodeSeed = parent.nextActionChildSeed
    val actionChildSeed = crypto.Hash.deriveNodeSeed(actionNodeSeed, _)
    override val authorizers: Set[Party] = actingParties union signatories
  }

  final case class TryContextInfo(
      nodeId: NodeId,
      parent: Context,
      // Set to the authorizers (the union of signatories & actors) of the nearest
      // parent exercise or the submitters if there is no parent exercise.
      authorizers: Set[Party],
  ) extends ContextInfo {
    val actionChildSeed: NodeIdx => crypto.Hash = parent.info.actionChildSeed
  }

  def initial(
      contractKeyUniqueness: ContractKeyUniquenessMode,
      initialSeeds: InitialSeeding,
      committers: Set[Party],
      disclosedContracts: ImmArray[DisclosedContract],
      checkAuthorization: Boolean,
  ) = PartialTransaction(
    nextNodeIdx = 0,
    nodes = HashMap.empty,
    actionNodeSeeds = BackStack.empty,
    context = Context(initialSeeds, committers),
    contractState = new ContractStateMachine[NodeId](contractKeyUniqueness).initial,
    actionNodeLocations = BackStack.empty,
    disclosedContracts = disclosedContracts,
    checkAuthorization = checkAuthorization,
  )

  @throws[SError.SErrorDamlException]
  private def assertRightKey[X](either: Either[Tx.InconsistentContractKey, X]): X =
    either match {
      case Right(value) =>
        value
      case Left(Tx.InconsistentContractKey(key)) =>
        throw SError.SErrorDamlException(interpretation.Error.InconsistentContractKey(key))
    }

  type NodeSeeds = ImmArray[(NodeId, crypto.Hash)]
}

/** A transaction under construction
  *
  *  @param nodes The nodes of the transaction graph being built up.
  *  @param actionNodeSeeds The seeds of create and exercise nodes in pre-order. NodeIds are determined by finish.
  *   Note that only other node types do not have seeds and are not included.
  *  @param context The context of what sub-transaction is being
  *                 built.
  *  @param contractState summarizes the changes to the contract states caused by nodes up to now
  *  @param actionNodeLocations The optional locations of create/exercise/fetch/lookup nodes in pre-order.
  *   Used by 'locationInfo()', called by 'finish()' and 'finishIncomplete()'
  *   @param disclosedContracts contracts that have been explicitly disclosed to Speedy (usage will be determined by 'finish()')
  */
private[speedy] case class PartialTransaction(
    nextNodeIdx: Int,
    nodes: HashMap[NodeId, Node],
    actionNodeSeeds: BackStack[crypto.Hash],
    context: PartialTransaction.Context,
    contractState: ContractStateMachine[NodeId]#State,
    actionNodeLocations: BackStack[Option[Location]],
    disclosedContracts: ImmArray[DisclosedContract],
    checkAuthorization: Boolean,
) {

  import PartialTransaction._

  val disclosedContractIds: Set[Value.ContractId] =
    disclosedContracts.map(_.contractId.value).toSeq.toSet

  def consumedByOrInactive(cid: Value.ContractId): Option[Either[NodeId, Unit]] = {
    contractState.consumedByOrInactive(cid)
  }

  def nodesToString: String =
    if (nodes.isEmpty) "<empty transaction>"
    else {
      val sb = new StringBuilder()

      def addToStringBuilder(
          nid: NodeId,
          node: Node,
          rootPrefix: String,
      ): Unit = {
        discard(
          sb.append(rootPrefix)
            .append("node ")
            .append(nid)
            .append(": ")
            .append(node.toString)
            .append(", ")
        )
      }

      def removeTrailingComma(): Unit = {
        if (sb.length >= 2) sb.setLength(sb.length - 2) // remove trailing ", "
      }

      // roots field is not initialized when this method is executed on a failed transaction,
      // so we need to compute them.
      val rootNodes = {
        val allChildNodeIds: Set[NodeId] = nodes.values.iterator.flatMap {
          case rb: Node.Rollback => rb.children.toSeq
          case _: Node.LeafOnlyAction => Nil
          case ex: Node.Exercise => ex.children.toSeq
        }.toSet

        nodes.keySet diff allChildNodeIds
      }
      val tx = Tx(nodes, rootNodes.to(ImmArray))

      tx.foreach { (nid, node) =>
        val rootPrefix = if (rootNodes.contains(nid)) "root " else ""
        addToStringBuilder(nid, node, rootPrefix)
      }
      removeTrailingComma()

      sb.toString
    }

  private[speedy] def locationInfo(): Map[NodeId, Location] = {
    this.actionNodeLocations.toImmArray.toSeq.view.zipWithIndex.collect { case (Some(loc), n) =>
      (NodeId(n), loc)
    }.toMap
  }

  private[this] def normByKey(version: TxVersion, byKey: Boolean): Boolean = {
    if (version < TxVersion.minByKey) {
      false
    } else {
      byKey
    }
  }

  /** Finish building a transaction; i.e., try to extract a complete
    *  transaction from the given 'PartialTransaction'. This returns:
    * - a SubmittedTransaction in case of success ;
    * - the 'PartialTransaction' itself if it is not yet complete or
    *   has been aborted ;
    * - an error in case the transaction cannot be serialized using
    *   the `outputTransactionVersions`.
    */
  private[speedy] def finish: Either[SError.SErrorCrash, (SubmittedTx, ImmArray[NodeId])] =
    context.info match {
      case _: RootContextInfo =>
        val roots = context.children.toImmArray
        val tx0 = Tx(nodes, roots)
        val (tx, seeds) = NormalizeRollbacks.normalizeTx(tx0)
        val txResult = SubmittedTx(TxVersion.asVersionedTransaction(tx))
        Right((txResult, seeds))

      case _ =>
        Left(
          SError.SErrorCrash(
            NameOf.qualifiedNameOfCurrentFunc,
            "ptx.finish: expected RootContextInfo",
          )
        )
    }

  // construct an IncompleteTransaction from the partial-transaction
  private[speedy] def finishIncomplete: transaction.IncompleteTransaction = {

    val ptx = unwind()

    transaction.IncompleteTransaction(
      Tx(
        ptx.nodes,
        ptx.context.children.toImmArray.toSeq.sortBy(_.index).toImmArray,
      ),
      ptx.locationInfo(),
    )
  }

  /** Extend the 'PartialTransaction' with a node for creating a
    * contract instance.
    */
  def insertCreate(
      submissionTime: Time.Timestamp,
      templateId: Ref.Identifier,
      arg: Value,
      agreementText: String,
      optLocation: Option[Location],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[Node.KeyWithMaintainers],
      version: TxVersion,
  ): Either[(PartialTransaction, Tx.TransactionError), (Value.ContractId, PartialTransaction)] = {
    val auth = Authorize(context.info.authorizers)
    val actionNodeSeed = context.nextActionChildSeed
    val discriminator =
      crypto.Hash.deriveContractDiscriminator(actionNodeSeed, submissionTime, stakeholders)
    val cid = Value.ContractId.V1(discriminator)
    val createNode = Node.Create(
      cid,
      templateId,
      arg,
      agreementText,
      signatories,
      stakeholders,
      key,
      version,
    )
    val nid = NodeId(nextNodeIdx)
    val ptx = copy(
      actionNodeLocations = actionNodeLocations :+ optLocation,
      nextNodeIdx = nextNodeIdx + 1,
      context = context.addActionChild(nid, version),
      nodes = nodes.updated(nid, createNode),
      actionNodeSeeds = actionNodeSeeds :+ actionNodeSeed,
    )
    val authorizationErrors =
      if (checkAuthorization) CheckAuthorization.authorizeCreate(optLocation, createNode)(auth)
      else Nil
    authorizationErrors match {
      case fa :: _ => Left((ptx, Tx.AuthFailureDuringExecution(nid, fa)))
      case Nil =>
        ptx.contractState.visitCreate(templateId, cid, key.map(_.key)) match {
          case Right(next) =>
            val nextPtx = ptx.copy(contractState = next)
            Right((cid, nextPtx))
          case Left(duplicate) =>
            Left((ptx, duplicate))
        }
    }
  }

  def insertFetch(
      coid: Value.ContractId,
      templateId: TypeConName,
      optLocation: Option[Location],
      signatories: Set[Party],
      observers: Set[Party],
      key: Option[Node.KeyWithMaintainers],
      byKey: Boolean,
      version: TxVersion,
  ): Either[Tx.TransactionError, PartialTransaction] = {
    val stakeholders = observers union signatories
    val contextActors = context.info.authorizers
    val actingParties = contextActors intersect stakeholders
    val auth = Authorize(context.info.authorizers)
    val nid = NodeId(nextNodeIdx)
    val node = Node.Fetch(
      coid,
      templateId,
      actingParties,
      signatories,
      stakeholders,
      key,
      normByKey(version, byKey),
      version,
    )
    mustBeActive(NameOf.qualifiedNameOfCurrentFunc, coid) {
      val newContractState = assertRightKey(
        // evaluation order tests require visitFetch proceeds authorizeFetch
        contractState.visitFetch(templateId, coid, key.map(_.key), byKey)
      )
      val authorizationErrors =
        if (checkAuthorization) CheckAuthorization.authorizeFetch(optLocation, node)(auth) else Nil
      authorizationErrors match {
        case fa :: _ => Left(Tx.AuthFailureDuringExecution(nid, fa))
        case Nil =>
          Right(insertLeafNode(node, version, optLocation, newContractState))
      }
    }
  }

  def insertLookup(
      templateId: TypeConName,
      optLocation: Option[Location],
      key: Node.KeyWithMaintainers,
      result: Option[Value.ContractId],
      version: TxVersion,
  ): Either[Tx.TransactionError, PartialTransaction] = {
    val auth = Authorize(context.info.authorizers)
    val nid = NodeId(nextNodeIdx)
    val node = Node.LookupByKey(
      templateId,
      key,
      result,
      version,
    )
    val gkey = GlobalKey.assertBuild(templateId, key.key)
    // This method is only called after we have already resolved the key in com.daml.lf.speedy.SBuiltin.SBUKeyBuiltin.execute
    // so the current state's global key inputs must resolve the key.
    val keyInput = contractState.globalKeyInputs(gkey)
    val newContractState = assertRightKey(
      contractState.visitLookup(templateId, key.key, keyInput.toKeyMapping, result)
    )
    val authorizationErrors =
      if (checkAuthorization) CheckAuthorization.authorizeLookupByKey(optLocation, node)(auth)
      else Nil
    authorizationErrors match {
      case fa :: _ => Left(Tx.AuthFailureDuringExecution(nid, fa))
      case Nil =>
        Right(insertLeafNode(node, version, optLocation, newContractState))
    }
  }

  /** Open an exercises context.
    * Must be closed by a `endExercises` or an `abortExercise`.
    */
  def beginExercises(
      targetId: Value.ContractId,
      templateId: TypeConName,
      interfaceId: Option[TypeConName],
      choiceId: ChoiceName,
      optLocation: Option[Location],
      consuming: Boolean,
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      choiceObservers: Set[Party],
      mbKey: Option[Node.KeyWithMaintainers],
      byKey: Boolean,
      chosenValue: Value,
      version: TxVersion,
  ): Either[Tx.TransactionError, PartialTransaction] = {
    val auth = Authorize(context.info.authorizers)
    val nid = NodeId(nextNodeIdx)
    val ec =
      ExercisesContextInfo(
        targetId = targetId,
        templateId = templateId,
        interfaceId = interfaceId,
        contractKey = mbKey,
        choiceId = choiceId,
        consuming = consuming,
        actingParties = actingParties,
        chosenValue = chosenValue,
        signatories = signatories,
        stakeholders = stakeholders,
        choiceObservers = choiceObservers,
        nodeId = nid,
        parent = context,
        byKey = byKey,
        version = version,
      )
    mustBeActive(NameOf.qualifiedNameOfCurrentFunc, targetId) {
      // important: the semantics of Daml dictate that contracts are immediately
      // inactive as soon as you exercise it. therefore, mark it as consumed now.
      val newContractState = assertRightKey(
        contractState.visitExercise(nid, templateId, targetId, mbKey.map(_.key), byKey, consuming)
      )
      val authorizationErrors =
        if (checkAuthorization)
          CheckAuthorization.authorizeExercise(optLocation, makeExNode(ec))(auth)
        else Nil
      authorizationErrors match {
        case fa :: _ => Left(Tx.AuthFailureDuringExecution(nid, fa))
        case Nil =>
          Right(
            copy(
              actionNodeLocations = actionNodeLocations :+ optLocation,
              nextNodeIdx = nextNodeIdx + 1,
              context = Context(ec),
              actionNodeSeeds = actionNodeSeeds :+ ec.actionNodeSeed, // must push before children
              contractState = newContractState,
            )
          )
      }
    }
  }

  /** Close normally an exercise context.
    * Must match a `beginExercises`.
    */
  def endExercises(result: TxVersion => Value): PartialTransaction =
    context.info match {
      case ec: ExercisesContextInfo =>
        val exerciseNode =
          makeExNode(ec).copy(
            children = context.children.toImmArray,
            exerciseResult = Some(result(ec.version)),
          )
        val nodeId = ec.nodeId
        copy(
          context =
            ec.parent.addActionChild(nodeId, exerciseNode.version min context.minChildVersion),
          nodes = nodes.updated(nodeId, exerciseNode),
        )
      case _ =>
        InternalError.runtimeException(
          NameOf.qualifiedNameOfCurrentFunc,
          "endExercises called in non-exercise context",
        )
    }

  /** Close a abruptly an exercise context du to an uncaught exception.
    * Must match a `beginExercises`.
    */
  def abortExercises: PartialTransaction =
    context.info match {
      case ec: ExercisesContextInfo =>
        val exerciseNode = makeExNode(ec).copy(children = context.children.toImmArray)
        val nodeId = ec.nodeId
        val actionNodeSeed = context.nextActionChildSeed
        copy(
          context =
            ec.parent.addActionChild(nodeId, exerciseNode.version min context.minChildVersion),
          nodes = nodes.updated(nodeId, exerciseNode),
          actionNodeSeeds =
            actionNodeSeeds :+ actionNodeSeed, // (NC) pushed by 'beginExercises'; why push again?
        )
      case _ =>
        InternalError.runtimeException(
          NameOf.qualifiedNameOfCurrentFunc,
          "abortExercises called in non-exercise context",
        )
    }

  private[this] def makeExNode(ec: ExercisesContextInfo): Node.Exercise = {
    Node.Exercise(
      targetCoid = ec.targetId,
      templateId = ec.templateId,
      interfaceId = ec.interfaceId,
      choiceId = ec.choiceId,
      consuming = ec.consuming,
      actingParties = ec.actingParties,
      chosenValue = ec.chosenValue,
      stakeholders = ec.stakeholders,
      signatories = ec.signatories,
      choiceObservers = ec.choiceObservers,
      children = ImmArray.Empty,
      exerciseResult = None,
      key = ec.contractKey,
      byKey = normByKey(ec.version, ec.byKey),
      version = ec.version,
    )
  }

  /** Open a Try context.
    *  Must be closed by `endTry` or `rollbackTry`.
    */
  def beginTry: PartialTransaction = {
    val nid = NodeId(nextNodeIdx)
    val info = TryContextInfo(nid, context, authorizers = context.info.authorizers)
    copy(
      nextNodeIdx = nextNodeIdx + 1,
      context = Context(info).copy(nextActionChildIdx = context.nextActionChildIdx),
      contractState = contractState.beginRollback(),
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
          ),
          contractState = contractState.dropRollback(),
        )
      case _ =>
        InternalError.runtimeException(
          NameOf.qualifiedNameOfCurrentFunc,
          "endTry called in non-catch context",
        )
    }

  /** Close a try context, by catching an exception,
    * i.e. a exception was thrown inside the context, and the catch associated to the try context did handle it.
    */
  def rollbackTry(ex: SValue.SAny): PartialTransaction = {
    // we must never create a rollback containing a node with a version pre-dating exceptions
    if (context.minChildVersion < TxVersion.minExceptions) {
      throw SError.SErrorDamlException(
        interpretation.Error.UnhandledException(ex.ty, ex.value.toUnnormalizedValue)
      )
    }
    context.info match {
      case info: TryContextInfo =>
        // In the case of there being no children we could drop the entire rollback node.
        // But we do that in a later normalization phase, not here.
        val rollbackNode = Node.Rollback(context.children.toImmArray)
        copy(
          context = info.parent
            .addRollbackChild(info.nodeId, context.minChildVersion, context.nextActionChildIdx),
          nodes = nodes.updated(info.nodeId, rollbackNode),
          contractState = contractState.endRollback(),
        )
      case _ =>
        InternalError.runtimeException(
          NameOf.qualifiedNameOfCurrentFunc,
          "rollbackTry called in non-catch context",
        )
    }
  }

  /** Double check the execution of a step with the unconsumedness of a
    * `ContractId`.
    */
  private[this] def mustBeActive[T](
      loc: => String,
      cid: Value.ContractId,
  )(
      body: => T
  ): T =
    if (consumedByOrInactive(cid).isDefined)
      InternalError.runtimeException(
        loc,
        "try to build a node using a consumed or inactive contract.",
      )
    else
      body

  /** Insert the given `LeafNode` under a fresh node-id, and return it */
  private[this] def insertLeafNode(
      node: Node.LeafOnlyAction,
      version: TxVersion,
      optLocation: Option[Location],
      newContractState: ContractStateMachine[NodeId]#State,
  ): PartialTransaction = {
    val _ = version
    val nid = NodeId(nextNodeIdx)
    copy(
      actionNodeLocations = actionNodeLocations :+ optLocation,
      nextNodeIdx = nextNodeIdx + 1,
      context = context.addActionChild(nid, version),
      nodes = nodes.updated(nid, node),
      contractState = newContractState,
    )
  }

  /** Unwind the transaction aborting all incomplete nodes */
  def unwind(): PartialTransaction = {
    @tailrec
    def go(ptx: PartialTransaction): PartialTransaction = ptx.context.info match {
      case _: PartialTransaction.ExercisesContextInfo => go(ptx.abortExercises)
      case _: PartialTransaction.TryContextInfo => go(ptx.endTry)
      case _: PartialTransaction.RootContextInfo => ptx
    }
    go(this)
  }

}
