// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref.{ChoiceName, Location, Party, TypeConName}
import com.daml.lf.data.{BackStack, Bytes, ImmArray, Time}
import com.daml.lf.transaction.{GenTransaction, Node, Transaction => Tx}
import com.daml.lf.value.Value

import scala.collection.breakOut
import scala.collection.immutable.HashMap

object PartialTransaction {

  type NodeIdx = Value.NodeIdx

  /** Contexts of the transaction graph builder, which we use to record
    * the sub-transaction structure due to 'exercises' statements.
    */
  sealed abstract class Context extends Product with Serializable {
    def contextSeed: Option[crypto.Hash]

    def children: BackStack[Value.NodeId]

    def addChild(child: Value.NodeId): Context
  }

  /** The root context, which is what we use when we are not exercising
    * a choice.
    */
  final case class ContextRoot(
      contextSeed: Option[crypto.Hash],
      children: BackStack[Value.NodeId] = BackStack.empty,
  ) extends Context {
    override def addChild(child: Value.NodeId): ContextRoot = copy(children = children :+ child)
  }

  /** Context when creating a sub-transaction due to an exercises. */
  final case class ContextExercise(
      ctx: ExercisesContext,
      children: BackStack[Value.NodeId] = BackStack.empty,
  ) extends Context {
    override def addChild(child: Value.NodeId): ContextExercise =
      copy(children = children :+ child)

    override def contextSeed: Option[crypto.Hash] = ctx.contextSeed
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
      contextSeed: Option[crypto.Hash],
      targetId: Value.ContractId,
      templateId: TypeConName,
      contractKey: Option[Node.KeyWithMaintainers[Tx.Value[Nothing]]],
      choiceId: ChoiceName,
      optLocation: Option[Location],
      consuming: Boolean,
      actingParties: Set[Party],
      chosenValue: Tx.Value[Value.ContractId],
      signatories: Set[Party],
      stakeholders: Set[Party],
      controllers: Set[Party],
      nodeId: Value.NodeId,
      parent: Context,
  )

  def initial(seedWithTime: Option[(crypto.Hash, Time.Timestamp)] = None) =
    PartialTransaction(
      submissionTime = seedWithTime.map(_._2),
      nextNodeIdx = 0,
      nodes = HashMap.empty,
      consumedBy = Map.empty,
      context = ContextRoot(seedWithTime.map(_._1)),
      aborted = None,
      keys = Map.empty,
      localContracts = Map.empty,
      globalContracts = Map.empty,
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
  * @param localContracts A map that associates to each contract created the
  *                      node in which it was created.
  * @param globalContracts A map that associates to each fetched AbsoluteContractId.V1
  *                       contract id its respective contract instance.
  *                       other format of contract ids are not cached.
  */
case class PartialTransaction(
    submissionTime: Option[Time.Timestamp],
    nextNodeIdx: Int,
    nodes: HashMap[Value.NodeId, Tx.Node],
    consumedBy: Map[Value.ContractId, Value.NodeId],
    context: PartialTransaction.Context,
    aborted: Option[Tx.TransactionError],
    keys: Map[Node.GlobalKey, Option[Value.ContractId]],
    localContracts: Map[Value.ContractId, Value.NodeId],
    globalContracts: Map[crypto.Hash, Map[Bytes, Tx.ContractInst[Value.ContractId]]],
) {

  import PartialTransaction._

  def nodesToString: String =
    if (nodes.isEmpty) "<empty transaction>"
    else {
      val sb = new StringBuilder()

      def addToStringBuilder(
          nid: Value.NodeId,
          node: Node.GenNode.WithTxValue[Value.NodeId, Value.ContractId],
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
        val allChildNodeIds: Set[Value.NodeId] = nodes.values.flatMap {
          case _: Node.LeafOnlyNode[_, _] => Nil
          case ex: Node.NodeExercises[Value.NodeId, _, _] => ex.children.toSeq
        }(breakOut)

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
    *  transaction from the given 'PartialTransaction'. This fails if
    *  the 'PartialTransaction' is not yet complete or has been
    *  aborted.
    */
  def finish: Either[PartialTransaction, Tx.Transaction] =
    context match {
      case ContextRoot(_, children) if aborted.isEmpty =>
        Right(GenTransaction(nodes = nodes, roots = children.toImmArray))
      case _ =>
        Left(this)
    }

  private def lookupLocalContract(
      lcoid: Value.ContractId,
  ): Option[Tx.ContractInst[Value.ContractId]] =
    for {
      nid <- localContracts.get(lcoid)
      node <- nodes.get(nid)
      coinst <- node match {
        case create: Node.NodeCreate.WithTxValue[Value.ContractId] =>
          Some(create.coinst)
        case _: Node.NodeExercises[_, _, _] | _: Node.NodeFetch[_, _] |
            _: Node.NodeLookupByKey[_, _] =>
          None
      }
    } yield coinst

  private def lookupGlobalContract(
      gcoid: Value.ContractId
  ): Option[Tx.ContractInst[Value.ContractId]] =
    gcoid match {
      case Value.AbsoluteContractId.V1(discriminator, suffix) =>
        globalContracts.get(discriminator).flatMap(_.get(suffix))
      case _ =>
        None
    }

  def lookupCachedContract(
      coid: Value.ContractId
  ): Option[Tx.ContractInst[Value.ContractId]] =
    lookupLocalContract(coid).orElse(lookupGlobalContract(coid))

  /** Update the globalContract if coid is an `Value.AbsoluteContractId.V1`,
    *  idempotent otherwise.
    */
  def cachedContract(
      coid: Value.ContractId,
      contract: Tx.ContractInst[Value.ContractId]
  ): PartialTransaction =
    coid match {
      case Value.AbsoluteContractId.V1(discriminator, suffix) =>
        copy(
          globalContracts = globalContracts.updated(
            discriminator,
            globalContracts.getOrElse(discriminator, Map.empty).updated(suffix, contract)
          )
        )
      case _ =>
        this
    }

  /** Extend the 'PartialTransaction' with a node for creating a
    * contract instance.
    */
  def insertCreate(
      coinst: Tx.ContractInst[Value.ContractId],
      optLocation: Option[Location],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[Node.KeyWithMaintainers[Tx.Value[Nothing]]],
  ): Either[String, (Value.ContractId, PartialTransaction)] = {
    val serializableErrs = serializable(coinst.arg)
    if (serializableErrs.nonEmpty) {
      Left(
        s"""Trying to create a contract with a non-serializable value: ${serializableErrs.iterator
          .mkString(",")}""",
      )
    } else {
      val nodeSeed = deriveChildSeed
      val discriminator =
        for {
          seed <- nodeSeed
          time <- submissionTime
        } yield crypto.Hash.deriveContractDiscriminator(seed, time, stakeholders)
      val cid = discriminator.fold[Value.ContractId](
        Value.RelativeContractId(Value.NodeId(nextNodeIdx))
      )(Value.AbsoluteContractId.V1(_))
      val createNode = Node.NodeCreate(
        nodeSeed,
        cid,
        coinst,
        optLocation,
        signatories,
        stakeholders,
        key,
      )
      val nid = Value.NodeId(nextNodeIdx)
      val ptx = copy(
        nextNodeIdx = nextNodeIdx + 1,
        context = context.addChild(nid),
        nodes = nodes.updated(nid, createNode),
        localContracts = localContracts.updated(cid, nid)
      )

      // if we have a contract key being added, include it in the list of
      // active keys
      key match {
        case None => Right((cid, ptx))
        case Some(kWithM) =>
          val ck = Node.GlobalKey(coinst.template, kWithM.key.value)
          Right((cid, ptx.copy(keys = ptx.keys.updated(ck, Some(cid)))))
      }
    }
  }

  def serializable(a: Tx.Value[Value.ContractId]): ImmArray[String] = a.value.serializable()

  def insertFetch(
      coid: Value.ContractId,
      templateId: TypeConName,
      optLocation: Option[Location],
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      key: Option[Node.KeyWithMaintainers[Tx.Value[Nothing]]]
  ): PartialTransaction =
    mustBeActive(
      coid,
      templateId,
      insertLeafNode(
        Node
          .NodeFetch(
            coid,
            templateId,
            optLocation,
            Some(actingParties),
            signatories,
            stakeholders,
            key),
      ),
    )

  def insertLookup(
      templateId: TypeConName,
      optLocation: Option[Location],
      key: Node.KeyWithMaintainers[Tx.Value[Nothing]],
      result: Option[Value.ContractId],
  ): PartialTransaction =
    insertLeafNode(Node.NodeLookupByKey(templateId, optLocation, key, result))

  def beginExercises(
      targetId: Value.ContractId,
      templateId: TypeConName,
      choiceId: ChoiceName,
      optLocation: Option[Location],
      consuming: Boolean,
      actingParties: Set[Party],
      signatories: Set[Party],
      stakeholders: Set[Party],
      controllers: Set[Party],
      mbKey: Option[Node.KeyWithMaintainers[Tx.Value[Nothing]]],
      chosenValue: Tx.Value[Value.ContractId],
  ): Either[String, PartialTransaction] = {
    val serializableErrs = serializable(chosenValue)
    if (serializableErrs.nonEmpty) {
      Left(
        s"""Trying to exercise a choice with a non-serializable value: ${serializableErrs.iterator
          .mkString(",")}""",
      )
    } else {
      val nid = Value.NodeId(nextNodeIdx)
      Right(
        mustBeActive(
          targetId,
          templateId,
          copy(
            nextNodeIdx = nextNodeIdx + 1,
            context = ContextExercise(
              ExercisesContext(
                contextSeed = deriveChildSeed,
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
                nodeId = nid,
                parent = context,
              ),
            ),
            // important: the semantics of DAML dictate that contracts are immediately
            // inactive as soon as you exercise it. therefore, mark it as consumed now.
            consumedBy = if (consuming) consumedBy.updated(targetId, nid) else consumedBy,
            keys = mbKey match {
              case Some(kWithM) if consuming =>
                keys.updated(Node.GlobalKey(templateId, kWithM.key.value), None)
              case _ => keys
            },
          ),
        ),
      )
    }
  }

  def endExercises(value: Tx.Value[Value.ContractId]): PartialTransaction =
    context match {
      case ContextExercise(ec, children) =>
        val exerciseNode = Node.NodeExercises(
          nodeSeed = ec.contextSeed,
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
        noteAbort(Tx.EndExerciseInRootContext)
    }

  /** Note that the transaction building failed due to the given error */
  def noteAbort(err: Tx.TransactionError): PartialTransaction = copy(aborted = Some(err))

  /** `True` iff the given `ContractId` has been consumed already */
  def isConsumed(coid: Value.ContractId): Boolean = consumedBy.contains(coid)

  /** Guard the execution of a step with the unconsumedness of a
    * `ContractId`
    */
  def mustBeActive(
      coid: Value.ContractId,
      templateId: TypeConName,
      f: => PartialTransaction,
  ): PartialTransaction =
    consumedBy.get(coid) match {
      case None => f
      case Some(nid) => noteAbort(Tx.ContractNotActive(coid, templateId, nid))
    }

  /** Insert the given `LeafNode` under a fresh node-id, and return it */
  def insertLeafNode(node: Tx.LeafNode): PartialTransaction = {
    val nid = Value.NodeId(nextNodeIdx)
    copy(
      nextNodeIdx = nextNodeIdx + 1,
      context = context.addChild(nid),
      nodes = nodes.updated(nid, node),
    )
  }

  def deriveChildSeed: Option[crypto.Hash] =
    context.contextSeed.map(crypto.Hash.deriveNodeSeed(_, context.children.length))

}
