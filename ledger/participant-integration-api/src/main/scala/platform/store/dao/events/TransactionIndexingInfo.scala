// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.ledger.participant.state.v1.{
  CommittedTransaction,
  DivulgedContract,
  Offset,
  SubmitterInfo
}
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.BlindingInfo

final case class TransactionIndexingInfo(
    submitterInfo: Option[SubmitterInfo],
    workflowId: Option[WorkflowId],
    transactionId: TransactionId,
    ledgerEffectiveTime: Instant,
    offset: Offset,
    events: Vector[(NodeId, Node)],
    netCreates: Set[Create],
    netArchives: Set[ContractId],
    archives: Set[ContractId],
    stakeholders: WitnessRelation[NodeId],
    disclosure: WitnessRelation[NodeId],
    netVisibility: WitnessRelation[ContractId],
    divulgedContracts: Iterable[DivulgedContract],
) {
  def serialize(translation: LfValueTranslation): TransactionIndexingInfo.Serialized = {

    val createArgumentsByContract = Map.newBuilder[ContractId, Array[Byte]]
    val createArguments = Map.newBuilder[NodeId, Array[Byte]]
    val createKeyValues = Map.newBuilder[NodeId, Array[Byte]]
    val exerciseArguments = Map.newBuilder[NodeId, Array[Byte]]
    val exerciseResults = Map.newBuilder[NodeId, Array[Byte]]

    for ((nodeId, event) <- events) {
      val eventId = EventId(transactionId, nodeId)
      event match {
        case create: Create =>
          val (createArgument, createKeyValue) = translation.serialize(eventId, create)
          createArgumentsByContract += ((create.coid, createArgument))
          createArguments += ((nodeId, createArgument))
          createKeyValue.foreach(key => createKeyValues += ((nodeId, key)))
        case exercise: Exercise =>
          val (exerciseArgument, exerciseResult) = translation.serialize(eventId, exercise)
          exerciseArguments += ((nodeId, exerciseArgument))
          exerciseResult.foreach(result => exerciseResults += ((nodeId, result)))
        case _ => throw new UnexpectedNodeException(nodeId, transactionId)
      }
    }

    for (DivulgedContract(contractId, contractInst) <- divulgedContracts) {
      val serializedCreateArgument = translation.serialize(contractId, contractInst.arg)
      createArgumentsByContract += ((contractId, serializedCreateArgument))
    }

    TransactionIndexingInfo.Serialized(
      info = this,
      createArgumentsByContract = createArgumentsByContract.result(),
      createArguments = createArguments.result(),
      createKeyValues = createKeyValues.result(),
      exerciseArguments = exerciseArguments.result(),
      exerciseResults = exerciseResults.result(),
    )
  }

}

object TransactionIndexingInfo {

  private class Builder(blinding: BlindingInfo) {

    private val events = Vector.newBuilder[(NodeId, Node)]
    private val creates = Set.newBuilder[Create]
    private val archives = Set.newBuilder[ContractId]
    private val stakeholders = Map.newBuilder[NodeId, Set[Party]]
    private val disclosure = Map.newBuilder[NodeId, Set[Party]]
    private val visibility = Vector.newBuilder[(ContractId, Set[Party])]

    private def addEventAndDisclosure(event: (NodeId, Node)): Unit = {
      events += event
      disclosure += ((event._1, blinding.disclosure(event._1)))
    }

    private def addCreate(create: Create): Unit =
      creates += create

    private def addArchive(contractId: ContractId): Unit =
      archives += contractId

    private def addDivulgence(contractId: ContractId): Unit =
      for (divulgees <- blinding.divulgence.get(contractId)) {
        visibility += ((contractId, divulgees))
      }

    private def addVisibility(contractId: ContractId, parties: Set[Party]): Unit =
      visibility += ((contractId, parties))

    private def addStakeholders(nodeId: NodeId, parties: Set[Party]): Unit =
      stakeholders += ((nodeId, parties))

    def add(event: (NodeId, Node)): Builder = {
      event match {
        case (nodeId, create: Create) =>
          addEventAndDisclosure(event)
          addVisibility(create.coid, blinding.disclosure(nodeId))
          addStakeholders(nodeId, create.stakeholders)
          addCreate(create)
        case (nodeId, exercise: Exercise) =>
          addEventAndDisclosure(event)
          addVisibility(exercise.targetCoid, blinding.disclosure(nodeId))
          addDivulgence(exercise.targetCoid)
          if (exercise.consuming) {
            addStakeholders(nodeId, exercise.stakeholders)
            addArchive(exercise.targetCoid)
          } else {
            addStakeholders(nodeId, Set.empty)
          }
        case (_, fetch: Fetch) =>
          addDivulgence(fetch.coid)
        case _ =>
          () // ignore anything else
      }
      this
    }

    private def visibility(contracts: Iterable[DivulgedContract]): WitnessRelation[ContractId] =
      Relation(
        contracts.map(c => c.contractId -> blinding.divulgence.getOrElse(c.contractId, Set.empty)),
      )

    def build(
        submitterInfo: Option[SubmitterInfo],
        workflowId: Option[WorkflowId],
        transactionId: TransactionId,
        ledgerEffectiveTime: Instant,
        offset: Offset,
        divulgedContracts: Iterable[DivulgedContract],
    ): TransactionIndexingInfo = {
      val created = creates.result()
      val archived = archives.result()
      val allCreatedContractIds = created.map(_.coid)
      val allContractIds = allCreatedContractIds.union(archived)
      val netCreates = created.filterNot(c => archived(c.coid))
      val netArchives = archived.filterNot(allCreatedContractIds)
      val netDivulgedContracts = divulgedContracts.filterNot(c => allContractIds(c.contractId))
      val netTransactionVisibility = Relation(visibility.result()).filterKeys(!archived(_))
      val netVisibility = Relation.union(netTransactionVisibility, visibility(netDivulgedContracts))
      TransactionIndexingInfo(
        submitterInfo = submitterInfo,
        workflowId = workflowId,
        transactionId = transactionId,
        ledgerEffectiveTime = ledgerEffectiveTime,
        offset = offset,
        events = events.result(),
        netCreates = netCreates,
        netArchives = netArchives,
        archives = archived,
        stakeholders = stakeholders.result(),
        disclosure = disclosure.result(),
        netVisibility = netVisibility,
        divulgedContracts = netDivulgedContracts,
      )
    }
  }

  final case class Serialized private (
      info: TransactionIndexingInfo,
      createArgumentsByContract: Map[ContractId, Array[Byte]],
      createArguments: Map[NodeId, Array[Byte]],
      createKeyValues: Map[NodeId, Array[Byte]],
      exerciseArguments: Map[NodeId, Array[Byte]],
      exerciseResults: Map[NodeId, Array[Byte]],
  )

  def from(
      blindingInfo: BlindingInfo,
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[DivulgedContract],
  ): TransactionIndexingInfo =
    transaction
      .fold(new Builder(blindingInfo))(_ add _)
      .build(
        submitterInfo = submitterInfo,
        workflowId = workflowId,
        transactionId = transactionId,
        ledgerEffectiveTime = ledgerEffectiveTime,
        offset = offset,
        divulgedContracts = divulgedContracts,
      )

}
