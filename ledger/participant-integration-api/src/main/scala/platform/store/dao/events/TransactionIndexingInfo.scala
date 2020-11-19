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

final case class TransactionIndexingInfo private (
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

    private def addCreation(create: Create): Unit =
      creates += create

    private def addArchival(contractId: ContractId): Unit =
      archives += contractId

    private def addDivulgence(contractId: ContractId): Unit =
      for (divulgees <- blinding.divulgence.get(contractId)) {
        visibility += ((contractId, divulgees))
      }

    private def addStakeholders(nodeId: NodeId, parties: Set[Party]): Unit =
      stakeholders += ((nodeId, parties))

    def add(event: (NodeId, Node)): Builder = {
      event match {
        case (nodeId, create: Create) =>
          create.templateId.toString
          addEventAndDisclosure(event)
          addStakeholders(nodeId, create.stakeholders)
          addCreation(create)
          visibility += ((create.coid, blinding.disclosure(nodeId)))
        case (nodeId, exercise: Exercise) =>
          addEventAndDisclosure(event)
          addDivulgence(exercise.targetCoid)
          visibility += ((exercise.targetCoid, blinding.disclosure(nodeId)))
          if (exercise.consuming) {
            addStakeholders(nodeId, exercise.stakeholders)
            addArchival(exercise.targetCoid)
          }
        case (_, fetch: Fetch) =>
          addDivulgence(fetch.coid)
        case _ =>
          () // ignore anything else
      }
      this
    }

    def build(
        submitterInfo: Option[SubmitterInfo],
        workflowId: Option[WorkflowId],
        transactionId: TransactionId,
        ledgerEffectiveTime: Instant,
        offset: Offset,
        divulgedContracts: Iterable[DivulgedContract],
    ): TransactionIndexingInfo = {
      val allCreates = creates.result()
      val allCreatedContractIds = allCreates.iterator.map(_.coid).toSet
      val allArchives = archives.result()
      val netCreates = allCreates.filter(c => !allArchives(c.coid))
      val netCreatedContractIds = netCreates.iterator.map(_.coid).toSet
      val netArchives = allArchives.diff(allCreatedContractIds)
      val fullVisibility = visibility.result()
      val netVisibility = fullVisibility
        .filter {
          case (contractId, _) => netCreatedContractIds(contractId)
        }
        .groupBy(_._1)
        .map {
          case (contractId, parts) =>
            contractId -> parts.map(_._2).reduce(_ union _)
        }
      TransactionIndexingInfo(
        submitterInfo = submitterInfo,
        workflowId = workflowId,
        transactionId = transactionId,
        ledgerEffectiveTime = ledgerEffectiveTime,
        offset = offset,
        events = events.result(),
        netCreates = netCreates,
        netArchives = netArchives,
        archives = allArchives,
        stakeholders = stakeholders.result(),
        disclosure = disclosure.result(),
        netVisibility = netVisibility,
        divulgedContracts = divulgedContracts.filter(c => !netCreatedContractIds(c.contractId)),
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
  ): TransactionIndexingInfo = {

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

}
