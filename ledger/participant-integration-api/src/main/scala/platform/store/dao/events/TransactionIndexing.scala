// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.ledger.participant.state.v1.{
  CommittedTransaction,
  DivulgedContract,
  Offset,
  SubmitterInfo,
}
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.BlindingInfo
import com.daml.platform.store.serialization.Compression

import scala.collection.compat._

final case class TransactionIndexing(
    transaction: TransactionIndexing.TransactionInfo,
    events: TransactionIndexing.EventsInfo,
    contracts: TransactionIndexing.ContractsInfo,
    contractWitnesses: TransactionIndexing.ContractWitnessesInfo,
)

object TransactionIndexing {

  def serialize(
      translation: LfValueTranslation,
      transactionId: TransactionId,
      events: Vector[(NodeId, Node)],
      divulgence: Iterable[DivulgedContract],
  ): Serialized = {

    val createArguments = Vector.newBuilder[(NodeId, ContractId, Array[Byte])]
    val divulgedContracts = Vector.newBuilder[(ContractId, Array[Byte])]
    val createKeyValues = Vector.newBuilder[(NodeId, Array[Byte])]
    val exerciseArguments = Vector.newBuilder[(NodeId, Array[Byte])]
    val exerciseResults = Vector.newBuilder[(NodeId, Array[Byte])]

    for ((nodeId, event) <- events) {
      val eventId = EventId(transactionId, nodeId)
      event match {
        case create: Create =>
          val (createArgument, createKeyValue) = translation.serialize(eventId, create)
          createArguments += ((nodeId, create.coid, createArgument))
          createKeyValue.foreach(key => createKeyValues += ((nodeId, key)))
        case exercise: Exercise =>
          val (exerciseArgument, exerciseResult) = translation.serialize(eventId, exercise)
          exerciseArguments += ((nodeId, exerciseArgument))
          exerciseResult.foreach(result => exerciseResults += ((nodeId, result)))
        case _ => throw new UnexpectedNodeException(nodeId, transactionId)
      }
    }

    for (DivulgedContract(contractId, contractInst) <- divulgence) {
      val serializedCreateArgument = translation.serialize(contractId, contractInst.arg)
      divulgedContracts += ((contractId, serializedCreateArgument))
    }

    Serialized(
      createArguments = createArguments.result(),
      divulgedContracts = divulgedContracts.result(),
      createKeyValues = createKeyValues.result(),
      exerciseArguments = exerciseArguments.result(),
      exerciseResults = exerciseResults.result(),
    )

  }

  def compress(
      serialized: Serialized,
      compressionStrategy: CompressionStrategy,
      compressionMetrics: CompressionMetrics,
  ): Compressed = {

    val createArgumentsByContract = Map.newBuilder[ContractId, Array[Byte]]
    val createArguments = Map.newBuilder[NodeId, Array[Byte]]
    val createKeyValues = Map.newBuilder[NodeId, Array[Byte]]
    val exerciseArguments = Map.newBuilder[NodeId, Array[Byte]]
    val exerciseResults = Map.newBuilder[NodeId, Array[Byte]]

    for ((contractId, argument) <- serialized.divulgedContracts) {
      val compressedArgument =
        compressionStrategy.createArgumentCompression
          .compress(argument, compressionMetrics.createArgument)
      createArgumentsByContract += ((contractId, compressedArgument))
    }

    for ((nodeId, contractId, argument) <- serialized.createArguments) {
      val compressedArgument = compressionStrategy.createArgumentCompression
        .compress(argument, compressionMetrics.createArgument)
      createArguments += ((nodeId, compressedArgument))
      createArgumentsByContract += ((contractId, compressedArgument))
    }

    for ((nodeId, key) <- serialized.createKeyValues) {
      val compressedKey = compressionStrategy.createKeyValueCompression
        .compress(key, compressionMetrics.createKeyValue)
      createKeyValues += ((nodeId, compressedKey))
    }

    for ((nodeId, argument) <- serialized.exerciseArguments) {
      val compressedArgument = compressionStrategy.exerciseArgumentCompression
        .compress(argument, compressionMetrics.exerciseArgument)
      exerciseArguments += ((nodeId, compressedArgument))
    }

    for ((nodeId, result) <- serialized.exerciseResults) {
      val compressedResult = compressionStrategy.exerciseResultCompression
        .compress(result, compressionMetrics.exerciseResult)
      exerciseResults += ((nodeId, compressedResult))
    }

    Compressed(
      contracts = Compressed.Contracts(
        createArguments = createArgumentsByContract.result(),
        createArgumentsCompression = compressionStrategy.createArgumentCompression,
      ),
      events = Compressed.Events(
        createArguments = createArguments.result(),
        createArgumentsCompression = compressionStrategy.createArgumentCompression,
        createKeyValues = createKeyValues.result(),
        createKeyValueCompression = compressionStrategy.createKeyValueCompression,
        exerciseArguments = exerciseArguments.result(),
        exerciseArgumentsCompression = compressionStrategy.exerciseArgumentCompression,
        exerciseResults = exerciseResults.result(),
        exerciseResultsCompression = compressionStrategy.exerciseResultCompression,
      ),
    )
  }

  private class Builder(blinding: BlindingInfo) {

    private val events = Vector.newBuilder[(NodeId, Node)]
    private val creates = Set.newBuilder[Create]
    private val archives = Set.newBuilder[ContractId]
    private val stakeholders = Map.newBuilder[NodeId, Set[Party]]
    private val disclosure = Map.newBuilder[NodeId, Set[Party]]
    private val visibility = Vector.newBuilder[(ContractId, Set[Party])]

    for (contractId <- blinding.divulgence.keys) {
      addDivulgence(contractId)
    }

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
        case _ =>
          () // ignore anything else
      }
      this
    }

    private def visibility(contracts: Iterable[DivulgedContract]): WitnessRelation[ContractId] =
      Relation.from(
        contracts.map(c => c.contractId -> blinding.divulgence.getOrElse(c.contractId, Set.empty))
      )

    def build(
        submitterInfo: Option[SubmitterInfo],
        workflowId: Option[WorkflowId],
        transactionId: TransactionId,
        ledgerEffectiveTime: Instant,
        offset: Offset,
        divulgedContracts: Iterable[DivulgedContract],
    ): TransactionIndexing = {
      val created = creates.result()
      val archived = archives.result()
      val allCreatedContractIds = created.map(_.coid)
      val allContractIds = allCreatedContractIds.union(archived)
      val netCreates = created.filterNot(c => archived(c.coid))
      val netArchives = archived.filterNot(allCreatedContractIds)
      val netDivulgedContracts = divulgedContracts.filterNot(c => allContractIds(c.contractId))
      val netTransactionVisibility =
        Relation.from(visibility.result()).view.filterKeys(!archived(_)).toMap
      val netVisibility = Relation.union(netTransactionVisibility, visibility(netDivulgedContracts))
      TransactionIndexing(
        transaction = TransactionInfo(
          submitterInfo = submitterInfo,
          workflowId = workflowId,
          transactionId = transactionId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          offset = offset,
        ),
        events = EventsInfo(
          events = events.result(),
          archives = archived,
          stakeholders = stakeholders.result(),
          disclosure = disclosure.result(),
        ),
        contracts = ContractsInfo(
          netCreates = netCreates,
          netArchives = netArchives,
          divulgedContracts = netDivulgedContracts,
        ),
        contractWitnesses = ContractWitnessesInfo(
          netArchives = netArchives,
          netVisibility = netVisibility,
        ),
      )
    }
  }

  final case class TransactionInfo(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offset: Offset,
  )

  final case class EventsInfo(
      events: Vector[(NodeId, Node)],
      archives: Set[ContractId],
      stakeholders: WitnessRelation[NodeId],
      disclosure: WitnessRelation[NodeId],
  )

  final case class ContractsInfo(
      netCreates: Set[Create],
      netArchives: Set[ContractId],
      divulgedContracts: Iterable[DivulgedContract],
  )

  final case class ContractWitnessesInfo(
      netArchives: Set[ContractId],
      netVisibility: WitnessRelation[ContractId],
  )

  final case class Serialized(
      createArguments: Vector[(NodeId, ContractId, Array[Byte])],
      divulgedContracts: Vector[(ContractId, Array[Byte])],
      createKeyValues: Vector[(NodeId, Array[Byte])],
      exerciseArguments: Vector[(NodeId, Array[Byte])],
      exerciseResults: Vector[(NodeId, Array[Byte])],
  ) {
    val createArgumentsByContract = createArguments.map { case (_, contractId, arg) =>
      contractId -> arg
    }.toMap
  }

  object Compressed {

    final case class Contracts(
        createArguments: Map[ContractId, Array[Byte]],
        createArgumentsCompression: Compression.Algorithm,
    )

    final case class Events(
        createArguments: Map[NodeId, Array[Byte]],
        createArgumentsCompression: Compression.Algorithm,
        createKeyValues: Map[NodeId, Array[Byte]],
        createKeyValueCompression: Compression.Algorithm,
        exerciseArguments: Map[NodeId, Array[Byte]],
        exerciseArgumentsCompression: Compression.Algorithm,
        exerciseResults: Map[NodeId, Array[Byte]],
        exerciseResultsCompression: Compression.Algorithm,
    ) {
      def assertCreate(nodeId: NodeId): (Array[Byte], Option[Array[Byte]]) = {
        assert(createArguments.contains(nodeId), s"Node $nodeId is not a create event")
        (createArguments(nodeId), createKeyValues.get(nodeId))
      }

      def assertExercise(nodeId: NodeId): (Array[Byte], Option[Array[Byte]]) = {
        assert(exerciseArguments.contains(nodeId), s"Node $nodeId is not an exercise event")
        (exerciseArguments(nodeId), exerciseResults.get(nodeId))
      }
    }

  }

  final case class Compressed(
      contracts: Compressed.Contracts,
      events: Compressed.Events,
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
  ): TransactionIndexing =
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
