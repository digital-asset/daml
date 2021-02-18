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
import com.daml.platform.indexer.OffsetStep
import com.daml.platform.store.serialization.Compression

import scala.collection.compat._

final case class TransactionIndexing(
    events: TransactionIndexing.EventsInfoBatch,
    contracts: TransactionIndexing.ContractsInfo,
    contractWitnesses: TransactionIndexing.ContractWitnessesInfo,
)

object TransactionIndexing {
  type Combine[T] = (T, T) => T

  val combine: Combine[TransactionIndexing] =
    (left, right) =>
      TransactionIndexing(
        events = EventsInfoBatch.combine(left.events, right.events),
        contracts = ContractsInfo.combine(left.contracts, right.contracts),
        contractWitnesses =
          ContractWitnessesInfo.combine(left.contractWitnesses, right.contractWitnesses),
      )

  def serialize(
      translation: LfValueTranslation,
      eventsBatchInfo: List[(TransactionId, EventsInfo)],
      divulgence: Iterable[DivulgedContract],
  ): Serialized = {
    val createArguments = Vector.newBuilder[(TransactionId, NodeId, ContractId, Array[Byte])]
    val divulgedContracts = Vector.newBuilder[(ContractId, Array[Byte])]
    val createKeyValues = Vector.newBuilder[(TransactionId, NodeId, Array[Byte])]
    val exerciseArguments = Vector.newBuilder[(TransactionId, NodeId, Array[Byte])]
    val exerciseResults = Vector.newBuilder[(TransactionId, NodeId, Array[Byte])]

    for {
      (transactionId, info) <- eventsBatchInfo
      (nodeId, event) <- info.events
    } {
      val eventId = EventId(transactionId, nodeId)
      event match {
        case create: Create =>
          val (createArgument, createKeyValue) = translation.serialize(eventId, create)
          createArguments += ((transactionId, nodeId, create.coid, createArgument))
          createKeyValue.foreach(key => createKeyValues += ((transactionId, nodeId, key)))
        case exercise: Exercise =>
          val (exerciseArgument, exerciseResult) = translation.serialize(eventId, exercise)
          exerciseArguments += ((transactionId, nodeId, exerciseArgument))
          exerciseResult.foreach(result => exerciseResults += ((transactionId, nodeId, result)))
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
    val createArguments = Map.newBuilder[EventId, Array[Byte]]
    val createKeyValues = Map.newBuilder[EventId, Array[Byte]]
    val exerciseArguments = Map.newBuilder[EventId, Array[Byte]]
    val exerciseResults = Map.newBuilder[EventId, Array[Byte]]

    for ((contractId, argument) <- serialized.divulgedContracts) {
      val compressedArgument =
        compressionStrategy.createArgumentCompression
          .compress(argument, compressionMetrics.createArgument)
      createArgumentsByContract += ((contractId, compressedArgument))
    }

    for ((transactionId, nodeId, contractId, argument) <- serialized.createArguments) {
      val compressedArgument = compressionStrategy.createArgumentCompression
        .compress(argument, compressionMetrics.createArgument)
      createArguments += (EventId(transactionId, nodeId) -> compressedArgument)
      createArgumentsByContract += ((contractId, compressedArgument))
    }

    for ((transactionId, nodeId, key) <- serialized.createKeyValues) {
      val compressedKey = compressionStrategy.createKeyValueCompression
        .compress(key, compressionMetrics.createKeyValue)
      createKeyValues += (EventId(transactionId, nodeId) -> compressedKey)
    }

    for ((transactionId, nodeId, argument) <- serialized.exerciseArguments) {
      val compressedArgument = compressionStrategy.exerciseArgumentCompression
        .compress(argument, compressionMetrics.exerciseArgument)
      exerciseArguments += (EventId(transactionId, nodeId) -> compressedArgument)
    }

    for ((transactionId, nodeId, result) <- serialized.exerciseResults) {
      val compressedResult = compressionStrategy.exerciseResultCompression
        .compress(result, compressionMetrics.exerciseResult)
      exerciseResults += (EventId(transactionId, nodeId) -> compressedResult)
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
    private val creates = Map.newBuilder[Create, Instant]
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

    private def addCreate(create: (Create, Instant)): Unit =
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

    def add(timestamp: Instant)(event: (NodeId, Node)): Builder = {
      event match {
        case (nodeId, create: Create) =>
          addEventAndDisclosure(event)
          addVisibility(create.coid, blinding.disclosure(nodeId))
          addStakeholders(nodeId, create.stakeholders)
          addCreate(create -> timestamp)
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
        offsetStep: OffsetStep,
        divulgedContracts: Iterable[DivulgedContract],
    ): TransactionIndexing = {
      val created = creates.result()
      val archived = archives.result()
      val allCreatedContractIds = created.iterator.map(_._1.coid).toSet
      val allContractIds = allCreatedContractIds.union(archived)
      val netCreates = created.filterNot { case (create, _) => archived(create.coid) }
      val netArchives = archived.filterNot(allCreatedContractIds)
      val netDivulgedContracts = divulgedContracts.filterNot(c => allContractIds(c.contractId))
      val netTransactionVisibility =
        Relation.from(visibility.result()).view.filterKeys(!archived(_)).toMap
      val netVisibility = Relation.union(netTransactionVisibility, visibility(netDivulgedContracts))
      TransactionIndexing(
        events = EventsInfoBatch(
          List(
            transactionId -> EventsInfo(
              submitterInfo = submitterInfo,
              workflowId = workflowId,
              ledgerEffectiveTime = ledgerEffectiveTime,
              offset = offsetStep.offset,
              events = events.result(),
              archives = archived,
              stakeholders = stakeholders.result(),
              disclosure = disclosure.result(),
            )
          )
        ),
        contracts = ContractsInfo(
          netCreates = netCreates,
          archives = netArchives,
          divulgedContracts = netDivulgedContracts,
        ),
        contractWitnesses = ContractWitnessesInfo(
          archives = netArchives,
          netVisibility = netVisibility,
        ),
      )
    }
  }

  case class EventsInfo(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      ledgerEffectiveTime: Instant,
      offset: Offset,
      events: Vector[(NodeId, Node)],
      archives: Set[ContractId],
      stakeholders: WitnessRelation[NodeId],
      disclosure: WitnessRelation[NodeId],
  )

  final case class EventsInfoBatch(eventsInfo: List[(TransactionId, EventsInfo)])

  object EventsInfoBatch {
    val empty: EventsInfoBatch = EventsInfoBatch(List.empty)
    val combine: Combine[EventsInfoBatch] = (left, right) =>
      EventsInfoBatch(left.eventsInfo ::: right.eventsInfo)
  }

  case class ContractsInfo(
      netCreates: Map[Create, Instant],
      archives: Set[ContractId],
      divulgedContracts: Iterable[DivulgedContract],
  )

  object ContractsInfo {
    val empty: ContractsInfo = ContractsInfo(Map.empty, Set.empty, Iterable.empty)

    val combine: Combine[ContractsInfo] = (left, right) => {
      val archives = left.archives ++ right.archives
      val creates = left.netCreates ++ right.netCreates
      val netCreates = creates.filterNot { case (create, _) =>
        archives(create.coid)
      }
      val netDivulged =
        (left.divulgedContracts ++ right.divulgedContracts)
          .filterNot { divulged =>
            archives(divulged.contractId)
          }

      ContractsInfo(
        netCreates = netCreates,
        archives = archives,
        divulgedContracts = netDivulged,
      )
    }
  }

  case class ContractWitnessesInfo(
      archives: Set[ContractId],
      netVisibility: WitnessRelation[ContractId],
  )

  object ContractWitnessesInfo {
    val empty: ContractWitnessesInfo = ContractWitnessesInfo(Set.empty, Map.empty)

    val combine: Combine[ContractWitnessesInfo] = (left, right) => {
      val visibility = Relation.union(left.netVisibility, right.netVisibility)
      val archives = left.archives ++ right.archives
      val netVisibility = visibility.filterKeys(!archives(_))
      ContractWitnessesInfo(
        archives = archives,
        netVisibility = netVisibility,
      )
    }
  }

  final case class Serialized(
      createArguments: Vector[(TransactionId, NodeId, ContractId, Array[Byte])],
      divulgedContracts: Vector[(ContractId, Array[Byte])],
      createKeyValues: Vector[(TransactionId, NodeId, Array[Byte])],
      exerciseArguments: Vector[(TransactionId, NodeId, Array[Byte])],
      exerciseResults: Vector[(TransactionId, NodeId, Array[Byte])],
  ) {
    val createArgumentsByContract: Map[ContractId, Array[Byte]] = createArguments.map {
      case (_, _, contractId, arg) =>
        contractId -> arg
    }.toMap
  }

  object Compressed {

    case class Contracts(
        createArguments: Map[ContractId, Array[Byte]],
        createArgumentsCompression: Compression.Algorithm,
    )

    case class Events(
        createArguments: Map[EventId, Array[Byte]],
        createArgumentsCompression: Compression.Algorithm,
        createKeyValues: Map[EventId, Array[Byte]],
        createKeyValueCompression: Compression.Algorithm,
        exerciseArguments: Map[EventId, Array[Byte]],
        exerciseArgumentsCompression: Compression.Algorithm,
        exerciseResults: Map[EventId, Array[Byte]],
        exerciseResultsCompression: Compression.Algorithm,
    ) {
      def assertCreate(eventId: EventId): (Array[Byte], Option[Array[Byte]]) = {
        assert(
          createArguments.contains(eventId),
          s"event $eventId is not a create event",
        )
        (createArguments(eventId), createKeyValues.get(eventId))
      }

      def assertExercise(eventId: EventId): (Array[Byte], Option[Array[Byte]]) = {
        assert(
          exerciseArguments.contains(eventId),
          s"Node $eventId is not an exercise event",
        )
        (exerciseArguments(eventId), exerciseResults.get(eventId))
      }
    }
  }

  case class Compressed(
      contracts: Compressed.Contracts,
      events: Compressed.Events,
  )
  def from(
      blindingInfo: BlindingInfo,
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offset: OffsetStep,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[DivulgedContract],
  ): TransactionIndexing =
    transaction
      .fold(new Builder(blindingInfo))(_.add(ledgerEffectiveTime)(_))
      .build(
        submitterInfo,
        workflowId,
        transactionId,
        ledgerEffectiveTime,
        offset,
        divulgedContracts,
      )
}
