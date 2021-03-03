// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.ledger.participant.state.v1.{
  CommittedTransaction,
  ContractInst,
  DivulgedContract,
  Offset,
  SubmitterInfo,
}
import com.daml.lf.ledger.EventId
import com.daml.lf.transaction.BlindingInfo

final case class TransactionIndexing(
    transaction: TransactionIndexing.TransactionInfo,
    events: TransactionIndexing.EventsInfo,
)

object TransactionIndexing {

  def serialize(
      translation: LfValueTranslation,
      transactionId: TransactionId,
      events: Vector[(NodeId, Node)],
      divulgences: Iterable[DivulgedContract],
  ): Serialized = {

    val createArguments = Vector.newBuilder[(NodeId, ContractId, Array[Byte])]
    val divulgedContracts = Vector.newBuilder[(ContractId, Array[Byte])]
    val createKeyValues = Vector.newBuilder[(NodeId, Array[Byte])]
    val createKeyHashes = Vector.newBuilder[(NodeId, Array[Byte])]
    val exerciseArguments = Vector.newBuilder[(NodeId, Array[Byte])]
    val exerciseResults = Vector.newBuilder[(NodeId, Array[Byte])]

    for ((nodeId, event) <- events) {
      val eventId = EventId(transactionId, nodeId)
      event match {
        case create: Create =>
          val (createArgument, createKeyValue) = translation.serialize(eventId, create)
          createArguments += ((nodeId, create.coid, createArgument))
          createKeyValue.foreach(key => createKeyValues += ((nodeId, key)))
          createKeyValue.foreach(_ =>
            createKeyHashes += (
              (
                nodeId,
                create.key
                  .map(convertLfValueKey(create.templateId, _))
                  .map(_.hash.bytes.toByteArray)
                  .orNull,
              ),
            )
          )
        case exercise: Exercise =>
          val (exerciseArgument, exerciseResult) = translation.serialize(eventId, exercise)
          exerciseArguments += ((nodeId, exerciseArgument))
          exerciseResult.foreach(result => exerciseResults += ((nodeId, result)))
        case _ => throw new UnexpectedNodeException(nodeId, transactionId)
      }
    }

    for (DivulgedContract(contractId, contractInst) <- divulgences) {
      val serializedCreateArgument = translation.serialize(contractId, contractInst.arg)
      divulgedContracts += ((contractId, serializedCreateArgument))
    }

    Serialized(
      createArguments = createArguments.result(),
      divulgedContracts = divulgedContracts.result(),
      createKeyValues = createKeyValues.result(),
      createKeyHashes = createKeyHashes.result(),
      exerciseArguments = exerciseArguments.result(),
      exerciseResults = exerciseResults.result(),
    )

  }

  def compress(
      serialized: Serialized,
      compressionStrategy: CompressionStrategy,
  ): Compressed = {

    val createArgumentsByContract = Map.newBuilder[ContractId, Array[Byte]]
    val createArguments = Map.newBuilder[NodeId, Array[Byte]]
    val createKeyValues = Map.newBuilder[NodeId, Array[Byte]]
    val createKeyHashes = Map.newBuilder[NodeId, Array[Byte]]
    val exerciseArguments = Map.newBuilder[NodeId, Array[Byte]]
    val exerciseResults = Map.newBuilder[NodeId, Array[Byte]]

    for ((contractId, argument) <- serialized.divulgedContracts) {
      val compressedArgument =
        compressionStrategy.createArgumentCompression
          .compress(argument)
      createArgumentsByContract += ((contractId, compressedArgument))
    }

    for ((nodeId, contractId, argument) <- serialized.createArguments) {
      val compressedArgument = compressionStrategy.createArgumentCompression
        .compress(argument)
      createArguments += ((nodeId, compressedArgument))
      createArgumentsByContract += ((contractId, compressedArgument))
    }

    for ((nodeId, key) <- serialized.createKeyValues) {
      val compressedKey = compressionStrategy.createKeyValueCompression
        .compress(key)
      createKeyValues += ((nodeId, compressedKey))
    }

    for ((nodeId, key) <- serialized.createKeyHashes) {
      createKeyHashes += ((nodeId, key))
    }

    for ((nodeId, argument) <- serialized.exerciseArguments) {
      val compressedArgument = compressionStrategy.exerciseArgumentCompression
        .compress(argument)
      exerciseArguments += ((nodeId, compressedArgument))
    }

    for ((nodeId, result) <- serialized.exerciseResults) {
      val compressedResult = compressionStrategy.exerciseResultCompression
        .compress(result)
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
        createKeyHashes = createKeyHashes.result(),
        exerciseArguments = exerciseArguments.result(),
        exerciseArgumentsCompression = compressionStrategy.exerciseArgumentCompression,
        exerciseResults = exerciseResults.result(),
        exerciseResultsCompression = compressionStrategy.exerciseResultCompression,
      ),
    )
  }

  private class Builder(blinding: BlindingInfo) {

    private val events = Vector.newBuilder[(NodeId, Node)]
    private val stakeholders = Map.newBuilder[NodeId, Set[Party]]
    private val disclosure = Map.newBuilder[NodeId, Set[Party]]

    private def addEventAndDisclosure(event: (NodeId, Node)): Unit = {
      events += event
      disclosure += ((event._1, blinding.disclosure(event._1)))
    }

    private def addStakeholders(nodeId: NodeId, parties: Set[Party]): Unit =
      stakeholders += ((nodeId, parties))

    def add(event: (NodeId, Node)): Builder = {
      event match {
        case (nodeId, create: Create) =>
          addEventAndDisclosure(event)
          addStakeholders(nodeId, create.stakeholders)
        case (nodeId, exercise: Exercise) =>
          addEventAndDisclosure(event)
          if (exercise.consuming) {
            addStakeholders(nodeId, exercise.stakeholders)
          } else {
            addStakeholders(nodeId, Set.empty)
          }
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
    ): TransactionIndexing = {
      val divulgedContractIndex = divulgedContracts
        .map(divulgedContract => divulgedContract.contractId -> divulgedContract)
        .toMap
      val divulgedContractInfos = blinding.divulgence.map { case (contractId, visibleToParties) =>
        DivulgedContractInfo(
          contractId = contractId,
          contractInst = divulgedContractIndex.get(contractId).map(_.contractInst),
          visibility = visibleToParties,
        )
      }
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
          stakeholders = stakeholders.result(),
          disclosure = disclosure.result(),
          divulgedContractInfos = divulgedContractInfos,
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
      stakeholders: WitnessRelation[NodeId],
      disclosure: WitnessRelation[NodeId],
      divulgedContractInfos: Iterable[DivulgedContractInfo],
  )

  // most involved change: divulgence is now an event instead encoded in contract/contract_witness tables
  // also we generate these events directly as part of the other events, so we need to land the necessery
  // data in the EventsTablePostgresql
  case class DivulgedContractInfo(
      contractId: ContractId,
      contractInst: Option[
        ContractInst
      ], // this we do not necessarily have: for public KV ledgers the divulged contract list is empty, since all contracts must be at all ledger as create nodes already
      visibility: Set[Party],
  )

  final case class Serialized(
      createArguments: Vector[(NodeId, ContractId, Array[Byte])],
      divulgedContracts: Vector[(ContractId, Array[Byte])],
      createKeyValues: Vector[(NodeId, Array[Byte])],
      createKeyHashes: Vector[(NodeId, Array[Byte])],
      exerciseArguments: Vector[(NodeId, Array[Byte])],
      exerciseResults: Vector[(NodeId, Array[Byte])],
  ) {
    val createArgumentsByContract: Map[ContractId, Array[Byte]] = createArguments.map {
      case (_, contractId, arg) =>
        contractId -> arg
    }.toMap
  }

  object Compressed {

    final case class Contracts(
        createArguments: Map[ContractId, Array[Byte]],
        createArgumentsCompression: FieldCompressionStrategy,
    )

    final case class Events(
        createArguments: Map[NodeId, Array[Byte]],
        createArgumentsCompression: FieldCompressionStrategy,
        createKeyValues: Map[NodeId, Array[Byte]],
        createKeyValueCompression: FieldCompressionStrategy,
        createKeyHashes: Map[NodeId, Array[Byte]],
        exerciseArguments: Map[NodeId, Array[Byte]],
        exerciseArgumentsCompression: FieldCompressionStrategy,
        exerciseResults: Map[NodeId, Array[Byte]],
        exerciseResultsCompression: FieldCompressionStrategy,
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
