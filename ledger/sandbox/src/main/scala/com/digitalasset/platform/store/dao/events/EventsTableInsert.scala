// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import anorm.{BatchSql, NamedParameter}
import com.daml.ledger.participant.state.v1.{Offset, SubmitterInfo}
import com.daml.ledger._
import com.daml.platform.events.EventIdFormatter.fromTransactionId
import com.daml.platform.store.Conversions._
import com.daml.platform.store.serialization.ValueSerializer.{serializeValue => serialize}

private[events] trait EventsTableInsert { this: EventsTable =>

  private def cantSerialize(attribute: String, forContract: ContractId): String =
    s"Cannot serialize $attribute for ${forContract.coid}"

  private def serializeCreateArgOrThrow(node: Create): Array[Byte] =
    serialize(
      value = node.coinst.arg,
      errorContext = cantSerialize(attribute = "create argument", forContract = node.coid),
    )

  private def serializeNullableKeyOrThrow(node: Create): Option[Array[Byte]] =
    node.key.map(
      k =>
        serialize(
          value = k.key,
          errorContext = cantSerialize(attribute = "key", forContract = node.coid),
      ))

  private def serializeExerciseArgOrThrow(node: Exercise): Array[Byte] =
    serialize(
      value = node.chosenValue,
      errorContext = cantSerialize(attribute = "exercise argument", forContract = node.targetCoid),
    )

  private def serializeNullableExerciseResultOrThrow(node: Exercise): Option[Array[Byte]] =
    node.exerciseResult.map(exerciseResult =>
      serialize(
        value = exerciseResult,
        errorContext = cantSerialize(attribute = "exercise result", forContract = node.targetCoid),
    ))

  private def insertEvent(columnNameAndValues: (String, String)*): String = {
    val (columns, values) = columnNameAndValues.unzip
    s"insert into participant_events(${columns.mkString(", ")}) values (${values.mkString(", ")})"
  }

  private val insertCreate: String =
    insertEvent(
      "event_id" -> "{event_id}",
      "event_offset" -> "{event_offset}",
      "contract_id" -> "{contract_id}",
      "transaction_id" -> "{transaction_id}",
      "workflow_id" -> "{workflow_id}",
      "ledger_effective_time" -> "{ledger_effective_time}",
      "template_id" -> "{template_id}",
      "node_index" -> "{node_index}",
      "command_id" -> "{command_id}",
      "application_id" -> "{application_id}",
      "submitter" -> "{submitter}",
      "create_argument" -> "{create_argument}",
      "create_signatories" -> "{create_signatories}",
      "create_observers" -> "{create_observers}",
      "create_agreement_text" -> "{create_agreement_text}",
      "create_consumed_at" -> "null",
      "create_key_value" -> "{create_key_value}"
    )

  private def create(
      applicationId: Option[ApplicationId],
      workflowId: Option[WorkflowId],
      commandId: Option[CommandId],
      transactionId: TransactionId,
      nodeId: NodeId,
      submitter: Option[Party],
      ledgerEffectiveTime: Instant,
      offset: Offset,
      create: Create,
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "event_id" -> fromTransactionId(transactionId, nodeId),
      "event_offset" -> offset,
      "contract_id" -> create.coid.coid,
      "transaction_id" -> transactionId,
      "workflow_id" -> workflowId,
      "ledger_effective_time" -> ledgerEffectiveTime,
      "template_id" -> create.coinst.template,
      "node_index" -> nodeId.index,
      "command_id" -> commandId,
      "application_id" -> applicationId,
      "submitter" -> submitter,
      "create_argument" -> serializeCreateArgOrThrow(create),
      "create_signatories" -> create.signatories.toArray[String],
      "create_observers" -> create.stakeholders.diff(create.signatories).toArray[String],
      "create_agreement_text" -> Some(create.coinst.agreementText).filter(_.nonEmpty),
      "create_key_value" -> serializeNullableKeyOrThrow(create),
    )

  private val insertExercise =
    insertEvent(
      "event_id" -> "{event_id}",
      "event_offset" -> "{event_offset}",
      "contract_id" -> "{contract_id}",
      "transaction_id" -> "{transaction_id}",
      "workflow_id" -> "{workflow_id}",
      "ledger_effective_time" -> "{ledger_effective_time}",
      "template_id" -> "{template_id}",
      "node_index" -> "{node_index}",
      "command_id" -> "{command_id}",
      "application_id" -> "{application_id}",
      "submitter" -> "{submitter}",
      "exercise_consuming" -> "{exercise_consuming}",
      "exercise_choice" -> "{exercise_choice}",
      "exercise_argument" -> "{exercise_argument}",
      "exercise_result" -> "{exercise_result}",
      "exercise_actors" -> "{exercise_actors}",
      "exercise_child_event_ids" -> "{exercise_child_event_ids}"
    )

  private def exercise(
      applicationId: Option[ApplicationId],
      workflowId: Option[WorkflowId],
      commandId: Option[CommandId],
      transactionId: TransactionId,
      nodeId: NodeId,
      submitter: Option[Party],
      ledgerEffectiveTime: Instant,
      offset: Offset,
      exercise: Exercise,
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "event_id" -> fromTransactionId(transactionId, nodeId),
      "event_offset" -> offset,
      "contract_id" -> exercise.targetCoid.coid,
      "transaction_id" -> transactionId,
      "workflow_id" -> workflowId,
      "ledger_effective_time" -> ledgerEffectiveTime,
      "template_id" -> exercise.templateId,
      "node_index" -> nodeId.index,
      "command_id" -> commandId,
      "application_id" -> applicationId,
      "submitter" -> submitter,
      "exercise_consuming" -> exercise.consuming,
      "exercise_choice" -> exercise.choiceId,
      "exercise_argument" -> serializeExerciseArgOrThrow(exercise),
      "exercise_result" -> serializeNullableExerciseResultOrThrow(exercise),
      "exercise_actors" -> exercise.actingParties.toArray[String],
      "exercise_child_event_ids" -> exercise.children
        .map(fromTransactionId(transactionId, _))
        .toArray[String],
    )

  private val updateArchived =
    """update participant_events set create_consumed_at={consumed_at} where contract_id={contract_id} and create_argument is not null"""

  private def archive(
      contractId: ContractId,
      consumedAt: Offset,
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "consumed_at" -> consumedAt,
      "contract_id" -> contractId.coid,
    )

  sealed abstract case class PreparedBatches(
      creates: Option[BatchSql],
      exercises: Option[BatchSql],
      archives: Option[BatchSql],
  ) {
    final def isEmpty: Boolean = creates.isEmpty && exercises.isEmpty && archives.isEmpty
    final def foreach[U](f: BatchSql => U): Unit = {
      creates.foreach(f)
      exercises.foreach(f)
      archives.foreach(f)
    }
  }

  private case class AccumulatingBatches(
      creates: Vector[Vector[NamedParameter]],
      exercises: Vector[Vector[NamedParameter]],
      archives: Vector[Vector[NamedParameter]],
  ) {

    def addCreate(create: Vector[NamedParameter]): AccumulatingBatches =
      copy(creates = creates :+ create)

    def addExercise(exercise: Vector[NamedParameter]): AccumulatingBatches =
      copy(exercises = exercises :+ exercise)

    def addArchive(archive: Vector[NamedParameter]): AccumulatingBatches =
      copy(archives = archives :+ archive)

    private def prepareNonEmpty(
        query: String,
        params: Vector[Vector[NamedParameter]],
    ): Option[BatchSql] =
      if (params.nonEmpty) Some(BatchSql(query, params.head, params.tail: _*)) else None

    def prepare: PreparedBatches =
      new PreparedBatches(
        prepareNonEmpty(insertCreate, creates),
        prepareNonEmpty(insertExercise, exercises),
        prepareNonEmpty(updateArchived, archives),
      ) {}

  }

  private object AccumulatingBatches {
    val empty: AccumulatingBatches = AccumulatingBatches(
      creates = Vector.empty,
      exercises = Vector.empty,
      archives = Vector.empty,
    )
  }

  /**
    * @throws RuntimeException If a value cannot be serialized into an array of bytes
    */
  @throws[RuntimeException]
  def prepareBatchInsert(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: Transaction,
  ): PreparedBatches =
    transaction
      .fold(AccumulatingBatches.empty) {
        case (batches, (nodeId, node: Create)) =>
          batches.addCreate(
            create(
              applicationId = submitterInfo.map(_.applicationId),
              workflowId = workflowId,
              commandId = submitterInfo.map(_.commandId),
              transactionId = transactionId,
              nodeId = nodeId,
              submitter = submitterInfo.map(_.submitter),
              ledgerEffectiveTime = ledgerEffectiveTime,
              offset = offset,
              create = node,
            )
          )
        case (batches, (nodeId, node: Exercise)) =>
          val batchWithExercises =
            batches.addExercise(
              exercise(
                applicationId = submitterInfo.map(_.applicationId),
                workflowId = workflowId,
                commandId = submitterInfo.map(_.commandId),
                transactionId = transactionId,
                nodeId = nodeId,
                submitter = submitterInfo.map(_.submitter),
                ledgerEffectiveTime = ledgerEffectiveTime,
                offset = offset,
                exercise = node,
              )
            )
          if (node.consuming) {
            batchWithExercises.addArchive(
              archive(
                contractId = node.targetCoid,
                consumedAt = offset,
              )
            )
          } else {
            batchWithExercises
          }
        case (batches, _) =>
          batches // ignore any event which is neither a create nor an exercise
      }
      .prepare

}
