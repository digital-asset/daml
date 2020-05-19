// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import anorm.{BatchSql, NamedParameter}
import com.daml.ledger.participant.state.v1.{Offset, SubmitterInfo}
import com.daml.ledger._
import com.daml.platform.store.Conversions._

private[events] trait EventsTableInsert { this: EventsTable =>

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

  final class RawBatches private[EventsTableInsert] (
      creates: Option[RawBatch],
      exercises: Option[RawBatch],
      archives: Option[BatchSql],
  ) {
    def applySerialization(): SerializedBatches =
      new SerializedBatches(
        creates = creates.map(_.applySerialization()),
        exercises = exercises.map(_.applySerialization()),
        archives = archives,
      )
  }

  final class SerializedBatches(
      creates: Option[Vector[Vector[NamedParameter]]],
      exercises: Option[Vector[Vector[NamedParameter]]],
      archives: Option[BatchSql],
  ) {
    def applyBatching(): PreparedBatches =
      new PreparedBatches(
        creates = creates.map(cs => BatchSql(insertCreate, cs.head, cs.tail: _*)),
        exercises = exercises.map(es => BatchSql(insertExercise, es.head, es.tail: _*)),
        archives = archives,
      )
  }

  final class PreparedBatches(
      creates: Option[BatchSql],
      exercises: Option[BatchSql],
      archives: Option[BatchSql],
  ) {
    def isEmpty: Boolean = creates.isEmpty && exercises.isEmpty && archives.isEmpty
    def foreach[U](f: BatchSql => U): Unit = {
      creates.foreach(f)
      exercises.foreach(f)
      archives.foreach(f)
    }
  }

  private case class AccumulatingBatches(
      creates: Vector[RawBatch.Event.Created],
      exercises: Vector[RawBatch.Event.Exercised],
      archives: Vector[Vector[NamedParameter]],
  ) {

    def add(create: RawBatch.Event.Created): AccumulatingBatches =
      copy(creates = creates :+ create)

    def add(exercise: RawBatch.Event.Exercised): AccumulatingBatches =
      copy(exercises = exercises :+ exercise)

    def add(archive: Vector[NamedParameter]): AccumulatingBatches =
      copy(archives = archives :+ archive)

    private def prepareRawNonEmpty(
        query: String,
        params: Vector[RawBatch.Event],
    ): Option[RawBatch] =
      if (params.nonEmpty) Some(new RawBatch(query, params)) else None

    private def prepareNonEmpty(
        query: String,
        params: Vector[Vector[NamedParameter]],
    ): Option[BatchSql] =
      if (params.nonEmpty) Some(BatchSql(query, params.head, params.tail: _*)) else None

    def prepare: RawBatches =
      new RawBatches(
        prepareRawNonEmpty(insertCreate, creates),
        prepareRawNonEmpty(insertExercise, exercises),
        prepareNonEmpty(updateArchived, archives),
      )

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
  ): RawBatches =
    transaction
      .fold(AccumulatingBatches.empty) {
        case (batches, (nodeId, node: Create)) =>
          batches.add(
            new RawBatch.Event.Created(
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
            batches.add(
              new RawBatch.Event.Exercised(
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
            batchWithExercises.add(
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
