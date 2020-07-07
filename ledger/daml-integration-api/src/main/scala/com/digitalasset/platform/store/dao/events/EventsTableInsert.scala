// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant
import anorm.{BatchSql, NamedParameter}
import com.daml.ledger.participant.state.v1.{CommittedTransaction, Offset, SubmitterInfo}
import com.daml.ledger._
import com.daml.platform.store.Conversions._

private[events] trait EventsTableInsert { this: EventsTable =>

  private def insertEvent(
      commonPrefix: Seq[(String, String)],
      columnNameAndValues: (String, String)*): String = {
    val (columns, values) = (commonPrefix ++ columnNameAndValues).unzip
    s"insert into participant_events(${columns.mkString(", ")}) values (${values.mkString(", ")})"
  }

  private def commonPrefix = Seq(
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
    "flat_event_witnesses" -> "{flat_event_witnesses}",
    "tree_event_witnesses" -> "{tree_event_witnesses}",
  )

  private val insertEvent: String =
    insertEvent(
      commonPrefix,
      // create event values
      "create_argument" -> "{create_argument}",
      "create_signatories" -> "{create_signatories}",
      "create_observers" -> "{create_observers}",
      "create_agreement_text" -> "{create_agreement_text}",
      "create_consumed_at" -> "null",
      "create_key_value" -> "{create_key_value}",
      // exercise event values
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
      events: Option[RawBatch],
      archives: Option[BatchSql],
  ) {
    def applySerialization(
        lfValueTranslation: LfValueTranslation,
    ): SerializedBatches =
      new SerializedBatches(
        events = events.map(_.applySerialization(lfValueTranslation)),
        archives = archives,
      )
  }

  final class SerializedBatches(
      events: Option[Vector[Vector[NamedParameter]]],
      archives: Option[BatchSql],
  ) {
    def applyBatching(): PreparedBatches =
      new PreparedBatches(
        events = events.map(cs => BatchSql(insertEvent, cs.head, cs.tail: _*)),
        archives = archives,
      )
  }

  final class PreparedBatches(
      events: Option[BatchSql],
      archives: Option[BatchSql],
  ) {
    def isEmpty: Boolean = events.isEmpty && archives.isEmpty
    def foreach[U](f: BatchSql => U): Unit = {
      events.foreach(f)
      archives.foreach(f)
    }
  }

  private case class AccumulatingBatches(
      events: Vector[RawBatch.Event[RawBatch.Event.Specific]],
      archives: Vector[Vector[NamedParameter]],
  ) {

    def add(event: RawBatch.Event[RawBatch.Event.Specific]): AccumulatingBatches =
      copy(events = this.events :+ event)

    def add(archive: Vector[NamedParameter]): AccumulatingBatches =
      copy(archives = archives :+ archive)

    private def prepareRawNonEmpty(
        query: String,
        params: Vector[RawBatch.Event[_]],
    ): Option[RawBatch] =
      if (params.nonEmpty) Some(new RawBatch(query, params)) else None

    private def prepareNonEmpty(
        query: String,
        params: Vector[Vector[NamedParameter]],
    ): Option[BatchSql] =
      if (params.nonEmpty) Some(BatchSql(query, params.head, params.tail: _*)) else None

    def prepare: RawBatches =
      new RawBatches(
        prepareRawNonEmpty(insertEvent, events),
        prepareNonEmpty(updateArchived, archives),
      )

  }

  private object AccumulatingBatches {
    val empty: AccumulatingBatches = AccumulatingBatches(
      events = Vector.empty,
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
      transaction: CommittedTransaction,
      flatWitnesses: WitnessRelation[NodeId],
      treeWitnesses: WitnessRelation[NodeId],
  ): RawBatches = {
    def event[Sp <: RawBatch.Event.Specific](nodeId: NodeId, sp: Sp) =
      new RawBatch.Event(
        applicationId = submitterInfo.map(_.applicationId),
        workflowId = workflowId,
        commandId = submitterInfo.map(_.commandId),
        transactionId = transactionId,
        nodeId = nodeId,
        submitter = submitterInfo.map(_.submitter),
        ledgerEffectiveTime = ledgerEffectiveTime,
        offset = offset,
        flatWitnesses = flatWitnesses getOrElse (nodeId, Set.empty),
        treeWitnesses = treeWitnesses getOrElse (nodeId, Set.empty),
        specific = sp,
      )

    transaction
      .fold(AccumulatingBatches.empty) {
        case (batches, (nodeId, node: Create)) =>
          batches.add(event(nodeId, new RawBatch.Event.Created(node)))
        case (batches, (nodeId, node: Exercise)) =>
          val batchWithExercises =
            batches.add(event(nodeId, new RawBatch.Event.Exercised(node)))
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

}
