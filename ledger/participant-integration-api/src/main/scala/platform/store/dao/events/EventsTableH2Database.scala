// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.{BatchSql, NamedParameter}
import com.daml.ledger.{EventId, TransactionId}
import com.daml.ledger.participant.state.v1.{CommittedTransaction, TransactionId, Offset, SubmitterInfo, WorkflowId}
import com.daml.platform.store.Conversions._

object EventsTableH2Database extends EventsTable {

  final class Batches(insertEvents: Option[BatchSql], updateArchives: Option[BatchSql])
      extends EventsTable.Batches {
    override def execute()(implicit connection: Connection): Unit = {
      insertEvents.foreach(_.execute())
      updateArchives.foreach(_.execute())
    }
    override def execute(submitterInfo: Option[SubmitterInfo], offset: Offset, transaction: CommittedTransaction, recordTime: Instant, transactionId: TransactionId)(implicit connection: Connection): Unit = ()
  }

  private val insertEvent: String = {
    val (columns, values) = Seq(
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
      "submitters" -> "{submitters}",
      "flat_event_witnesses" -> "{flat_event_witnesses}",
      "tree_event_witnesses" -> "{tree_event_witnesses}",
      "create_argument" -> "{create_argument}",
      "create_signatories" -> "{create_signatories}",
      "create_observers" -> "{create_observers}",
      "create_agreement_text" -> "{create_agreement_text}",
      "create_consumed_at" -> "null", // Every inserted contract starts as not consumed
      "create_key_value" -> "{create_key_value}",
      "exercise_consuming" -> "{exercise_consuming}",
      "exercise_choice" -> "{exercise_choice}",
      "exercise_argument" -> "{exercise_argument}",
      "exercise_result" -> "{exercise_result}",
      "exercise_actors" -> "{exercise_actors}",
      "exercise_child_event_ids" -> "{exercise_child_event_ids}"
    ).unzip
    s"insert into participant_events(${columns.mkString(", ")}) values (${values.mkString(", ")})"
  }

  private def transaction(
      offset: Offset,
      transactionId: TransactionId,
      workflowId: Option[WorkflowId],
      ledgerEffectiveTime: Instant,
      submitterInfo: Option[SubmitterInfo],
      events: Vector[(NodeId, Node)],
      stakeholders: WitnessRelation[NodeId],
      disclosure: WitnessRelation[NodeId],
      createArguments: Map[NodeId, Array[Byte]],
      createKeyValues: Map[NodeId, Array[Byte]],
      exerciseArguments: Map[NodeId, Array[Byte]],
      exerciseResults: Map[NodeId, Array[Byte]],
  ): Vector[Vector[NamedParameter]] = {
    val shared =
      Vector[NamedParameter](
        "event_offset" -> offset,
        "transaction_id" -> transactionId,
        "workflow_id" -> workflowId,
        "ledger_effective_time" -> ledgerEffectiveTime,
        "command_id" -> submitterInfo.map(_.commandId),
        "application_id" -> submitterInfo.map(_.applicationId),
        "submitters" -> Party.Array(submitterInfo.map(_.actAs).getOrElse(List.empty): _*),
      )
    for ((nodeId, node) <- events)
      yield
        shared ++ event(
          transactionId,
          nodeId,
          stakeholders(nodeId),
          disclosure(nodeId),
          node,
          createArguments.get(nodeId),
          createKeyValues.get(nodeId),
          exerciseArguments.get(nodeId),
          exerciseResults.get(nodeId),
        )
  }

  private def event(
      transactionId: TransactionId,
      nodeId: NodeId,
      stakeholders: Set[Party],
      disclosure: Set[Party],
      node: Node,
      createArgument: Option[Array[Byte]],
      createKeyValue: Option[Array[Byte]],
      exerciseArgument: Option[Array[Byte]],
      exerciseResult: Option[Array[Byte]],
  ): Vector[NamedParameter] = {
    val shared =
      Vector[NamedParameter](
        "event_id" -> EventId(transactionId, nodeId).toLedgerString,
        "node_index" -> nodeId.index,
        "flat_event_witnesses" -> Party.Array(stakeholders.toSeq: _*),
        "tree_event_witnesses" -> Party.Array(disclosure.toSeq: _*),
      )
    val nonShared =
      node match {
        case event: Create => create(event, createArgument.get, createKeyValue)
        case event: Exercise => exercise(event, transactionId, exerciseArgument.get, exerciseResult)
        case _ => throw new UnexpectedNodeException(nodeId, transactionId)
      }
    shared ++ nonShared
  }

  private def create(
      event: Create,
      argument: Array[Byte],
      key: Option[Array[Byte]],
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "contract_id" -> event.coid.coid,
      "template_id" -> event.coinst.template,
      "create_argument" -> argument,
      "create_signatories" -> event.signatories.toArray[String],
      "create_observers" -> event.stakeholders.diff(event.signatories).toArray[String],
      "create_agreement_text" -> event.coinst.agreementText,
      "create_key_value" -> key,
    ) ++ emptyExerciseFields

  private def exercise(
      event: Exercise,
      transactionId: TransactionId,
      argument: Array[Byte],
      result: Option[Array[Byte]],
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "contract_id" -> event.targetCoid,
      "template_id" -> event.templateId,
      "exercise_consuming" -> event.consuming,
      "exercise_choice" -> event.choiceId,
      "exercise_argument" -> argument,
      "exercise_result" -> result,
      "exercise_actors" -> event.actingParties.toArray[String],
      "exercise_child_event_ids" -> event.children
        .map(EventId(transactionId, _).toLedgerString)
        .toArray[String],
    ) ++ emptyCreateFields

  private val emptyCreateFields = Vector[NamedParameter](
    "create_argument" -> Option.empty[Array[Byte]],
    "create_signatories" -> Option.empty[Array[String]],
    "create_observers" -> Option.empty[Array[String]],
    "create_agreement_text" -> Option.empty[String],
    "create_key_value" -> Option.empty[Array[Byte]],
  )

  private val emptyExerciseFields = Vector[NamedParameter](
    "exercise_consuming" -> Option.empty[Boolean],
    "exercise_choice" -> Option.empty[String],
    "exercise_argument" -> Option.empty[Array[Byte]],
    "exercise_result" -> Option.empty[Array[Byte]],
    "exercise_actors" -> Option.empty[Array[String]],
    "exercise_child_event_ids" -> Option.empty[Array[String]],
  )

  private val updateArchived =
    """update participant_events set create_consumed_at={consumed_at} where contract_id={contract_id} and create_argument is not null"""

  private def archive(consumedAt: Offset)(contractId: ContractId): Vector[NamedParameter] =
    Vector[NamedParameter](
      "consumed_at" -> consumedAt,
      "contract_id" -> contractId.coid,
    )

  def toExecutables(
      tx: TransactionIndexing.TransactionInfo,
      info: TransactionIndexing.EventsInfo,
      serialized: TransactionIndexing.Serialized,
  ): EventsTable.Batches = {

    val events = transaction(
      offset = tx.offset,
      transactionId = tx.transactionId,
      workflowId = tx.workflowId,
      ledgerEffectiveTime = tx.ledgerEffectiveTime,
      submitterInfo = tx.submitterInfo,
      events = info.events,
      stakeholders = info.stakeholders,
      disclosure = info.disclosure,
      createArguments = serialized.createArguments,
      createKeyValues = serialized.createKeyValues,
      exerciseArguments = serialized.exerciseArguments,
      exerciseResults = serialized.exerciseResults,
    )

    val archivals =
      info.archives.iterator.map(archive(tx.offset)).toList

    new Batches(
      insertEvents = batch(insertEvent, events),
      updateArchives = batch(updateArchived, archivals),
    )

  }
}
