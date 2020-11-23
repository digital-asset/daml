// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.{Connection, PreparedStatement}
import java.time.Instant

import anorm.{BatchSql, NamedParameter, Row, SimpleSql, SqlStringInterpolation, ToStatement}
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.ledger.EventId
import com.daml.platform.store.Conversions._

object EventsTablePostgresql extends EventsTable {

  /**
    * Insertions are represented by a single statement made of nested arrays, one per column, instead of JDBC batches.
    * This leverages a PostgreSQL-specific feature known as "array unnesting", which has shown to be considerable
    * faster than using JDBC batches.
    */
  final class Batches(
      insertEvents: Option[SimpleSql[Row]],
      updateArchives: Option[BatchSql],
  ) extends EventsTable.Batches {
    override def execute()(implicit connection: Connection): Unit = {
      insertEvents.foreach(_.execute())
      updateArchives.foreach(_.execute())
    }
  }

  private val updateArchived =
    """update participant_events set create_consumed_at={consumed_at} where contract_id={contract_id} and create_argument is not null"""

  private def archive(consumedAt: Offset)(contractId: ContractId): Vector[NamedParameter] =
    Vector[NamedParameter](
      "consumed_at" -> consumedAt,
      "contract_id" -> contractId.coid,
    )

  override def toExecutables(
      tx: TransactionIndexing.TransactionInfo,
      info: TransactionIndexing.EventsInfo,
      serialized: TransactionIndexing.Serialized,
  ): EventsTable.Batches = {
    val capacity = info.events.size
    val eventIds = Array.fill(capacity)(null.asInstanceOf[String])
    val eventOffsets = Array.fill(capacity)(tx.offset.toByteArray)
    val contractIds = Array.fill(capacity)(null.asInstanceOf[String])
    val transactionIds = Array.fill(capacity)(tx.transactionId.asInstanceOf[String])
    val workflowIds = Array.fill(capacity)(tx.workflowId.map(_.asInstanceOf[String]).orNull)
    val ledgerEffectiveTimes = Array.fill(capacity)(tx.ledgerEffectiveTime)
    val templateIds = Array.fill(capacity)(null.asInstanceOf[String])
    val nodeIndexes = Array.fill(capacity)(null.asInstanceOf[Int])
    val commandIds =
      Array.fill(capacity)(tx.submitterInfo.map(_.commandId.asInstanceOf[String]).orNull)
    val applicationIds =
      Array.fill(capacity)(tx.submitterInfo.map(_.applicationId.asInstanceOf[String]).orNull)
    val submitters = Array.fill(capacity)(
      tx.submitterInfo.map(_.singleSubmitterOrThrow().asInstanceOf[String]).orNull)
    val flatEventWitnesses = Array.fill(capacity)(null.asInstanceOf[String])
    val treeEventWitnesses = Array.fill(capacity)(null.asInstanceOf[String])
    val createArguments = Array.fill(capacity)(null.asInstanceOf[Array[Byte]])
    val createSignatories = Array.fill(capacity)(null.asInstanceOf[String])
    val createObservers = Array.fill(capacity)(null.asInstanceOf[String])
    val createAgreementTexts = Array.fill(capacity)(null.asInstanceOf[String])
    val createConsumedAt = Array.fill(capacity)(null.asInstanceOf[Array[Byte]])
    val createKeyValues = Array.fill(capacity)(null.asInstanceOf[Array[Byte]])
    val exerciseConsuming = Array.fill(capacity)(null.asInstanceOf[Boolean])
    val exerciseChoices = Array.fill(capacity)(null.asInstanceOf[String])
    val exerciseArguments = Array.fill(capacity)(null.asInstanceOf[Array[Byte]])
    val exerciseResults = Array.fill(capacity)(null.asInstanceOf[Array[Byte]])
    val exerciseActors = Array.fill(capacity)(null.asInstanceOf[String])
    val exerciseChildEventIds = Array.fill(capacity)(null.asInstanceOf[String])

    for (((nodeId, node), i) <- info.events.zipWithIndex) {
      node match {
        case create: Create =>
          val eventId = EventId(tx.transactionId, nodeId)
          contractIds(i) = create.coid.coid
          templateIds(i) = create.coinst.template.toString
          eventIds(i) = eventId.toLedgerString
          nodeIndexes(i) = nodeId.index
          flatEventWitnesses(i) = info.stakeholders.getOrElse(nodeId, Set.empty).mkString("|")
          treeEventWitnesses(i) = info.disclosure.getOrElse(nodeId, Set.empty).mkString("|")
          createArguments(i) = serialized.createArguments(nodeId)
          createSignatories(i) = create.signatories.mkString("|")
          createObservers(i) = create.stakeholders.diff(create.signatories).mkString("|")
          if (create.coinst.agreementText.nonEmpty) {
            createAgreementTexts(i) = create.coinst.agreementText
          }
          createKeyValues(i) = serialized.createKeyValues.get(nodeId).orNull
        case exercise: Exercise =>
          val eventId = EventId(tx.transactionId, nodeId)
          contractIds(i) = exercise.targetCoid.coid
          templateIds(i) = exercise.templateId.toString
          eventIds(i) = eventId.toLedgerString
          nodeIndexes(i) = nodeId.index
          flatEventWitnesses(i) = info.stakeholders.getOrElse(nodeId, Set.empty).mkString("|")
          treeEventWitnesses(i) = info.disclosure.getOrElse(nodeId, Set.empty).mkString("|")
          exerciseConsuming(i) = exercise.consuming
          exerciseChoices(i) = exercise.choiceId
          exerciseArguments(i) = serialized.exerciseArguments(nodeId)
          exerciseResults(i) = serialized.exerciseResults.get(nodeId).orNull
          exerciseActors(i) = exercise.actingParties.mkString("|")
          exerciseChildEventIds(i) = exercise.children
            .map(EventId(tx.transactionId, _).toLedgerString)
            .iterator
            .mkString("|")
        case _ => throw new UnexpectedNodeException(nodeId, tx.transactionId)
      }
    }

    val inserts = insertEvents(
      eventIds,
      eventOffsets,
      contractIds,
      transactionIds,
      workflowIds,
      ledgerEffectiveTimes,
      templateIds,
      nodeIndexes,
      commandIds,
      applicationIds,
      submitters,
      flatEventWitnesses,
      treeEventWitnesses,
      createArguments,
      createSignatories,
      createObservers,
      createAgreementTexts,
      createConsumedAt,
      createKeyValues,
      exerciseConsuming,
      exerciseChoices,
      exerciseArguments,
      exerciseResults,
      exerciseActors,
      exerciseChildEventIds
    )

    val archivals =
      info.archives.iterator.map(archive(tx.offset)).toList

    new Batches(
      insertEvents = Some(inserts),
      updateArchives = batch(updateArchived, archivals),
    )
  }

  // Specific for PostgreSQL parallel unnesting insertions

  private implicit object BooleanArrayToStatement extends ToStatement[Array[Boolean]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Boolean]): Unit = {
      val conn = s.getConnection
      val bs = conn.createArrayOf("BOOLEAN", v.map(Boolean.box))
      s.setArray(index, bs)
    }
  }

  private implicit object IntArrayToStatement extends ToStatement[Array[Int]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Int]): Unit = {
      val conn = s.getConnection
      val is = conn.createArrayOf("INT", v.map(Int.box))
      s.setArray(index, is)
    }
  }

  private implicit object ByteArrayArrayToStatement extends ToStatement[Array[Array[Byte]]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Array[Byte]]): Unit =
      s.setObject(index, v)
  }

  private implicit object InstantArrayToStatement extends ToStatement[Array[Instant]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Instant]): Unit = {
      val conn = s.getConnection
      val ts = conn.createArrayOf("TIMESTAMP", v.map(java.sql.Timestamp.from))
      s.setArray(index, ts)
    }
  }

  private def insertEvents(
      eventIds: Array[String],
      eventOffsets: Array[Array[Byte]],
      contractIds: Array[String],
      transactionIds: Array[String],
      workflowIds: Array[String],
      ledgerEffectiveTimes: Array[Instant],
      templateIds: Array[String],
      nodeIndexes: Array[Int],
      commandIds: Array[String],
      applicationIds: Array[String],
      submitters: Array[String],
      flatEventWitnesses: Array[String],
      treeEventWitnesses: Array[String],
      createArguments: Array[Array[Byte]],
      createSignatories: Array[String],
      createObservers: Array[String],
      createAgreementTexts: Array[String],
      createConsumedAt: Array[Array[Byte]],
      createKeyValues: Array[Array[Byte]],
      exerciseConsuming: Array[Boolean],
      exerciseChoices: Array[String],
      exerciseArguments: Array[Array[Byte]],
      exerciseResults: Array[Array[Byte]],
      exerciseActors: Array[String],
      exerciseChildEventIds: Array[String],
  ) =
    SQL"""insert into participant_events(
           event_id, event_offset, contract_id, transaction_id, workflow_id, ledger_effective_time, template_id, node_index, command_id, application_id, submitter, flat_event_witnesses, tree_event_witnesses,
           create_argument, create_signatories, create_observers, create_agreement_text, create_consumed_at, create_key_value,
           exercise_consuming, exercise_choice, exercise_argument, exercise_result, exercise_actors, exercise_child_event_ids
         )
         select
           event_id, event_offset, contract_id, transaction_id, workflow_id, ledger_effective_time, template_id, node_index, command_id, application_id, submitter, string_to_array(flat_event_witnesses, '|'), string_to_array(tree_event_witnesses, '|'),
           create_argument, string_to_array(create_signatories,'|'), string_to_array(create_observers,'|'), create_agreement_text, create_consumed_at, create_key_value,
           exercise_consuming, exercise_choice, exercise_argument, exercise_result, string_to_array(exercise_actors,'|'), string_to_array(exercise_child_event_ids,'|')
         from
           unnest(
             $eventIds::varchar[], $eventOffsets::bytea[], $contractIds::varchar[], $transactionIds::varchar[], $workflowIds::varchar[], $ledgerEffectiveTimes::timestamp[], $templateIds::varchar[], $nodeIndexes::int[], $commandIds::varchar[], $applicationIds::varchar[], $submitters::varchar[], $flatEventWitnesses::varchar[], $treeEventWitnesses::varchar[],
             $createArguments::bytea[], $createSignatories::varchar[], $createObservers::varchar[], $createAgreementTexts::varchar[], $createConsumedAt::bytea[], $createKeyValues::bytea[],
             $exerciseConsuming::bool[], $exerciseChoices::varchar[], $exerciseArguments::bytea[], $exerciseResults::bytea[], $exerciseActors::varchar[], $exerciseChildEventIds::varchar[]
           )
           as
               t(
                 event_id, event_offset, contract_id, transaction_id, workflow_id, ledger_effective_time, template_id, node_index, command_id, application_id, submitter, flat_event_witnesses, tree_event_witnesses,
                 create_argument, create_signatories, create_observers, create_agreement_text, create_consumed_at, create_key_value,
                 exercise_consuming, exercise_choice, exercise_argument, exercise_result, exercise_actors, exercise_child_event_ids
               )
       """

}
