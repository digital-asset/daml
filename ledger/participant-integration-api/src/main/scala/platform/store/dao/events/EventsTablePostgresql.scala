// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.{BatchSql, NamedParameter, Row, SimpleSql}
import com.daml.ledger.offset.Offset
import com.daml.lf.ledger.EventId
import com.daml.platform.store.Conversions._
import com.daml.platform.store.JdbcArrayConversions._

case class EventsTablePostgresql(idempotentEventInsertions: Boolean) extends EventsTable {

  /** Insertions are represented by a single statement made of nested arrays, one per column, instead of JDBC batches.
    * This leverages a PostgreSQL-specific feature known as "array unnesting", which has shown to be considerable
    * faster than using JDBC batches.
    */
  final class Batches(
      eventsInsertion: SimpleSql[Row],
      archivesUpdate: Option[BatchSql],
  ) extends EventsTable.Batches {
    override def execute()(implicit connection: Connection): Unit = {
      eventsInsertion.execute()
      archivesUpdate.foreach(_.execute())
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
      compressed: TransactionIndexing.Compressed.Events,
  ): EventsTable.Batches = {

    val batchSize = info.events.size
    val eventIds = Array.ofDim[String](batchSize)
    val eventOffsets = Array.fill[String](batchSize)(tx.offset.toHexString)
    val contractIds = Array.ofDim[String](batchSize)
    val transactionIds = Array.fill(batchSize)(tx.transactionId.asInstanceOf[String])
    val workflowIds = Array.fill(batchSize)(tx.workflowId.map(_.asInstanceOf[String]).orNull)
    val ledgerEffectiveTimes = Array.fill(batchSize)(tx.ledgerEffectiveTime)
    val templateIds = Array.ofDim[String](batchSize)
    val nodeIndexes = Array.ofDim[java.lang.Integer](batchSize)
    val commandIds =
      Array.fill(batchSize)(tx.completionInfo.map(_.commandId.asInstanceOf[String]).orNull)
    val applicationIds =
      Array.fill(batchSize)(tx.completionInfo.map(_.applicationId.asInstanceOf[String]).orNull)
    val submitters = Array.ofDim[String](batchSize)
    val flatEventWitnesses = Array.ofDim[String](batchSize)
    val treeEventWitnesses = Array.ofDim[String](batchSize)
    val createArguments = Array.ofDim[Array[Byte]](batchSize)
    val createSignatories = Array.ofDim[String](batchSize)
    val createObservers = Array.ofDim[String](batchSize)
    val createAgreementTexts = Array.ofDim[String](batchSize)
    val createConsumedAt = Array.ofDim[String](batchSize)
    val createKeyValues = Array.ofDim[Array[Byte]](batchSize)
    val exerciseConsuming = Array.ofDim[java.lang.Boolean](batchSize)
    val exerciseChoices = Array.ofDim[String](batchSize)
    val exerciseArguments = Array.ofDim[Array[Byte]](batchSize)
    val exerciseResults = Array.ofDim[Array[Byte]](batchSize)
    val exerciseActors = Array.ofDim[String](batchSize)
    val exerciseChildEventIds = Array.ofDim[String](batchSize)

    val submittersValue = tx.completionInfo.map(_.actAs.mkString("|")).orNull

    for (((nodeId, node), i) <- info.events.zipWithIndex) {
      node match {
        case create: Create =>
          submitters(i) = submittersValue
          contractIds(i) = create.coid.coid
          templateIds(i) = create.templateId.toString
          eventIds(i) = EventId(tx.transactionId, nodeId).toLedgerString
          nodeIndexes(i) = nodeId.index
          flatEventWitnesses(i) = info.stakeholders.getOrElse(nodeId, Set.empty).mkString("|")
          treeEventWitnesses(i) = info.disclosure.getOrElse(nodeId, Set.empty).mkString("|")
          createArguments(i) = compressed.createArguments(nodeId)
          createSignatories(i) = create.signatories.mkString("|")
          createObservers(i) = create.stakeholders.diff(create.signatories).mkString("|")
          if (create.agreementText.nonEmpty) {
            createAgreementTexts(i) = create.agreementText
          }
          createKeyValues(i) = compressed.createKeyValues.get(nodeId).orNull
        case exercise: Exercise =>
          submitters(i) = submittersValue
          contractIds(i) = exercise.targetCoid.coid
          templateIds(i) = exercise.templateId.toString
          eventIds(i) = EventId(tx.transactionId, nodeId).toLedgerString
          nodeIndexes(i) = nodeId.index
          flatEventWitnesses(i) = info.stakeholders.getOrElse(nodeId, Set.empty).mkString("|")
          treeEventWitnesses(i) = info.disclosure.getOrElse(nodeId, Set.empty).mkString("|")
          exerciseConsuming(i) = exercise.consuming
          exerciseChoices(i) = exercise.choiceId
          exerciseArguments(i) = compressed.exerciseArguments(nodeId)
          exerciseResults(i) = compressed.exerciseResults.get(nodeId).orNull
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
      exerciseChildEventIds,
      createArgumentsCompression = eventIds.map(_ => compressed.createArgumentsCompression.id),
      createKeyValueCompression = eventIds.map(_ => compressed.createKeyValueCompression.id),
      exerciseArgumentCompression = eventIds.map(_ => compressed.exerciseArgumentsCompression.id),
      exerciseResultCompression = eventIds.map(_ => compressed.exerciseResultsCompression.id),
    )

    val archivals =
      info.archives.iterator.map(archive(tx.offset)).toList

    new Batches(
      eventsInsertion = inserts,
      archivesUpdate = batch(updateArchived, archivals),
    )
  }

  private object Params {
    val eventIds = "eventIds"
    val eventOffsets = "eventOffsets"
    val contractIds = "contractIds"
    val transactionIds = "transactionIds"
    val workflowIds = "workflowIds"
    val ledgerEffectiveTimes = "ledgerEffectiveTimes"
    val templateIds = "templateIds"
    val nodeIndexes = "nodeIndexes"
    val commandIds = "commandIds"
    val applicationIds = "applicationIds"
    val submitters = "submitters"
    val flatEventWitnesses = "flatEventWitnesses"
    val treeEventWitnesses = "treeEventWitnesses"
    val createArguments = "createArguments"
    val createSignatories = "createSignatories"
    val createObservers = "createObservers"
    val createAgreementTexts = "createAgreementTexts"
    val createConsumedAt = "createConsumedAt"
    val createKeyValues = "createKeyValues"
    val exerciseConsuming = "exerciseConsuming"
    val exerciseChoices = "exerciseChoices"
    val exerciseArguments = "exerciseArguments"
    val exerciseResults = "exerciseResults"
    val exerciseActors = "exerciseActors"
    val exerciseChildEventIds = "exerciseChildEventIds"
    val createArgumentsCompression = "createArgumentsCompression"
    val createKeyValueCompression = "create_key_value_compression"
    val exerciseArgumentCompression = "exercise_argument_compression"
    val exerciseResultCompression = "exercise_result_compression"
  }

  /** Allows idempotent event insertions (i.e. discards new rows on `event_id` conflicts).
    *
    * Idempotent insertions are necessary for seamless restarts of the [[com.daml.platform.indexer.JdbcIndexer]]
    * after partially persisted transactions.
    * (e.g. a transaction's events are persisted but the corresponding ledger end not).
    *
    * Partially-persisted ledger entries are possible when performing transaction updates in a pipelined fashion.
    * For more details on pipelined transaction updates, see [[com.daml.platform.indexer.PipelinedExecuteUpdate]].
    */
  private val conflictActionClause =
    if (idempotentEventInsertions) "on conflict do nothing" else ""

  private[events] val insertStmt = {
    import Params._
    s"""insert into participant_events(
           event_id, event_offset, contract_id, transaction_id, workflow_id, ledger_effective_time, template_id, node_index, command_id, application_id, submitters, flat_event_witnesses, tree_event_witnesses,
           create_argument, create_argument_compression, create_signatories, create_observers, create_agreement_text, create_consumed_at, create_key_value, create_key_value_compression,
           exercise_consuming, exercise_choice, exercise_argument, exercise_argument_compression, exercise_result, exercise_result_compression, exercise_actors, exercise_child_event_ids
         )
         select
           event_id, event_offset, contract_id, transaction_id, workflow_id, ledger_effective_time, template_id, node_index, command_id, application_id, string_to_array(submitters,'|'), string_to_array(flat_event_witnesses, '|'), string_to_array(tree_event_witnesses, '|'),
           create_argument, create_argument_compression, string_to_array(create_signatories,'|'), string_to_array(create_observers,'|'), create_agreement_text, create_consumed_at, create_key_value, create_key_value_compression,
           exercise_consuming, exercise_choice, exercise_argument, exercise_argument_compression, exercise_result, exercise_result_compression, string_to_array(exercise_actors,'|'), string_to_array(exercise_child_event_ids,'|')
         from
           unnest(
             {$eventIds}, {$eventOffsets}, {$contractIds}, {$transactionIds}, {$workflowIds}, {$ledgerEffectiveTimes}, {$templateIds}, {$nodeIndexes}, {$commandIds}, {$applicationIds}, {$submitters}, {$flatEventWitnesses}, {$treeEventWitnesses},
             {$createArguments}, {$createArgumentsCompression}, {$createSignatories}, {$createObservers}, {$createAgreementTexts}, {$createConsumedAt}, {$createKeyValues}, {$createKeyValueCompression},
             {$exerciseConsuming}, {$exerciseChoices}, {$exerciseArguments}, {$exerciseArgumentCompression}, {$exerciseResults}, {$exerciseResultCompression}, {$exerciseActors}, {$exerciseChildEventIds}
           )
           as
               t(
                 event_id, event_offset, contract_id, transaction_id, workflow_id, ledger_effective_time, template_id, node_index, command_id, application_id, submitters, flat_event_witnesses, tree_event_witnesses,
                 create_argument, create_argument_compression, create_signatories, create_observers, create_agreement_text, create_consumed_at, create_key_value, create_key_value_compression,
                 exercise_consuming, exercise_choice, exercise_argument, exercise_argument_compression, exercise_result, exercise_result_compression, exercise_actors, exercise_child_event_ids
               )
         $conflictActionClause
       """
  }

  private def insertEvents(
      eventIds: Array[String],
      eventOffsets: Array[String],
      contractIds: Array[String],
      transactionIds: Array[String],
      workflowIds: Array[String],
      ledgerEffectiveTimes: Array[Instant],
      templateIds: Array[String],
      nodeIndexes: Array[java.lang.Integer],
      commandIds: Array[String],
      applicationIds: Array[String],
      submitters: Array[String],
      flatEventWitnesses: Array[String],
      treeEventWitnesses: Array[String],
      createArguments: Array[Array[Byte]],
      createSignatories: Array[String],
      createObservers: Array[String],
      createAgreementTexts: Array[String],
      createConsumedAt: Array[String],
      createKeyValues: Array[Array[Byte]],
      exerciseConsuming: Array[java.lang.Boolean],
      exerciseChoices: Array[String],
      exerciseArguments: Array[Array[Byte]],
      exerciseResults: Array[Array[Byte]],
      exerciseActors: Array[String],
      exerciseChildEventIds: Array[String],
      createArgumentsCompression: Array[Option[Int]],
      createKeyValueCompression: Array[Option[Int]],
      exerciseArgumentCompression: Array[Option[Int]],
      exerciseResultCompression: Array[Option[Int]],
  ): SimpleSql[Row] = {
    import com.daml.platform.store.JdbcArrayConversions.IntToSmallIntConversions._
    anorm
      .SQL(insertStmt)
      .on(
        Params.eventIds -> eventIds,
        Params.eventOffsets -> eventOffsets,
        Params.contractIds -> contractIds,
        Params.transactionIds -> transactionIds,
        Params.workflowIds -> workflowIds,
        Params.ledgerEffectiveTimes -> ledgerEffectiveTimes,
        Params.templateIds -> templateIds,
        Params.nodeIndexes -> nodeIndexes,
        Params.commandIds -> commandIds,
        Params.applicationIds -> applicationIds,
        Params.submitters -> submitters,
        Params.flatEventWitnesses -> flatEventWitnesses,
        Params.treeEventWitnesses -> treeEventWitnesses,
        Params.createArguments -> createArguments,
        Params.createSignatories -> createSignatories,
        Params.createObservers -> createObservers,
        Params.createAgreementTexts -> createAgreementTexts,
        Params.createConsumedAt -> createConsumedAt,
        Params.createKeyValues -> createKeyValues,
        Params.exerciseConsuming -> exerciseConsuming,
        Params.exerciseChoices -> exerciseChoices,
        Params.exerciseArguments -> exerciseArguments,
        Params.exerciseResults -> exerciseResults,
        Params.exerciseActors -> exerciseActors,
        Params.exerciseChildEventIds -> exerciseChildEventIds,
        Params.exerciseResultCompression -> exerciseResultCompression,
        Params.exerciseArgumentCompression -> exerciseArgumentCompression,
        Params.createArgumentsCompression -> createArgumentsCompression,
        Params.createKeyValueCompression -> createKeyValueCompression,
      )
  }
}
