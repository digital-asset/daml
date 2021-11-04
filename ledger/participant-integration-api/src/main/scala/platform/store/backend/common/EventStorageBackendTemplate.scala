// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import anorm.SqlParser.{array, bool, byteArray, int, long, str}
import anorm.{Row, RowParser, SimpleSql, ~}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.Conversions.{
  contractId,
  eventId,
  identifier,
  offset,
  timestampFromMicros,
}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.appendonlydao.events.{EventsTable, Identifier, Raw}
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.EventStorageBackend.{FilterParams, RangeParams}
import com.daml.platform.store.backend.EventStorageBackend.RawTransactionEvent
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}

import scala.collection.compat.immutable.ArraySeq

abstract class EventStorageBackendTemplate(
    eventStrategy: EventStrategy,
    queryStrategy: QueryStrategy,
    // TODO Refactoring: This method is needed in pruneEvents, but belongs to [[ParameterStorageBackend]].
    //                   Remove with the break-out of pruneEvents.
    participantAllDivulgedContractsPrunedUpToInclusive: Connection => Option[Offset],
) extends EventStorageBackend {
  import com.daml.platform.store.Conversions.ArrayColumnToStringArray.arrayColumnToStringArray

  private val logger: ContextualizedLogger = ContextualizedLogger.get(this.getClass)

  private val selectColumnsForFlatTransactions =
    Seq(
      "event_offset",
      "transaction_id",
      "node_index",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "event_id",
      "contract_id",
      "template_id",
      "create_argument",
      "create_argument_compression",
      "create_signatories",
      "create_observers",
      "create_agreement_text",
      "create_key_value",
      "create_key_value_compression",
    ).mkString(", ")

  private type SharedRow =
    Offset ~ String ~ Int ~ Long ~ String ~ String ~ Timestamp ~ Identifier ~ Option[String] ~
      Option[String] ~ Array[String]

  private val sharedRow: RowParser[SharedRow] =
    offset("event_offset") ~
      str("transaction_id") ~
      int("node_index") ~
      long("event_sequential_id") ~
      str("event_id") ~
      str("contract_id") ~
      timestampFromMicros("ledger_effective_time") ~
      identifier("template_id") ~
      str("command_id").? ~
      str("workflow_id").? ~
      array[String]("event_witnesses")

  private type CreatedEventRow =
    SharedRow ~ Array[Byte] ~ Option[Int] ~ Array[String] ~ Array[String] ~ Option[String] ~
      Option[Array[Byte]] ~ Option[Int]

  private val createdEventRow: RowParser[CreatedEventRow] =
    sharedRow ~
      byteArray("create_argument") ~
      int("create_argument_compression").? ~
      array[String]("create_signatories") ~
      array[String]("create_observers") ~
      str("create_agreement_text").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").?

  private type ExercisedEventRow =
    SharedRow ~ Boolean ~ String ~ Array[Byte] ~ Option[Int] ~ Option[Array[Byte]] ~ Option[Int] ~
      Array[String] ~ Array[String]

  private val exercisedEventRow: RowParser[ExercisedEventRow] = {
    import com.daml.platform.store.Conversions.bigDecimalColumnToBoolean
    sharedRow ~
      bool("exercise_consuming") ~
      str("exercise_choice") ~
      byteArray("exercise_argument") ~
      int("exercise_argument_compression").? ~
      byteArray("exercise_result").? ~
      int("exercise_result_compression").? ~
      array[String]("exercise_actors") ~
      array[String]("exercise_child_event_ids")
  }

  private type ArchiveEventRow = SharedRow

  private val archivedEventRow: RowParser[ArchiveEventRow] = sharedRow

  private val createdFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~
          templateId ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createArgumentCompression ~
          createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue ~ createKeyValueCompression =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.FlatEvent.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createSignatories = ArraySeq.unsafeWrapArray(createSignatories),
            createObservers = ArraySeq.unsafeWrapArray(createObservers),
            createAgreementText = createAgreementText,
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            eventWitnesses = ArraySeq.unsafeWrapArray(eventWitnesses),
          ),
        )
    }

  private val archivedFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent.Archived]] =
    archivedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.FlatEvent.Archived(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            eventWitnesses = ArraySeq.unsafeWrapArray(eventWitnesses),
          ),
        )
    }

  private val rawFlatEventParser: RowParser[EventsTable.Entry[Raw.FlatEvent]] =
    createdFlatEventParser | archivedFlatEventParser

  private val createdTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent.Created]] =
    createdEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ createArgument ~ createArgumentCompression ~ createSignatories ~ createObservers ~ createAgreementText ~ createKeyValue ~ createKeyValueCompression =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.TreeEvent.Created(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            createArgument = createArgument,
            createArgumentCompression = createArgumentCompression,
            createSignatories = ArraySeq.unsafeWrapArray(createSignatories),
            createObservers = ArraySeq.unsafeWrapArray(createObservers),
            createAgreementText = createAgreementText,
            createKeyValue = createKeyValue,
            createKeyValueCompression = createKeyValueCompression,
            eventWitnesses = ArraySeq.unsafeWrapArray(eventWitnesses),
          ),
        )
    }

  private val exercisedTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent.Exercised]] =
    exercisedEventRow map {
      case eventOffset ~ transactionId ~ nodeIndex ~ eventSequentialId ~ eventId ~ contractId ~ ledgerEffectiveTime ~ templateId ~ commandId ~ workflowId ~ eventWitnesses ~ exerciseConsuming ~ exerciseChoice ~ exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~ exerciseChildEventIds =>
        // ArraySeq.unsafeWrapArray is safe here
        // since we get the Array from parsing and don't let it escape anywhere.
        EventsTable.Entry(
          eventOffset = eventOffset,
          transactionId = transactionId,
          nodeIndex = nodeIndex,
          eventSequentialId = eventSequentialId,
          ledgerEffectiveTime = ledgerEffectiveTime,
          commandId = commandId.getOrElse(""),
          workflowId = workflowId.getOrElse(""),
          event = Raw.TreeEvent.Exercised(
            eventId = eventId,
            contractId = contractId,
            templateId = templateId,
            exerciseConsuming = exerciseConsuming,
            exerciseChoice = exerciseChoice,
            exerciseArgument = exerciseArgument,
            exerciseArgumentCompression = exerciseArgumentCompression,
            exerciseResult = exerciseResult,
            exerciseResultCompression = exerciseResultCompression,
            exerciseActors = ArraySeq.unsafeWrapArray(exerciseActors),
            exerciseChildEventIds = ArraySeq.unsafeWrapArray(exerciseChildEventIds),
            eventWitnesses = ArraySeq.unsafeWrapArray(eventWitnesses),
          ),
        )
    }

  private val rawTreeEventParser: RowParser[EventsTable.Entry[Raw.TreeEvent]] =
    createdTreeEventParser | exercisedTreeEventParser

  private val selectColumnsForTransactionTree = Seq(
    "event_offset",
    "transaction_id",
    "node_index",
    "event_sequential_id",
    "participant_events.event_id",
    "contract_id",
    "ledger_effective_time",
    "template_id",
    "workflow_id",
    "create_argument",
    "create_argument_compression",
    "create_signatories",
    "create_observers",
    "create_agreement_text",
    "create_key_value",
    "create_key_value_compression",
    "exercise_choice",
    "exercise_argument",
    "exercise_argument_compression",
    "exercise_result",
    "exercise_result_compression",
    "exercise_actors",
    "exercise_child_event_ids",
  ).mkString(", ")

  private def events[T](
      columnPrefix: String,
      joinClause: String,
      additionalAndClause: CompositeSql,
      rowParser: RowParser[T],
      selectColumns: String,
      witnessesColumn: String,
  )(
      limit: Option[Int],
      fetchSizeHint: Option[Int],
      filterParams: FilterParams,
  )(connection: Connection): Vector[T] = {
    val parties = filterParams.wildCardParties ++ filterParams.partiesAndTemplates.flatMap(_._1)
    SQL"""
        SELECT
          #$selectColumns, ${eventStrategy
      .filteredEventWitnessesClause(witnessesColumn, parties)} as event_witnesses,
          case when ${eventStrategy
      .submittersArePartiesClause("submitters", parties)} then command_id else '' end as command_id
        FROM
          participant_events #$columnPrefix #$joinClause
        WHERE
        $additionalAndClause
          ${eventStrategy.witnessesWhereClause(witnessesColumn, filterParams)}
        ORDER BY event_sequential_id
        ${queryStrategy.limitClause(limit)}"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rowParser)(connection)
  }

  override def activeContractEvents(
      rangeParams: RangeParams,
      filterParams: FilterParams,
      endInclusiveOffset: Offset,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    events(
      columnPrefix = "active_cs",
      joinClause = "",
      additionalAndClause = cSQL"""
            event_sequential_id > ${rangeParams.startExclusive} AND
            event_sequential_id <= ${rangeParams.endInclusive} AND
            active_cs.event_kind = 10 AND -- create
            NOT EXISTS (
              SELECT 1
              FROM participant_events archived_cs
              WHERE
                archived_cs.contract_id = active_cs.contract_id AND
                archived_cs.event_kind = 20 AND -- consuming
                archived_cs.event_offset <= $endInclusiveOffset
            ) AND""",
      rowParser = rawFlatEventParser,
      selectColumns = selectColumnsForFlatTransactions,
      witnessesColumn = "flat_event_witnesses",
    )(
      limit = rangeParams.limit,
      fetchSizeHint = rangeParams.fetchSizeHint,
      filterParams,
    )(connection)
  }

  override def transactionEvents(
      rangeParams: RangeParams,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    events(
      columnPrefix = "",
      joinClause = "",
      additionalAndClause = cSQL"""
            event_sequential_id > ${rangeParams.startExclusive} AND
            event_sequential_id <= ${rangeParams.endInclusive} AND""",
      rowParser = rawFlatEventParser,
      selectColumns = selectColumnsForFlatTransactions,
      witnessesColumn = "flat_event_witnesses",
    )(
      limit = rangeParams.limit,
      fetchSizeHint = rangeParams.fetchSizeHint,
      filterParams,
    )(connection)
  }

  override def flatTransaction(
      transactionId: Ref.TransactionId,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.ledgerStringToStatement
    events(
      columnPrefix = "",
      joinClause = """JOIN parameters ON
          |  (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
          |  AND event_offset <= ledger_end""".stripMargin,
      additionalAndClause = cSQL"""
            transaction_id = $transactionId AND
            event_kind != 0 AND -- we do not want to fetch divulgence events""",
      rowParser = rawFlatEventParser,
      selectColumns = selectColumnsForFlatTransactions,
      witnessesColumn = "flat_event_witnesses",
    )(
      limit = None,
      fetchSizeHint = None,
      filterParams = filterParams,
    )(connection)
  }

  override def transactionTreeEvents(
      rangeParams: RangeParams,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] = {
    events(
      columnPrefix = "",
      joinClause = "",
      additionalAndClause = cSQL"""
            event_sequential_id > ${rangeParams.startExclusive} AND
            event_sequential_id <= ${rangeParams.endInclusive} AND
            event_kind != 0 AND -- we do not want to fetch divulgence events""",
      rowParser = rawTreeEventParser,
      selectColumns =
        s"$selectColumnsForTransactionTree, ${queryStrategy.columnEqualityBoolean("event_kind", "20")} as exercise_consuming",
      witnessesColumn = "tree_event_witnesses",
    )(
      limit = rangeParams.limit,
      fetchSizeHint = rangeParams.fetchSizeHint,
      filterParams,
    )(connection)
  }

  override def transactionTree(
      transactionId: Ref.TransactionId,
      filterParams: FilterParams,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] = {
    import com.daml.platform.store.Conversions.ledgerStringToStatement
    events(
      columnPrefix = "",
      joinClause = """JOIN parameters ON
          |  (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
          |  AND event_offset <= ledger_end""".stripMargin,
      additionalAndClause = cSQL"""
            transaction_id = $transactionId AND
            event_kind != 0 AND -- we do not want to fetch divulgence events""",
      rowParser = rawTreeEventParser,
      selectColumns =
        s"$selectColumnsForTransactionTree, ${queryStrategy.columnEqualityBoolean("event_kind", "20")} as exercise_consuming",
      witnessesColumn = "tree_event_witnesses",
    )(
      limit = None,
      fetchSizeHint = None,
      filterParams,
    )(connection)
  }

  // TODO Refactoring: This method is too complex for StorageBackend.
  //                   Break the method into its constituents and trigger them from the caller of this method.
  override def pruneEvents(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(connection: Connection, loggingContext: LoggingContext): Unit = {
    import com.daml.platform.store.Conversions.OffsetToStatement

    if (pruneAllDivulgedContracts) {
      pruneWithLogging(queryDescription = "All retroactive divulgence events pruning") {
        SQL"""
          -- Retroactive divulgence events
          delete from participant_events_divulgence delete_events
          where delete_events.event_offset <= $pruneUpToInclusive
            or delete_events.event_offset is null
          """
      }(connection, loggingContext)
    } else {
      pruneWithLogging(queryDescription = "Archived retroactive divulgence events pruning") {
        SQL"""
          -- Retroactive divulgence events (only for contracts archived before the specified offset)
          delete from participant_events_divulgence delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive
            and exists (
              select 1 from participant_events_consuming_exercise archive_events
              where
                archive_events.event_offset <= $pruneUpToInclusive and
                archive_events.contract_id = delete_events.contract_id
            )"""
      }(connection, loggingContext)
    }

    pruneWithLogging(queryDescription = "Create events pruning") {
      SQL"""
          -- Create events (only for contracts archived before the specified offset)
          delete from participant_events_create delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive and
            exists (
              SELECT 1 FROM participant_events_consuming_exercise archive_events
              WHERE
                archive_events.event_offset <= $pruneUpToInclusive AND
                archive_events.contract_id = delete_events.contract_id
            )"""
    }(connection, loggingContext)

    if (pruneAllDivulgedContracts) {
      val pruneAfterClause = {
        // We need to distinguish between the two cases since lexicographical comparison
        // in Oracle doesn't work with '' (empty strings are treated as NULLs) as one of the operands
        participantAllDivulgedContractsPrunedUpToInclusive(connection) match {
          case Some(pruneAfter) => cSQL"and event_offset > $pruneAfter"
          case None => cSQL""
        }
      }

      pruneWithLogging(queryDescription = "Immediate divulgence events pruning") {
        SQL"""
            -- Immediate divulgence pruning
            delete from participant_events_create c
            where event_offset <= $pruneUpToInclusive
            -- Only prune create events which did not have a locally hosted party before their creation offset
            and not exists (
              select 1
              from party_entries p
              where p.typ = 'accept'
              and p.ledger_offset <= c.event_offset
              and #${queryStrategy.isTrue("p.is_local")}
              and #${queryStrategy.arrayContains("c.flat_event_witnesses", "p.party")}
            )
            $pruneAfterClause
         """
      }(connection, loggingContext)
    }

    pruneWithLogging(queryDescription = "Exercise (consuming) events pruning") {
      SQL"""
          -- Exercise events (consuming)
          delete from participant_events_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }(connection, loggingContext)

    pruneWithLogging(queryDescription = "Exercise (non-consuming) events pruning") {
      SQL"""
          -- Exercise events (non-consuming)
          delete from participant_events_non_consuming_exercise delete_events
          where
            delete_events.event_offset <= $pruneUpToInclusive"""
    }(connection, loggingContext)
  }

  private def pruneWithLogging(queryDescription: String)(query: SimpleSql[Row])(
      connection: Connection,
      loggingContext: LoggingContext,
  ): Unit = {
    val deletedRows = query.executeUpdate()(connection)
    logger.info(s"$queryDescription finished: deleted $deletedRows rows.")(loggingContext)
  }

  private val rawTransactionEventParser: RowParser[RawTransactionEvent] = {
    import com.daml.platform.store.Conversions.ArrayColumnToStringArray.arrayColumnToStringArray
    (int("event_kind") ~
      str("transaction_id") ~
      int("node_index") ~
      str("command_id").? ~
      str("workflow_id").? ~
      eventId("event_id") ~
      contractId("contract_id") ~
      identifier("template_id").? ~
      timestampFromMicros("ledger_effective_time").? ~
      array[String]("create_signatories").? ~
      array[String]("create_observers").? ~
      str("create_agreement_text").? ~
      byteArray("create_key_value").? ~
      int("create_key_value_compression").? ~
      byteArray("create_argument").? ~
      int("create_argument_compression").? ~
      array[String]("tree_event_witnesses") ~
      array[String]("flat_event_witnesses") ~
      array[String]("submitters").? ~
      str("exercise_choice").? ~
      byteArray("exercise_argument").? ~
      int("exercise_argument_compression").? ~
      byteArray("exercise_result").? ~
      int("exercise_result_compression").? ~
      array[String]("exercise_actors").? ~
      array[String]("exercise_child_event_ids").? ~
      long("event_sequential_id") ~
      offset("event_offset")).map {
      case eventKind ~ transactionId ~ nodeIndex ~ commandId ~ workflowId ~ eventId ~ contractId ~ templateId ~ ledgerEffectiveTime ~ createSignatories ~
          createObservers ~ createAgreementText ~ createKeyValue ~ createKeyCompression ~
          createArgument ~ createArgumentCompression ~ treeEventWitnesses ~ flatEventWitnesses ~ submitters ~ exerciseChoice ~
          exerciseArgument ~ exerciseArgumentCompression ~ exerciseResult ~ exerciseResultCompression ~ exerciseActors ~
          exerciseChildEventIds ~ eventSequentialId ~ offset =>
        RawTransactionEvent(
          eventKind,
          transactionId,
          nodeIndex,
          commandId,
          workflowId,
          eventId,
          contractId,
          templateId,
          ledgerEffectiveTime,
          createSignatories,
          createObservers,
          createAgreementText,
          createKeyValue,
          createKeyCompression,
          createArgument,
          createArgumentCompression,
          treeEventWitnesses.toSet,
          flatEventWitnesses.toSet,
          submitters.map(_.toSet).getOrElse(Set.empty),
          exerciseChoice,
          exerciseArgument,
          exerciseArgumentCompression,
          exerciseResult,
          exerciseResultCompression,
          exerciseActors,
          exerciseChildEventIds,
          eventSequentialId,
          offset,
        )
    }
  }

  override def rawEvents(startExclusive: Long, endInclusive: Long)(
      connection: Connection
  ): Vector[RawTransactionEvent] =
    SQL"""
       SELECT
           event_kind,
           transaction_id,
           node_index,
           command_id,
           workflow_id,
           event_id,
           contract_id,
           template_id,
           ledger_effective_time,
           create_signatories,
           create_observers,
           create_agreement_text,
           create_key_value,
           create_key_value_compression,
           create_argument,
           create_argument_compression,
           tree_event_witnesses,
           flat_event_witnesses,
           submitters,
           exercise_choice,
           exercise_argument,
           exercise_argument_compression,
           exercise_result,
           exercise_result_compression,
           exercise_actors,
           exercise_child_event_ids,
           event_sequential_id,
           event_offset
       FROM
           participant_events
       WHERE
           event_sequential_id > $startExclusive
           and event_sequential_id <= $endInclusive
           and event_kind != 0
       ORDER BY event_sequential_id ASC"""
      .asVectorOf(rawTransactionEventParser)(connection)
}

/** This encapsulates the moving part as composing various Events queries.
  */
trait EventStrategy {

  /** This populates the following part of the query:
    *   SELECT ..., [THIS PART] as event_witnesses
    * Should boil down to an intersection between the set of the witnesses-column and the parties.
    *
    * @param witnessesColumnName name of the witnesses column in the query
    * @param parties which is all the parties we are interested in in the resul
    * @return the composable SQL
    */
  def filteredEventWitnessesClause(
      witnessesColumnName: String,
      parties: Set[Ref.Party],
  ): CompositeSql

  /** This populates the following part of the query:
    *   SELECT ...,case when [THIS PART] then command_id else "" end as command_id
    * Should boil down to a do-intersect? query between the submittersColumName column and the parties
    *
    * @param submittersColumnName name of the Array column holding submitters
    * @param parties which is all the parties we are interested in in the resul
    * @return the composable SQL
    */
  def submittersArePartiesClause(
      submittersColumnName: String,
      parties: Set[Ref.Party],
  ): CompositeSql

  /** This populates the following part of the query:
    *   SELECT ... WHERE ... AND [THIS PART]
    * This strategy is responsible to generate appropriate SQL cod based on the filterParams, so that results match the criteria
    *
    * @param witnessesColumnName name of the Array column holding witnesses
    * @param filterParams the filtering criteria
    * @return the composable SQL
    */
  def witnessesWhereClause(
      witnessesColumnName: String,
      filterParams: FilterParams,
  ): CompositeSql
}
