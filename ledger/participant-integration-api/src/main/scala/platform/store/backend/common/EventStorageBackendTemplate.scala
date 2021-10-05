// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import java.time.Instant
import anorm.SqlParser.{array, bool, byteArray, int, long, str}
import anorm.{RowParser, ~}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.Conversions.{identifier, instantFromMicros, offset}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf
import com.daml.platform.store.appendonlydao.events.{EventsTable, Identifier, Raw}
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.EventStorageBackend.{FilterParams, RangeParams}
import com.daml.platform.store.backend.common.ComposableQuery.{CompositeSql, SqlStringInterpolation}

import scala.collection.compat.immutable.ArraySeq

trait EventStorageBackendTemplate extends EventStorageBackend {
  import com.daml.platform.store.Conversions.ArrayColumnToStringArray.arrayColumnToStringArray

  def eventStrategy: EventStrategy
  def queryStrategy: QueryStrategy

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
    Offset ~ String ~ Int ~ Long ~ String ~ String ~ Instant ~ Identifier ~ Option[String] ~
      Option[String] ~ Array[String]

  private val sharedRow: RowParser[SharedRow] =
    offset("event_offset") ~
      str("transaction_id") ~
      int("node_index") ~
      long("event_sequential_id") ~
      str("event_id") ~
      str("contract_id") ~
      instantFromMicros("ledger_effective_time") ~
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
