// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.io.InputStream
import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.{array, binaryStream, bool, int, long, str}
import anorm.{RowParser, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.{ApplicationId, TransactionId}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.participant.state.v1.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.CompletionFromTransaction.toApiCheckpoint
import com.daml.platform.store.Conversions.{contractId, identifier, instant, offset}
import com.daml.platform.store.appendonlydao.events.{ContractId, EventsTable, Identifier, Key, Raw}
import com.daml.platform.store.backend.StorageBackend
import com.google.rpc.status.Status
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf

import scala.collection.compat.immutable.ArraySeq

private[backend] object TemplatedStorageBackend {

  private val sharedCompletionColumns: RowParser[Offset ~ Instant ~ String] =
    offset("completion_offset") ~ instant("record_time") ~ str("command_id")

  private val acceptedCommandParser: RowParser[CompletionStreamResponse] =
    sharedCompletionColumns ~ str("transaction_id") map {
      case offset ~ recordTime ~ commandId ~ transactionId =>
        CompletionStreamResponse(
          checkpoint = toApiCheckpoint(recordTime, offset),
          completions = Seq(Completion(commandId, Some(Status()), transactionId)),
        )
    }

  private val rejectedCommandParser: RowParser[CompletionStreamResponse] =
    sharedCompletionColumns ~ int("status_code") ~ str("status_message") map {
      case offset ~ recordTime ~ commandId ~ statusCode ~ statusMessage =>
        CompletionStreamResponse(
          checkpoint = toApiCheckpoint(recordTime, offset),
          completions = Seq(Completion(commandId, Some(Status(statusCode, statusMessage)))),
        )
    }

  private val completionParser: RowParser[CompletionStreamResponse] =
    acceptedCommandParser | rejectedCommandParser

  def commandCompletions(
      startExclusive: Offset,
      endInclusive: Offset,
      applicationId: ApplicationId,
      submittersInPartiesClause: String,
  )(connection: Connection): List[CompletionStreamResponse] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    import com.daml.platform.store.Conversions.ledgerStringToStatement
    SQL"""
      select
        completion_offset,
        record_time,
        command_id,
        transaction_id,
        status_code,
        status_message
      from
        participant_command_completions
      where
        completion_offset > $startExclusive and
        completion_offset <= $endInclusive and
        application_id = $applicationId and
        #$submittersInPartiesClause
        order by completion_offset asc"""
      .as(completionParser.*)(connection)
  }

  private val contractRowParser: RowParser[StorageBackend.RawContract] =
    (str("template_id")
      ~ binaryStream("create_argument")
      ~ int("create_argument_compression").?)
      .map(SqlParser.flatten)
      .map(StorageBackend.RawContract.tupled)

  def activeContractWithArgument(
      treeEventWitnessesWhereClause: String,
      contractId: ContractId,
  )(connection: Connection): Option[StorageBackend.RawContract] = {
    import com.daml.platform.store.Conversions.ContractIdToStatement

    SQL"""
  WITH archival_event AS (
         SELECT participant_events.*
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 20  -- consuming exercise
            AND event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$treeEventWitnessesWhereClause  -- only use visible archivals
          FETCH NEXT 1 ROW ONLY
       ),
       create_event AS (
         SELECT contract_id, template_id, create_argument, create_argument_compression
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$treeEventWitnessesWhereClause
          FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
       ),
       -- no visibility check, as it is used to backfill missing template_id and create_arguments for divulged contracts
       create_event_unrestricted AS (
         SELECT contract_id, template_id, create_argument, create_argument_compression
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
          FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
       ),
       divulged_contract AS (
         SELECT divulgence_events.contract_id,
                -- Note: the divulgance_event.template_id and .create_argument can be NULL
                -- for certain integrations. For example, the KV integration exploits that
                -- every participant node knows about all create events. The integration
                -- therefore only communicates the change in visibility to the IndexDB, but
                -- does not include a full divulgence event.
                COALESCE(divulgence_events.template_id, create_event_unrestricted.template_id),
                COALESCE(divulgence_events.create_argument, create_event_unrestricted.create_argument),
                COALESCE(divulgence_events.create_argument_compression, create_event_unrestricted.create_argument_compression)
           FROM participant_events divulgence_events LEFT OUTER JOIN create_event_unrestricted USING (contract_id),
                parameters
          WHERE divulgence_events.contract_id = $contractId -- restrict to aid query planner
            AND divulgence_events.event_kind = 0 -- divulgence
            AND divulgence_events.event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$treeEventWitnessesWhereClause
          ORDER BY divulgence_events.event_sequential_id
            -- prudent engineering: make results more stable by preferring earlier divulgence events
            -- Results might still change due to pruning.
          FETCH NEXT 1 ROW ONLY
       ),
       create_and_divulged_contracts AS (
         (SELECT * FROM create_event)   -- prefer create over divulgance events
         UNION ALL
         (SELECT * FROM divulged_contract)
       )
  SELECT contract_id, template_id, create_argument, create_argument_compression
    FROM create_and_divulged_contracts
   WHERE NOT EXISTS (SELECT 1 FROM archival_event)
   FETCH NEXT 1 ROW ONLY"""
      .as(contractRowParser.singleOpt)(connection)
  }

  private val contractWithoutValueRowParser: RowParser[String] =
    str("template_id")

  def activeContractWithoutArgument(
      treeEventWitnessesWhereClause: String,
      contractId: ContractId,
  )(connection: Connection): Option[String] = {
    import com.daml.platform.store.Conversions.ContractIdToStatement
    SQL"""
  WITH archival_event AS (
         SELECT participant_events.*
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 20  -- consuming exercise
            AND event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$treeEventWitnessesWhereClause  -- only use visible archivals
          FETCH NEXT 1 ROW ONLY
       ),
       create_event AS (
         SELECT contract_id, template_id
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$treeEventWitnessesWhereClause
          FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
       ),
       -- no visibility check, as it is used to backfill missing template_id and create_arguments for divulged contracts
       create_event_unrestricted AS (
         SELECT contract_id, template_id
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
          FETCH NEXT 1 ROW ONLY -- limit here to guide planner wrt expected number of results
       ),
       divulged_contract AS (
         SELECT divulgence_events.contract_id,
                -- Note: the divulgance_event.template_id can be NULL
                -- for certain integrations. For example, the KV integration exploits that
                -- every participant node knows about all create events. The integration
                -- therefore only communicates the change in visibility to the IndexDB, but
                -- does not include a full divulgence event.
                COALESCE(divulgence_events.template_id, create_event_unrestricted.template_id)
           FROM participant_events divulgence_events LEFT OUTER JOIN create_event_unrestricted USING (contract_id),
                parameters
          WHERE divulgence_events.contract_id = $contractId -- restrict to aid query planner
            AND divulgence_events.event_kind = 0 -- divulgence
            AND divulgence_events.event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$treeEventWitnessesWhereClause
          ORDER BY divulgence_events.event_sequential_id
            -- prudent engineering: make results more stable by preferring earlier divulgence events
            -- Results might still change due to pruning.
          FETCH NEXT 1 ROW ONLY
       ),
       create_and_divulged_contracts AS (
         (SELECT * FROM create_event)   -- prefer create over divulgence events
         UNION ALL
         (SELECT * FROM divulged_contract)
       )
  SELECT contract_id, template_id
    FROM create_and_divulged_contracts
   WHERE NOT EXISTS (SELECT 1 FROM archival_event)
   FETCH NEXT 1 ROW ONLY
           """.as(contractWithoutValueRowParser.singleOpt)(connection)
  }

  def contractKey(flatEventWitnesses: String => String, key: Key)(
      connection: Connection
  ): Option[ContractId] = {
    import com.daml.platform.store.Conversions.HashToStatement
    SQL"""
  WITH last_contract_key_create AS (
         SELECT participant_events.*
           FROM participant_events, parameters
          WHERE event_kind = 10 -- create
            AND create_key_hash = ${key.hash}
            AND event_sequential_id <= parameters.ledger_end_sequential_id
                -- do NOT check visibility here, as otherwise we do not abort the scan early
          ORDER BY event_sequential_id DESC
          FETCH NEXT 1 ROW ONLY
       )
  SELECT contract_id
    FROM last_contract_key_create -- creation only, as divulged contracts cannot be fetched by key
  WHERE #${flatEventWitnesses("last_contract_key_create")} -- check visibility only here
    AND NOT EXISTS       -- check no archival visible
         (SELECT 1
            FROM participant_events, parameters
           WHERE event_kind = 20 -- consuming exercise
             AND event_sequential_id <= parameters.ledger_end_sequential_id
             AND #${flatEventWitnesses("participant_events")}
             AND contract_id = last_contract_key_create.contract_id
         )
       """.as(contractId("contract_id").singleOpt)(connection)
  }

  private val selectColumnsForTransactions =
    Seq(
      "event_offset",
      "transaction_id",
      "node_index",
      "event_sequential_id",
      "ledger_effective_time",
      "workflow_id",
      "participant_events.event_id",
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

  private val selectColumnsForACS =
    Seq(
      "active_cs.event_offset",
      "active_cs.transaction_id",
      "active_cs.node_index",
      "active_cs.event_sequential_id",
      "active_cs.ledger_effective_time",
      "active_cs.workflow_id",
      "active_cs.event_id",
      "active_cs.contract_id",
      "active_cs.template_id",
      "active_cs.create_argument",
      "active_cs.create_argument_compression",
      "active_cs.create_signatories",
      "active_cs.create_observers",
      "active_cs.create_agreement_text",
      "active_cs.create_key_value",
      "active_cs.create_key_value_compression",
    ).mkString(", ")

  private type SharedRow =
    Offset ~ String ~ Int ~ Long ~ String ~ String ~ Instant ~ Identifier ~ Option[String] ~
      Option[String] ~ Array[String]

  import com.daml.platform.store.Conversions.ArrayColumnToStringArray.arrayColumnToStringArray

  private val sharedRow: RowParser[SharedRow] =
    offset("event_offset") ~
      str("transaction_id") ~
      int("node_index") ~
      long("event_sequential_id") ~
      str("event_id") ~
      str("contract_id") ~
      instant("ledger_effective_time") ~
      identifier("template_id") ~
      str("command_id").? ~
      str("workflow_id").? ~
      array[String]("event_witnesses")

  private type CreatedEventRow =
    SharedRow ~ InputStream ~ Option[Int] ~ Array[String] ~ Array[String] ~ Option[String] ~
      Option[InputStream] ~ Option[Int]

  private val createdEventRow: RowParser[CreatedEventRow] =
    sharedRow ~
      binaryStream("create_argument") ~
      int("create_argument_compression").? ~
      array[String]("create_signatories") ~
      array[String]("create_observers") ~
      str("create_agreement_text").? ~
      binaryStream("create_key_value").? ~
      int("create_key_value_compression").?

  private type ExercisedEventRow =
    SharedRow ~ Boolean ~ String ~ InputStream ~ Option[Int] ~ Option[InputStream] ~ Option[Int] ~
      Array[String] ~ Array[String]

  private val exercisedEventRow: RowParser[ExercisedEventRow] =
    sharedRow ~
      bool("exercise_consuming") ~
      str("exercise_choice") ~
      binaryStream("exercise_argument") ~
      int("exercise_argument_compression").? ~
      binaryStream("exercise_result").? ~
      int("exercise_result_compression").? ~
      array[String]("exercise_actors") ~
      array[String]("exercise_child_event_ids")

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

  def transactionsEventsSingleWildcardParty(
      startExclusive: Long,
      endInclusive: Long,
      party: Ref.Party,
      partyArrayContext: (String, String),
      witnessesWhereClause: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.partyToStatement
    SQL"""
            select #$selectColumnsForTransactions, #${partyArrayContext._1}$party#${partyArrayContext._2} as event_witnesses,
                   case when submitters = #${partyArrayContext._1}$party#${partyArrayContext._2} then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > $startExclusive
                  and event_sequential_id <= $endInclusive
                  and #$witnessesWhereClause
            order by event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def transactionsEventsSinglePartyWithTemplates(
      startExclusive: Long,
      endInclusive: Long,
      party: Ref.Party,
      partyArrayContext: (String, String),
      witnessesWhereClause: String,
      templateIds: Set[Ref.Identifier],
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.partyToStatement
    import com.daml.platform.store.Conversions.IdentifierToStatement
    SQL"""
            select #$selectColumnsForTransactions, #${partyArrayContext._1}$party#${partyArrayContext._2} as event_witnesses,
                   case when submitters = #${partyArrayContext._1}$party#${partyArrayContext._2} then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > $startExclusive
                  and event_sequential_id <= $endInclusive
                  and #$witnessesWhereClause
                  and template_id in ($templateIds)
            order by event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def transactionsEventsOnlyWildcardParties(
      startExclusive: Long,
      endInclusive: Long,
      filteredWitnessesClause: String,
      submittersInPartiesClause: String,
      witnessesWhereClause: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    SQL"""
            select #$selectColumnsForTransactions, #$filteredWitnessesClause as event_witnesses,
                   case when #$submittersInPartiesClause then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > $startExclusive
                  and event_sequential_id <= $endInclusive
                  and #$witnessesWhereClause
            order by event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def transactionsEventsSameTemplates(
      startExclusive: Long,
      endInclusive: Long,
      filteredWitnessesClause: String,
      submittersInPartiesClause: String,
      witnessesWhereClause: String,
      templateIds: Set[Ref.Identifier],
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.IdentifierToStatement
    SQL"""
            select #$selectColumnsForTransactions, #$filteredWitnessesClause as event_witnesses,
                   case when #$submittersInPartiesClause then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > $startExclusive
                  and event_sequential_id <= $endInclusive
                  and #$witnessesWhereClause
                  and template_id in ($templateIds)
            order by event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def transactionsEventsMixedTemplates(
      startExclusive: Long,
      endInclusive: Long,
      filteredWitnessesClause: String,
      submittersInPartiesClause: String,
      partiesAndTemplatesCondition: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    SQL"""
            select #$selectColumnsForTransactions, #$filteredWitnessesClause as event_witnesses,
                   case when #$submittersInPartiesClause then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > $startExclusive
                  and event_sequential_id <= $endInclusive
                  and #$partiesAndTemplatesCondition
            order by event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def transactionsEventsMixedTemplatesWithWildcardParties(
      startExclusive: Long,
      endInclusive: Long,
      filteredWitnessesClause: String,
      submittersInPartiesClause: String,
      witnessesWhereClause: String,
      partiesAndTemplatesCondition: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    SQL"""
            select #$selectColumnsForTransactions, #$filteredWitnessesClause as event_witnesses,
                   case when #$submittersInPartiesClause then command_id else '' end as command_id
            from participant_events
            where event_sequential_id > $startExclusive
                  and event_sequential_id <= $endInclusive
                  and (#$witnessesWhereClause or #$partiesAndTemplatesCondition)
            order by event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def activeContractsEventsSingleWildcardParty(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      party: Ref.Party,
      partyArrayContext: (String, String),
      witnessesWhereClause: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
      submittersInPartyClause: String,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.partyToStatement
    import com.daml.platform.store.Conversions.OffsetToStatement
    SQL"""select #$selectColumnsForACS, #${partyArrayContext._1}$party#${partyArrayContext._2} as event_witnesses,
                   case when #${submittersInPartyClause} then active_cs.command_id else '' end as command_id
            from participant_events active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > $startExclusive
                  and active_cs.event_sequential_id <= $endInclusiveSeq
                  and not exists (
                    select 1
                    from participant_events archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming
                      archived_cs.event_offset <= $endInclusiveOffset
                  )
                  and #$witnessesWhereClause
            order by active_cs.event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def activeContractsEventsSinglePartyWithTemplates(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      party: Ref.Party,
      partyArrayContext: (String, String),
      templateIds: Set[Ref.Identifier],
      witnessesWhereClause: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
      submittersInPartyClause: String,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.partyToStatement
    import com.daml.platform.store.Conversions.OffsetToStatement
    import com.daml.platform.store.Conversions.IdentifierToStatement
    // TODO BH: figure out why interpolation is required for oracle but bombs h2 and postgres
    SQL"""select #$selectColumnsForACS, #${partyArrayContext._1}$party#${partyArrayContext._2} as event_witnesses,
                   case when #${submittersInPartyClause} then active_cs.command_id else '' end as command_id
            from participant_events active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > $startExclusive
                  and active_cs.event_sequential_id <= $endInclusiveSeq
                  and not exists (
                    select 1
                    from participant_events archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming
                      archived_cs.event_offset <= $endInclusiveOffset
                  )
                  and #$witnessesWhereClause
                  and active_cs.template_id in ($templateIds)
            order by active_cs.event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def activeContractsEventsOnlyWildcardParties(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      filteredWitnessesClause: String,
      submittersInPartiesClause: String,
      witnessesWhereClause: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    //TODO BH: confirm as is not needed for any
    SQL"""select #$selectColumnsForACS, #$filteredWitnessesClause as event_witnesses,
                   case when #$submittersInPartiesClause then active_cs.command_id else '' end as command_id
            from participant_events active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > $startExclusive
                  and active_cs.event_sequential_id <= $endInclusiveSeq
                  and not exists (
                    select 1
                    from participant_events archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming
                      archived_cs.event_offset <= $endInclusiveOffset
                  )
                  and #$witnessesWhereClause
            order by active_cs.event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def activeContractsEventsSameTemplates(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      templateIds: Set[Ref.Identifier],
      filteredWitnessesClause: String,
      submittersInPartiesClause: String,
      witnessesWhereClause: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    import com.daml.platform.store.Conversions.IdentifierToStatement
    //TODO BH: confirm as is not needed for any
    SQL"""select #$selectColumnsForACS, #$filteredWitnessesClause as event_witnesses,
                   case when #$submittersInPartiesClause then active_cs.command_id else '' end as command_id
            from participant_events active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > $startExclusive
                  and active_cs.event_sequential_id <= $endInclusiveSeq
                  and not exists (
                    select 1
                    from participant_events archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming
                      archived_cs.event_offset <= $endInclusiveOffset
                  )
                  and #$witnessesWhereClause
                  and active_cs.template_id in ($templateIds)
            order by active_cs.event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def activeContractsEventsMixedTemplates(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      filteredWitnessesClause: String,
      submittersInPartiesClause: String,
      partiesAndTemplatesCondition: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    //TODO BH: confirm as is not needed for any
    SQL"""select #$selectColumnsForACS, #$filteredWitnessesClause as event_witnesses,
                   case when #$submittersInPartiesClause then active_cs.command_id else '' end as command_id
            from participant_events active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > $startExclusive
                  and active_cs.event_sequential_id <= $endInclusiveSeq
                  and not exists (
                    select 1
                    from participant_events archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming
                      archived_cs.event_offset <= $endInclusiveOffset
                  )
                  and #$partiesAndTemplatesCondition
            order by active_cs.event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def activeContractsEventsMixedTemplatesWithWildcardParties(
      startExclusive: Long,
      endInclusiveSeq: Long,
      endInclusiveOffset: Offset,
      filteredWitnessesClause: String,
      submittersInPartiesClause: String,
      witnessesWhereClause: String,
      partiesAndTemplatesCondition: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    //TODO BH: confirm as is not needed for any
    SQL"""select #$selectColumnsForACS, #$filteredWitnessesClause as event_witnesses,
                   case when #$submittersInPartiesClause then active_cs.command_id else '' end as command_id
            from participant_events active_cs
            where active_cs.event_kind = 10 -- create
                  and active_cs.event_sequential_id > $startExclusive
                  and active_cs.event_sequential_id <= $endInclusiveSeq
                  and not exists (
                    select 1
                    from participant_events archived_cs
                    where
                      archived_cs.contract_id = active_cs.contract_id and
                      archived_cs.event_kind = 20 and -- consuming
                      archived_cs.event_offset <= $endInclusiveOffset
                  )
                  and (#$witnessesWhereClause or #$partiesAndTemplatesCondition)
            order by active_cs.event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def flatTransactionSingleParty(
      transactionId: TransactionId,
      requestingParty: Ref.Party,
      partyArrayContext: (String, String),
      witnessesWhereClause: String,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.partyToStatement
    import com.daml.platform.store.Conversions.ledgerStringToStatement
    SQL"""select #$selectColumnsForTransactions, #${partyArrayContext._1}$requestingParty#${partyArrayContext._2} as event_witnesses,
                 case when submitters = #${partyArrayContext._1}$requestingParty#${partyArrayContext._2} then command_id else '' end as command_id
          from participant_events
          join parameters on
              (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
              and event_offset <= ledger_end
          where transaction_id = $transactionId and #$witnessesWhereClause and event_kind != 0 -- we do not want to fetch divulgence events
          order by event_sequential_id"""
      .asVectorOf(rawFlatEventParser)(connection)
  }

  def flatTransactionMultiParty(
      transactionId: TransactionId,
      witnessesWhereClause: String,
      submittersInPartiesClause: String,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.FlatEvent]] = {
    import com.daml.platform.store.Conversions.ledgerStringToStatement
    SQL"""select #$selectColumnsForTransactions, flat_event_witnesses as event_witnesses,
                 case when #$submittersInPartiesClause then command_id else '' end as command_id
          from participant_events
          join parameters on
              (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
              and event_offset <= ledger_end
          where transaction_id = $transactionId and #$witnessesWhereClause and event_kind != 0 -- we do not want to fetch divulgence events
          order by event_sequential_id"""
      .asVectorOf(rawFlatEventParser)(connection)
  }

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

  def transactionTreeSingleParty(
      transactionId: TransactionId,
      requestingParty: Ref.Party,
      partyArrayContext: (String, String),
      witnessesWhereClause: String,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] = {
    import com.daml.platform.store.Conversions.partyToStatement
    import com.daml.platform.store.Conversions.ledgerStringToStatement
    SQL"""select #$selectColumnsForTransactionTree, #${partyArrayContext._1}$requestingParty#${partyArrayContext._2} as event_witnesses,
                 event_kind = 20 as exercise_consuming,
                 case when submitters = #${partyArrayContext._1}$requestingParty#${partyArrayContext._2} then command_id else '' end as command_id
          from participant_events
          join parameters on
              (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
              and event_offset <= ledger_end
          where transaction_id = $transactionId and #$witnessesWhereClause and event_kind != 0 -- we do not want to fetch divulgence events
          order by node_index asc"""
      .asVectorOf(rawTreeEventParser)(connection)
  }

  def transactionTreeMultiParty(
      transactionId: TransactionId,
      witnessesWhereClause: String,
      submittersInPartiesClause: String,
      filteredWitnessesClause: String,
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] = {
    import com.daml.platform.store.Conversions.ledgerStringToStatement
    SQL"""select #$selectColumnsForTransactionTree, #$filteredWitnessesClause as event_witnesses,
                 event_kind = 20 as exercise_consuming,
                 case when #$submittersInPartiesClause then command_id else '' end as command_id
          from participant_events
          join parameters on
              (participant_pruned_up_to_inclusive is null or event_offset > participant_pruned_up_to_inclusive)
              and event_offset <= ledger_end
          where transaction_id = $transactionId and #$witnessesWhereClause and event_kind != 0 -- we do not want to fetch divulgence events
          order by node_index asc"""
      .asVectorOf(rawTreeEventParser)(connection)
  }

  def transactionTreeEventsSingleParty(
      startExclusive: Long,
      endInclusive: Long,
      requestingParty: Ref.Party,
      partyArrayContext: (String, String),
      witnessesWhereClause: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] = {
    import com.daml.platform.store.Conversions.partyToStatement
    SQL"""
        select #$selectColumnsForTransactionTree, #${partyArrayContext._1}$requestingParty#${partyArrayContext._2} as event_witnesses,
               event_kind = 20 as exercise_consuming,
               case when submitters = #${partyArrayContext._1}$requestingParty#${partyArrayContext._2} then command_id else '' end as command_id
        from participant_events
        where event_sequential_id > $startExclusive
              and event_sequential_id <= $endInclusive
              and #$witnessesWhereClause
              and event_kind != 0 -- we do not want to fetch divulgence events
        order by event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawTreeEventParser)(connection)
  }

  def transactionTreeEventsMultiParty(
      startExclusive: Long,
      endInclusive: Long,
      witnessesWhereClause: String,
      filteredWitnessesClause: String,
      submittersInPartiesClause: String,
      limitExpr: String,
      fetchSizeHint: Option[Int],
  )(connection: Connection): Vector[EventsTable.Entry[Raw.TreeEvent]] = {
    SQL"""
        select #$selectColumnsForTransactionTree, #$filteredWitnessesClause as event_witnesses,
               event_kind = 20 as exercise_consuming,
               case when #$submittersInPartiesClause then command_id else '' end as command_id
        from participant_events
        where event_sequential_id > $startExclusive
              and event_sequential_id <= $endInclusive
              and #$witnessesWhereClause
              and event_kind != 0 -- we do not want to fetch divulgence events
        order by event_sequential_id #$limitExpr"""
      .withFetchSize(fetchSizeHint)
      .asVectorOf(rawTreeEventParser)(connection)
  }

}
