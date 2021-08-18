// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.{binaryStream, int, str}
import anorm.{RowParser, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.completion.Completion
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.platform.store.CompletionFromTransaction.toApiCheckpoint
import com.daml.platform.store.Conversions.{contractId, instant, offset}
import com.daml.platform.store.appendonlydao.events.{ContractId, Key}
import com.daml.platform.store.backend.StorageBackend
import com.google.rpc.status.Status

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
      applicationId: Ref.ApplicationId,
      submittersInPartiesClause: String,
  )(connection: Connection): List[CompletionStreamResponse] = {
    import com.daml.platform.store.Conversions.OffsetToStatement
    import com.daml.platform.store.Conversions.ledgerStringToStatement

    println("this is the query", s"""
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
        order by completion_offset asc""")
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
      participantTreeWitnessEventsWhereClause: String,
      divulgenceEventsTreeWitnessWhereClause: String,
      contractId: ContractId,
  )(connection: Connection): Option[StorageBackend.RawContract] = {
    import com.daml.platform.store.Conversions.ContractIdToStatement

    println("THIS IS THE QUERY1")
    SQL"""
  WITH archival_event AS (
         SELECT participant_events.*
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 20  -- consuming exercise
            AND event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$participantTreeWitnessEventsWhereClause  -- only use visible archivals
          FETCH NEXT 1 ROW ONLY
       ),
       create_event AS (
         SELECT contract_id, template_id, create_argument, create_argument_compression
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$participantTreeWitnessEventsWhereClause
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
           FROM participant_events divulgence_events LEFT OUTER JOIN create_event_unrestricted ON (divulgence_events.contract_id = create_event_unrestricted.contract_id),
                parameters
          WHERE divulgence_events.contract_id = $contractId -- restrict to aid query planner
            AND divulgence_events.event_kind = 0 -- divulgence
            AND divulgence_events.event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$divulgenceEventsTreeWitnessWhereClause
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
    println("THIS IS THE QUERY2")
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
           FROM participant_events divulgence_events LEFT OUTER JOIN create_event_unrestricted ON (divulgence_events.contract_id = create_event_unrestricted.contract_id),
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

}
