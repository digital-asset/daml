// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream
import java.time.Instant

import anorm.SqlParser._
import anorm.{Row, RowParser, SimpleSql, SqlParser, SqlStringInterpolation, ~}
import com.codahale.metrics.Timer
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.Conversions.{contractId, _}
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.dao.events.SqlFunctions.{H2SqlFunctions, PostgresSqlFunctions}
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

import scala.concurrent.{ExecutionContext, Future}

/** @see [[ContractsTable]]
  */
sealed class ContractsReader(
    dispatcher: DbDispatcher,
    metrics: Metrics,
    sqlFunctions: SqlFunctions,
)(implicit ec: ExecutionContext)
    extends ContractStore {

  val committedContracts: PostCommitValidationData = ContractsTable

  import ContractsReader._

  private val contractRowParser: RowParser[(String, InputStream, Option[Int])] =
    str("template_id") ~ binaryStream("create_argument") ~ int(
      "create_argument_compression"
    ).? map SqlParser.flatten

  /** Lookup of a contract in the case the contract value is not already known */
  private def lookupActiveContractAndLoadArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] = {
    val tree_event_witnessesWhere =
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", readers)
    dispatcher
      .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics) { implicit connection =>
        SQL"""
  WITH archival_event AS (
         SELECT participant_events.*
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 20  -- consuming exercise
            AND event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$tree_event_witnessesWhere  -- only use visible archivals
          LIMIT 1
       ),
       create_event AS (
         SELECT contract_id, template_id, create_argument, create_argument_compression
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$tree_event_witnessesWhere
          LIMIT 1 -- limit here to guide planner wrt expected number of results
       ),
       -- no visibility check, as it is used to backfill missing template_id and create_arguments for divulged contracts
       create_event_unrestricted AS (
         SELECT contract_id, template_id, create_argument, create_argument_compression
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
          LIMIT 1 -- limit here to guide planner wrt expected number of results
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
           FROM participant_events AS divulgence_events LEFT OUTER JOIN create_event_unrestricted USING (contract_id),
                parameters
          WHERE divulgence_events.contract_id = $contractId -- restrict to aid query planner
            AND divulgence_events.event_kind = 0 -- divulgence
            AND divulgence_events.event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$tree_event_witnessesWhere
          ORDER BY divulgence_events.event_sequential_id
            -- prudent engineering: make results more stable by preferring earlier divulgence events
            -- Results might still change due to pruning.
          LIMIT 1
       ),
       create_and_divulged_contracts AS (
         (SELECT * FROM create_event)   -- prefer create over divulgance events
         UNION ALL
         (SELECT * FROM divulged_contract)
       )
  SELECT contract_id, template_id, create_argument, create_argument_compression
    FROM create_and_divulged_contracts
   WHERE NOT EXISTS (SELECT 1 FROM archival_event)
   LIMIT 1;""".as(contractRowParser.singleOpt)
      }
      .map(_.map { case (templateId, createArgument, createArgumentCompression) =>
        toContract(
          contractId = contractId,
          templateId = templateId,
          createArgument = createArgument,
          createArgumentCompression = Compression.Algorithm.assertLookup(createArgumentCompression),
          decompressionTimer = metrics.daml.index.db.lookupActiveContractDbMetrics.compressionTimer,
          deserializationTimer =
            metrics.daml.index.db.lookupActiveContractDbMetrics.translationTimer,
        )
      })
  }

  override def lookupActiveContract(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
    lookupActiveContractAndLoadArgument(readers, contractId)

  /** Returns the offset at which the key was last assigned for an active contract, otherwise the ledger end */
  override def lookupContractKey(key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Long, ContractId, Set[Party], Option[Long])]] =
    dispatcher.executeSql(metrics.daml.index.db.lookupContractByKey) { implicit connection =>
      lookupLastCreateKeyWithPotentialArchive(key).as(
        (long("created_at") ~ long("archived_at").? ~ contractId(
          "contract_id"
        ) ~ flatEventWitnessesColumn("create_event_witnesses")).map {
          case createdAt ~ maybeArchivedAt ~ contractId ~ flatEventWitnesses =>
            (createdAt, contractId, flatEventWitnesses, maybeArchivedAt)
        }.singleOpt
      )
    }

  private def lookupLastCreateKeyWithPotentialArchive(key: GlobalKey): SimpleSql[Row] =
    SQL"""SELECT
                creates.event_sequential_id as created_at,
                archives.event_sequential_id as archived_at,
                creates.contract_id as contract_id,
                creates.flat_event_witnesses as create_event_witnesses
             FROM participant_events creates
             LEFT JOIN participant_events archives ON creates.contract_id = archives.contract_id, parameters
            WHERE creates.event_kind = 10 -- create
              AND archives.event_kind = 20 -- consuming exercise
              AND archives.event_sequential_id <= parameters.ledger_end_sequential_id
              AND creates.create_key_hash = ${key.hash}
              AND creates.event_sequential_id <= parameters.ledger_end_sequential_id
            ORDER BY created_at DESC
            LIMIT 1
            """

  override def lookupContractKey(
      readers: Set[Party],
      key: Key,
  )(implicit loggingContext: LoggingContext): Future[Option[ContractId]] =
    dispatcher.executeSql(metrics.daml.index.db.lookupContractByKey) { implicit connection =>
      lookupContractKeyQuery(readers, key).as(contractId("contract_id").singleOpt)
    }

  private def lookupContractKeyQuery(readers: Set[Party], key: Key): SimpleSql[Row] = {
    def flat_event_witnessesWhere(columnPrefix: String) =
      sqlFunctions.arrayIntersectionWhereClause(s"$columnPrefix.flat_event_witnesses", readers)
    SQL"""
  WITH last_contract_key_create AS (
         SELECT participant_events.*
           FROM participant_events, parameters
          WHERE event_kind = 10 -- create
            AND create_key_hash = ${key.hash}
            AND event_sequential_id <= parameters.ledger_end_sequential_id
                -- do NOT check visibility here, as otherwise we do not abort the scan early
          ORDER BY event_sequential_id DESC
          LIMIT 1
       )
  SELECT contract_id
    FROM last_contract_key_create -- creation only, as divulged contracts cannot be fetched by key
  WHERE #${flat_event_witnessesWhere("last_contract_key_create")} -- check visibility only here
    AND NOT EXISTS       -- check no archival visible
         (SELECT 1
            FROM participant_events, parameters
           WHERE event_kind = 20 -- consuming exercise
             AND event_sequential_id <= parameters.ledger_end_sequential_id
             AND #${flat_event_witnessesWhere("participant_events")}
             AND contract_id = last_contract_key_create.contract_id
         );
       """
  }

  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    dispatcher
      .executeSql(metrics.daml.index.db.lookupMaximumLedgerTimeDbMetrics) { implicit connection =>
        committedContracts.lookupMaximumLedgerTime(ids)
      }
      .map(_.get)
}

private[dao] object ContractsReader {

  private[dao] def apply(
      dispatcher: DbDispatcher,
      dbType: DbType,
      metrics: Metrics,
  )(implicit ec: ExecutionContext): ContractsReader = {
    def sqlFunctions = dbType match {
      case DbType.Postgres => PostgresSqlFunctions
      case DbType.H2Database => H2SqlFunctions
    }
    new ContractsReader(
      dispatcher = dispatcher,
      metrics = metrics,
      sqlFunctions = sqlFunctions,
    )
  }

  // The contracts table _does not_ store agreement texts as they are
  // unnecessary for interpretation and validation. The contracts returned
  // from this table will _always_ have an empty agreement text.
  private def toContract(
      contractId: ContractId,
      templateId: String,
      createArgument: InputStream,
      createArgumentCompression: Compression.Algorithm,
      decompressionTimer: Timer,
      deserializationTimer: Timer,
  ): Contract = {
    val decompressed =
      Timed.value(
        timer = decompressionTimer,
        value = createArgumentCompression.decompress(createArgument),
      )
    val deserialized =
      Timed.value(
        timer = deserializationTimer,
        value = ValueSerializer.deserializeValue(
          stream = decompressed,
          errorContext = s"Failed to deserialize create argument for contract ${contractId.coid}",
        ),
      )
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = deserialized,
      agreementText = "",
    )
  }
}
