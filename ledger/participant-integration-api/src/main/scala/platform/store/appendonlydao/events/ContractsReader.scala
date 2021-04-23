// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.io.InputStream
import java.time.Instant

import anorm.SqlParser._
import anorm.{RowParser, SqlParser, SqlStringInterpolation, _}
import com.codahale.metrics.Timer
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.interfaces.LedgerDaoContractsReader._
import com.daml.platform.store.appendonlydao.events.ContractsReader._
import com.daml.platform.store.appendonlydao.events.SqlFunctions.{
  H2SqlFunctions,
  PostgresSqlFunctions,
}
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

import scala.concurrent.{ExecutionContext, Future}

private[appendonlydao] sealed class ContractsReader(
    val committedContracts: PostCommitValidationData,
    dispatcher: DbDispatcher,
    metrics: Metrics,
    sqlFunctions: SqlFunctions,
)(implicit ec: ExecutionContext)
    extends LedgerDaoContractsReader {

  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Instant]] =
    Timed.future(
      metrics.daml.index.db.lookupMaximumLedgerTime,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupMaximumLedgerTimeDbMetrics) { implicit connection =>
          committedContracts.lookupMaximumLedgerTime(ids)
        }
        .map(_.get),
    )

  /** Lookup a contract key state at a specific ledger offset.
    *
    * @param key the contract key
    * @param validAt the event_sequential_id of the ledger at which to query for the key state
    * @return the key state.
    */
  override def lookupKeyState(key: Key, validAt: Long)(implicit
      loggingContext: LoggingContext
  ): Future[KeyState] =
    Timed.future(
      metrics.daml.index.db.lookupKey,
      dispatcher.executeSql(metrics.daml.index.db.lookupContractByKeyDbMetrics) {
        implicit connection =>
          SQL"""
          WITH last_contract_key_create AS (
                 SELECT contract_id, flat_event_witnesses
                   FROM participant_events
                  WHERE event_kind = 10 -- create
                    AND create_key_hash = ${key.hash}
                    AND event_sequential_id <= $validAt
                  ORDER BY event_sequential_id DESC
                  LIMIT 1
               )
          SELECT contract_id, flat_event_witnesses
            FROM last_contract_key_create -- creation only, as divulged contracts cannot be fetched by key
          WHERE NOT EXISTS       -- check no archival visible
                 (SELECT 1
                    FROM participant_events
                   WHERE event_kind = 20 -- consuming exercise
                     AND event_sequential_id <= $validAt
                     AND contract_id = last_contract_key_create.contract_id
         );
         """
            .as(
              (contractId("contract_id") ~ flatEventWitnessesColumn("flat_event_witnesses")).map {
                case cId ~ stakeholders => KeyAssigned(cId, stakeholders)
              }.singleOpt
            )
            .getOrElse(KeyUnassigned)
      },
    )

  override def lookupContractState(contractId: ContractId, before: Long)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractState]] =
    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics) { implicit connection =>
          SQL"""
           SELECT
             template_id,
             flat_event_witnesses,
             create_argument,
             create_argument_compression,
             event_kind,
             ledger_effective_time
           FROM participant_events
           WHERE
             contract_id = $contractId
             AND event_sequential_id <= $before
             AND (event_kind = 10 OR event_kind = 20)
           ORDER BY event_sequential_id DESC
           LIMIT 1;
           """
            .as(fullDetailsContractRowParser.singleOpt)
        }
        .map(_.map {
          case (
                templateId,
                stakeholders,
                createArgument,
                createArgumentCompression,
                10,
                maybeCreateLedgerEffectiveTime,
              ) =>
            val contract = toContract(
              contractId = contractId,
              templateId =
                assertPresent(templateId)("template_id must be present for a create event"),
              createArgument =
                assertPresent(createArgument)("create_argument must be present for a create event"),
              createArgumentCompression =
                Compression.Algorithm.assertLookup(createArgumentCompression),
              decompressionTimer =
                metrics.daml.index.db.lookupActiveContractDbMetrics.compressionTimer,
              deserializationTimer =
                metrics.daml.index.db.lookupActiveContractDbMetrics.translationTimer,
            )
            ActiveContract(
              contract,
              stakeholders,
              assertPresent(maybeCreateLedgerEffectiveTime)(
                "ledger_effective_time must be present for a create event"
              ),
            )
          case (_, stakeholders, _, _, 20, _) => ArchivedContract(stakeholders)
          case (_, _, _, _, kind, _) => throw ContractsReaderError(s"Unexpected event kind $kind")
        }),
    )

  /** Lookup of a contract in the case the contract value is not already known */
  override def lookupActiveContractAndLoadArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] = {

    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics) { implicit connection =>
          lookupActiveContractAndLoadArgumentQuery(contractId, readers)
            .as(contractRowParser.singleOpt)
        }
        .map(_.map { case (templateId, createArgument, createArgumentCompression) =>
          toContract(
            contractId = contractId,
            templateId = templateId,
            createArgument = createArgument,
            createArgumentCompression =
              Compression.Algorithm.assertLookup(createArgumentCompression),
            decompressionTimer =
              metrics.daml.index.db.lookupActiveContractDbMetrics.compressionTimer,
            deserializationTimer =
              metrics.daml.index.db.lookupActiveContractDbMetrics.translationTimer,
          )
        }),
    )
  }

  /** Lookup of a contract in the case the contract value is already known (loaded from a cache) */
  override def lookupActiveContractWithCachedArgument(
      readers: Set[Party],
      contractId: ContractId,
      createArgument: Value,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] = {

    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics) { implicit connection =>
          lookupActiveContractWithCachedArgumentQuery(contractId, readers)
            .as(contractWithoutValueRowParser.singleOpt)
        }
        .map(
          _.map(templateId =>
            toContract(
              templateId = templateId,
              createArgument = createArgument,
            )
          )
        ),
    )
  }

  override def lookupContractKey(
      key: Key,
      readers: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[ContractId]] =
    Timed.future(
      metrics.daml.index.db.lookupKey,
      dispatcher.executeSql(metrics.daml.index.db.lookupContractByKeyDbMetrics) {
        implicit connection =>
          lookupContractKeyQuery(readers, key).as(contractId("contract_id").singleOpt)
      },
    )

  private def lookupActiveContractAndLoadArgumentQuery(
      contractId: ContractId,
      readers: Set[Party],
  ) = {
    val tree_event_witnessesWhere =
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", readers)
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
   LIMIT 1;"""
  }

  private def lookupActiveContractWithCachedArgumentQuery(
      contractId: ContractId,
      readers: Set[Party],
  ) = {
    val tree_event_witnessesWhere =
      sqlFunctions.arrayIntersectionWhereClause("tree_event_witnesses", readers)

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
         SELECT contract_id, template_id
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
            AND #$tree_event_witnessesWhere
          LIMIT 1 -- limit here to guide planner wrt expected number of results
       ),
       -- no visibility check, as it is used to backfill missing template_id and create_arguments for divulged contracts
       create_event_unrestricted AS (
         SELECT contract_id, template_id
           FROM participant_events, parameters
          WHERE contract_id = $contractId
            AND event_kind = 10  -- create
            AND event_sequential_id <= parameters.ledger_end_sequential_id
          LIMIT 1 -- limit here to guide planner wrt expected number of results
       ),
       divulged_contract AS (
         SELECT divulgence_events.contract_id,
                -- Note: the divulgance_event.template_id can be NULL
                -- for certain integrations. For example, the KV integration exploits that
                -- every participant node knows about all create events. The integration
                -- therefore only communicates the change in visibility to the IndexDB, but
                -- does not include a full divulgence event.
                COALESCE(divulgence_events.template_id, create_event_unrestricted.template_id)
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
         (SELECT * FROM create_event)   -- prefer create over divulgence events
         UNION ALL
         (SELECT * FROM divulged_contract)
       )
  SELECT contract_id, template_id
    FROM create_and_divulged_contracts
   WHERE NOT EXISTS (SELECT 1 FROM archival_event)
   LIMIT 1;
           """
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
}

private[appendonlydao] object ContractsReader {
  private val contractWithoutValueRowParser: RowParser[String] =
    str("template_id")

  private val fullDetailsContractRowParser: RowParser[
    (Option[String], Set[Party], Option[InputStream], Option[Int], Int, Option[Instant])
  ] =
    str("template_id").? ~ flatEventWitnessesColumn("flat_event_witnesses") ~ binaryStream(
      "create_argument"
    ).? ~ int(
      "create_argument_compression"
    ).? ~ int("event_kind") ~ get[Instant](
      "ledger_effective_time"
    )(anorm.Column.columnToInstant).? map SqlParser.flatten

  private val contractRowParser: RowParser[(String, InputStream, Option[Int])] =
    str("template_id") ~ binaryStream("create_argument") ~ int(
      "create_argument_compression"
    ).? map SqlParser.flatten

  private[appendonlydao] def apply(
      dispatcher: DbDispatcher,
      dbType: DbType,
      metrics: Metrics,
  )(implicit ec: ExecutionContext): ContractsReader = {
    def sqlFunctions = dbType match {
      case DbType.Postgres => PostgresSqlFunctions
      case DbType.H2Database => H2SqlFunctions
      case DbType.Oracle => throw new NotImplementedError("not yet supported")
    }
    new ContractsReader(
      committedContracts = ContractsTable,
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

  private def toContract(
      templateId: String,
      createArgument: Value,
  ): Contract =
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = createArgument,
      agreementText = "",
    )

  private def assertPresent[T](in: Option[T])(err: String): T =
    in.getOrElse(throw ContractsReaderError(err))

  case class ContractsReaderError(msg: String) extends RuntimeException(msg)
}
