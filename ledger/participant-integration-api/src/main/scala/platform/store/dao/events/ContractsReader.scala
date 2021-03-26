// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream
import java.time.Instant

import anorm.SqlParser._
import anorm._
import anorm.{RowParser, SqlParser, SqlStringInterpolation}
import com.codahale.metrics.Timer
import com.daml.lf.transaction.GlobalKey
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.dao.events.ContractsReader._
import com.daml.platform.store.dao.events.SqlFunctions.{H2SqlFunctions, PostgresSqlFunctions}
import com.daml.platform.store.dao.events.contracts.LedgerDaoContractsReader
import com.daml.platform.store.dao.events.contracts.LedgerDaoContractsReader._
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

import scala.concurrent.{ExecutionContext, Future}

private[dao] sealed class ContractsReader(
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

  override def lookupContractState(contractId: ContractId, before: Long)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractState]] =
    Timed.future(
      metrics.daml.index.db.lookupContractState,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupContractStateDbMetrics) { implicit connection =>
          SQL"""
           SELECT
             template_id,
             flat_event_witnesses,
             create_argument,
             create_argument_compression,
             #$eventKindQueryClause,
             ledger_effective_time
           FROM participant_events
           WHERE
             contract_id = $contractId
             AND event_sequential_id <= $before
             AND (#$isCreateQueryPredicate OR #$isConsumingExercisePredicate)
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
                EventKindCreated,
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
                metrics.daml.index.db.lookupContractStateDbMetrics.compressionTimer,
              deserializationTimer =
                metrics.daml.index.db.lookupContractStateDbMetrics.translationTimer,
            )
            ActiveContract(
              contract,
              stakeholders,
              assertPresent(maybeCreateLedgerEffectiveTime)(
                "ledger_effective_time must be present for a create event"
              ),
            )
          case (_, stakeholders, _, _, EventKindArchived, _) => ArchivedContract(stakeholders)
          case (_, _, _, _, kind, _) => throw ContractsReaderError(s"Unexpected event kind $kind")
        }),
    )

  override def lookupActiveContractAndLoadArgument(
      contractId: ContractId,
      readers: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics) { implicit connection =>
          SQL"select participant_contracts.contract_id, template_id, create_argument, create_argument_compression from #$contractsTable where contract_witness in ($readers) and participant_contracts.contract_id = $contractId limit 1"
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

  override def lookupActiveContractWithCachedArgument(
      contractId: ContractId,
      readers: Set[Party],
      createArgument: Value,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
    Timed.future(
      metrics.daml.index.db.lookupActiveContractWithCachedArgument,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractWithCachedArgumentDbMetrics) {
          implicit connection =>
            SQL"select participant_contracts.contract_id, template_id from #$contractsTable where contract_witness in ($readers) and participant_contracts.contract_id = $contractId limit 1"
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

// @TODO Replace with implementation below once the participant_events.create_key_hash is available, as part of the append-only schema.
//  /** Lookup a contract key state at a specific ledger offset.
//    *
//    * @param key the contract key
//    * @param validAt the event_sequential_id of the ledger at which to query for the key state
//    * @return the key state.
//    */
//  override def lookupKeyStateAt(key: GlobalKey, validAt: Long)(implicit
//      loggingContext: LoggingContext
//  ): Future[Option[KeyState]] =
//    Timed.future(
//      metrics.daml.index.db.lookupKeyState,
//      dispatcher.executeSql(metrics.daml.index.db.lookupContractByKey) { implicit connection =>
//        SQL"""
//        WITH last_contract_key_create AS (
//               SELECT contract_id, flat_event_witnesses
//                 FROM participant_events events
//                 INNER JOIN participant_contracts contracts
//                 ON contracts.contract_id = events.contract_id
//                WHERE #$isCreateQueryPredicate -- create
//                  AND contracts.create_key_hash = ${key.hash}
//                  AND events.event_sequential_id <= $validAt
//                ORDER BY event_sequential_id DESC
//                LIMIT 1
//             )
//        SELECT contract_id, flat_event_witnesses
//          FROM last_contract_key_create -- creation only, as divulged contracts cannot be fetched by key
//        WHERE NOT EXISTS       -- check no archival visible
//               (SELECT 1
//                  FROM participant_events
//                 WHERE #$isConsumingExercisePredicate -- consuming exercise
//                   AND event_sequential_id <= $validAt
//                   AND contract_id = last_contract_key_create.contract_id
//               );
//       """.as(
//          (contractId("contract_id") ~ flatEventWitnessesColumn("flat_event_witnesses")).map {
//            case cId ~ stakeholders => KeyAssigned(cId, stakeholders)
//          }.singleOpt
//        )
//      },
//    )

  /** Lookup a contract's key state.
    *
    * NOTE: This method is a temporary stub for the version which uses `validAt` (see above) as a query restriction
    * on the append-only participant_events schema. It should NOT be used in production.
    * @param key     the contract key
    * @param validAt NOT USED
    * @return the key state.
    */
  override def lookupKeyState(key: GlobalKey, validAt: Long)(implicit
      loggingContext: LoggingContext
  ): Future[KeyState] = {
    val _ = validAt
    Timed.future(
      metrics.daml.index.db.lookupKeyState, {
        dispatcher
          .executeSql(metrics.daml.index.db.lookupKeyStateDbMetrics) { implicit connection =>
            SQL"select contract_id, create_stakeholders from participant_contracts where create_key_hash = ${key.hash} limit 1"
              .as(
                (contractId("contract_id") ~ flatEventWitnessesColumn(
                  "create_stakeholders"
                )).singleOpt
              )
              .map { case contractId ~ stakeholders => KeyAssigned(contractId, stakeholders) }
              .getOrElse(KeyUnassigned)
          }
      },
    )
  }

  override def lookupContractKey(
      key: Key,
      readers: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[ContractId]] =
    Timed.future(
      metrics.daml.index.db.lookupKey, {
        val stakeholdersWhere =
          sqlFunctions.arrayIntersectionWhereClause("create_stakeholders", readers)

        dispatcher.executeSql(metrics.daml.index.db.lookupContractByKey) { implicit connection =>
          SQL"select participant_contracts.contract_id from #$contractsTable where #$stakeholdersWhere and contract_witness in ($readers) and create_key_hash = ${key.hash} limit 1"
            .as(contractId("contract_id").singleOpt)
        }
      },
    )
}

private[dao] object ContractsReader {
  private val EventKindCreated = 10
  private val EventKindArchived = 20
  private val contractsTable =
    "participant_contracts natural join participant_contract_witnesses"
  // TODO use participant_events.event_kind once it lands with the append-only schema
  private val eventKindQueryClause =
    """
      |             CASE
      |              WHEN create_argument IS NOT NULL
      |                THEN 10
      |              WHEN exercise_consuming IS NOT NULL AND exercise_consuming = true
      |                THEN 20
      |             END event_kind
      |""".stripMargin
  // TODO use participant_events.event_kind once it lands with the append-only schema
  private val isCreateQueryPredicate = "create_argument IS NOT NULL"
  // TODO use participant_events.event_kind once it lands with the append-only schema
  private val isConsumingExercisePredicate =
    "exercise_consuming IS NOT NULL AND exercise_consuming = true"
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
      committedContracts = ContractsTable(dbType),
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
