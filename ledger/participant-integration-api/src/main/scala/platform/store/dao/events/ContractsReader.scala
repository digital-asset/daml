// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream
import java.time.Instant

import anorm.SqlParser._
import anorm.{RowParser, SqlParser, SqlStringInterpolation}
import com.codahale.metrics.Timer
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.interfaces.LedgerDaoContractsReader._
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.dao.events.SqlFunctions.{
  H2SqlFunctions,
  OracleSqlFunctions,
  PostgresSqlFunctions,
}
import com.daml.platform.store.dao.events.ContractsReader._
import com.daml.platform.store.interfaces.LedgerDaoContractsReader
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

  override def lookupContractKey(
      key: Key,
      readers: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[ContractId]] =
    Timed.future(
      metrics.daml.index.db.lookupKey, {
        val stakeholdersWhere =
          sqlFunctions.arrayIntersectionWhereClause("create_stakeholders", readers)

        dispatcher.executeSql(metrics.daml.index.db.lookupContractByKeyDbMetrics) {
          implicit connection =>
            SQL"""select pc.contract_id from #$contractsTable
                 where #$stakeholdersWhere and contract_witness in ($readers)
                 and #${sqlFunctions.equalsClause("create_key_hash")} ${key.hash} #${sqlFunctions
              .equalsClauseEnd()}
                 #${sqlFunctions.limitClause(1)}"""
              .as(contractId("contract_id").singleOpt)
        }
      },
    )

  override def lookupActiveContractAndLoadArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics) { implicit connection =>
          SQL"""select pc.contract_id, pc.template_id, pc.create_argument, pc.create_argument_compression from #$contractsTable
               where
               pc.contract_id = pcw.contract_id and
               pcw.contract_witness in ($readers) and pc.contract_id = $contractId #${sqlFunctions
            .limitClause(1)}"""
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
      readers: Set[Party],
      contractId: ContractId,
      createArgument: Value,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      dispatcher
        .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics) { implicit connection =>
          SQL"select participant_contracts.contract_id, template_id from #$contractsTable where contract_witness in ($readers) and participant_contracts.contract_id = $contractId #${sqlFunctions
            .limitClause(1)}"
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

  override def lookupKeyState(key: Key, validAt: Long)(implicit
      loggingContext: LoggingContext
  ): Future[KeyState] = {
    val _ = (key, validAt)
    Future.failed(NotSupportedError("lookupKeyState"))
  }

  override def lookupContractState(contractId: ContractId, before: Long)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractState]] = {
    val _ = (contractId, before)
    Future.failed(NotSupportedError("lookupContractState"))
  }
}

private[dao] object ContractsReader {
  private val contractsTable =
    "participant_contracts pc, participant_contract_witnesses pcw"
  private val contractWithoutValueRowParser: RowParser[String] =
    str("template_id")
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
      case DbType.Oracle => OracleSqlFunctions
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

  case class NotSupportedError(methodName: String)
      extends RuntimeException(s"Method $methodName not supported on the legacy index schema")
}
