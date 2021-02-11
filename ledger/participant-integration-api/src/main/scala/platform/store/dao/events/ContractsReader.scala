// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream
import java.time.Instant

import anorm.SqlParser.{binaryStream, int, str}
import anorm.{Row, RowParser, SimpleSql, SqlParser, SqlStringInterpolation}
import com.codahale.metrics.Timer
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.dao.events.SqlFunctions.{H2SqlFunctions, PostgresSqlFunctions}
import com.daml.platform.store.serialization.{Compression, ValueSerializer}

import scala.concurrent.{ExecutionContext, Future}

/** @see [[ContractsTable]]
  */
private[dao] sealed class ContractsReader(

    dispatcher: DbDispatcher,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslation.Cache,
    sqlFunctions: SqlFunctions,
)(implicit ec: ExecutionContext)
    extends ContractStore {

  val committedContracts: PostCommitValidationData = ContractsTable

  import ContractsReader._

  private val contractRowParser: RowParser[(String, InputStream, Option[Int])] =
    str("template_id") ~ binaryStream("create_argument") ~ int(
      "create_argument_compression"
    ).? map SqlParser.flatten

  private val contractWithoutValueRowParser: RowParser[String] =
    str("template_id")

  /** Lookup of a contract in the case the contract value is not already known */
  private def lookupActiveContractAndLoadArgument(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
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
          createArgumentCompression = Compression.Algorithm.assertLookup(createArgumentCompression),
          decompressionTimer = metrics.daml.index.db.lookupActiveContractDbMetrics.compressionTimer,
          deserializationTimer =
            metrics.daml.index.db.lookupActiveContractDbMetrics.translationTimer,
        )
      })

  /** Lookup of a contract in the case the contract value is already known (loaded from a cache) */
  private def lookupActiveContractWithCachedArgument(
      readers: Set[Party],
      contractId: ContractId,
      createArgument: Value,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
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
      )

  override def lookupActiveContract(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit loggingContext: LoggingContext): Future[Option[Contract]] =
    // Depending on whether the contract argument is cached or not, submit a different query to the database
    lfValueTranslationCache.contracts
      .getIfPresent(LfValueTranslation.ContractCache.Key(contractId)) match {
      case Some(createArgument) =>
        lookupActiveContractWithCachedArgument(readers, contractId, createArgument.argument)
      case None =>
        lookupActiveContractAndLoadArgument(readers, contractId)

    }

  override def lookupContractKey(
      readers: Set[Party],
      key: Key,
  )(implicit loggingContext: LoggingContext): Future[Option[ContractId]] =
    dispatcher.executeSql(metrics.daml.index.db.lookupContractByKey) { implicit connection =>
      lookupContractKeyQuery(readers, key).as(contractId("contract_id").singleOpt)
    }

  private def lookupContractKeyQuery(readers: Set[Party], key: Key): SimpleSql[Row] = {
    val stakeholdersWhere =
      sqlFunctions.arrayIntersectionWhereClause("create_stakeholders", readers)
    SQL"select participant_contracts.contract_id from #$contractsTable where #$stakeholdersWhere and contract_witness in ($readers) and create_key_hash = ${key.hash} limit 1"
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
      lfValueTranslationCache: LfValueTranslation.Cache,
  )(implicit ec: ExecutionContext): ContractsReader = {
    def sqlFunctions = dbType match {
      case DbType.Postgres => PostgresSqlFunctions
      case DbType.H2Database => H2SqlFunctions
    }
    new ContractsReader(
      dispatcher = dispatcher,
      metrics = metrics,
      lfValueTranslationCache = lfValueTranslationCache,
      sqlFunctions = sqlFunctions,
    )
  }

  private val contractsTable = "participant_contracts natural join participant_contract_witnesses"

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
}
