// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream
import java.time.Instant

import anorm.SqlParser.{binaryStream, str}
import anorm.{Row, RowParser, SimpleSql, SqlParser, SqlStringInterpolation}
import com.codahale.metrics.Timer
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.logging.{ContextualizedLogger, ThreadLogger}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.serialization.ValueSerializer

import scala.concurrent.{ExecutionContext, Future}

/**
  * @see [[ContractsTable]]
  */
private[dao] sealed abstract class ContractsReader(
    val committedContracts: PostCommitValidationData,
    dispatcher: DbDispatcher,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslation.Cache,
)(implicit ec: ExecutionContext)
    extends ContractStore {

  private val logger = ContextualizedLogger.get(this.getClass)

  import ContractsReader._

  private val contractRowParser: RowParser[(String, InputStream)] =
    str("template_id") ~ binaryStream("create_argument") map SqlParser.flatten

  private val contractWithoutValueRowParser: RowParser[String] =
    str("template_id")

  private val translation: LfValueTranslation =
    new LfValueTranslation(lfValueTranslationCache)

  protected def lookupContractKeyQuery(submitter: Party, key: Key): SimpleSql[Row]

  /** Lookup of a contract in the case the contract value is not already known */
  private def lookupActiveContractAndLoadArgument(
      submitter: Party,
      contractId: ContractId,
  ): Future[Option[Contract]] = {
    ThreadLogger.traceThread("ContractsReader.lookupActiveContract")
    dispatcher
      .executeSql(metrics.daml.index.db.lookupActiveContractDbMetrics) { implicit connection =>
        SQL"select participant_contracts.contract_id, template_id, create_argument from #$contractsTable where contract_witness = $submitter and participant_contracts.contract_id = $contractId"
          .as(contractRowParser.singleOpt)
      }
      .map(_.map {
        case (templateId, createArgument) =>
          toContract(
            contractId = contractId,
            templateId = templateId,
            createArgument = createArgument,
            deserializationTimer =
              metrics.daml.index.db.lookupActiveContractDbMetrics.translationTimer,
          )
      })
  }

  /** Lookup of a contract in the case the contract value is already known (loaded from a cache) */
  private def lookupActiveContractWithCachedArgument(
      submitter: Party,
      contractId: ContractId,
      createArgument: Value,
  ): Future[Option[Contract]] =
    dispatcher
      .executeSql(metrics.daml.index.db.lookupActiveContractWithCachedArgumentDbMetrics) {
        implicit connection =>
          SQL"select participant_contracts.contract_id, template_id from #$contractsTable where contract_witness = $submitter and participant_contracts.contract_id = $contractId"
            .as(contractWithoutValueRowParser.singleOpt)
      }
      .map(
        _.map(
          templateId =>
            toContract(
              templateId = templateId,
              createArgument = createArgument,
          )))

  override def lookupActiveContract(
      submitter: Party,
      contractId: ContractId,
  ): Future[Option[Contract]] = {
    ThreadLogger.traceThread("ContractsReader.lookupActiveContract")
    // Depending on whether the contract argument is cached or not, submit a different query to the database
    translation.cache.contracts
      .getIfPresent(LfValueTranslation.ContractCache.Key(contractId)) match {
      case Some(createArgument) =>
        lookupActiveContractWithCachedArgument(submitter, contractId, createArgument.argument)
      case None =>
        lookupActiveContractAndLoadArgument(submitter, contractId)

    }
  }

  override def lookupContractKey(
      submitter: Party,
      key: Key,
  ): Future[Option[ContractId]] = {
    ThreadLogger.traceThread("ContractsReader.lookupContractKey")
    dispatcher.executeSql(metrics.daml.index.db.lookupContractByKey) { implicit connection =>
      lookupContractKeyQuery(submitter, key).as(contractId("contract_id").singleOpt)
    }
  }

  override def lookupMaximumLedgerTime(ids: Set[ContractId]): Future[Option[Instant]] = {
    ThreadLogger.traceThread("ContractsReader.lookupMaximumLedgerTime")
    dispatcher
      .executeSql(metrics.daml.index.db.lookupMaximumLedgerTimeDbMetrics) { implicit connection =>
        committedContracts.lookupMaximumLedgerTime(ids)
      }
      .map(_.get)
  }

}

object ContractsReader {

  private[dao] def apply(
      dispatcher: DbDispatcher,
      dbType: DbType,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslation.Cache,
  )(implicit ec: ExecutionContext): ContractsReader = {
    val table = ContractsTable(dbType)
    dbType match {
      case DbType.Postgres => new Postgresql(table, dispatcher, metrics, lfValueTranslationCache)
      case DbType.H2Database => new H2Database(table, dispatcher, metrics, lfValueTranslationCache)
    }
  }

  private final class Postgresql(
      table: ContractsTable,
      dispatcher: DbDispatcher,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslation.Cache,
  )(implicit ec: ExecutionContext)
      extends ContractsReader(table, dispatcher, metrics, lfValueTranslationCache) {
    override protected def lookupContractKeyQuery(
        submitter: Party,
        key: Key,
    ): SimpleSql[Row] =
      SQL"select participant_contracts.contract_id from #$contractsTable where $submitter =ANY(create_stakeholders) and contract_witness = $submitter and create_key_hash = ${key.hash}"
  }

  private final class H2Database(
      table: ContractsTable,
      dispatcher: DbDispatcher,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslation.Cache,
  )(implicit ec: ExecutionContext)
      extends ContractsReader(table, dispatcher, metrics, lfValueTranslationCache) {
    override protected def lookupContractKeyQuery(
        submitter: Party,
        key: Key,
    ): SimpleSql[Row] =
      SQL"select participant_contracts.contract_id from #$contractsTable where array_contains(create_stakeholders, $submitter) and contract_witness = $submitter and create_key_hash = ${key.hash}"
  }

  private val contractsTable = "participant_contracts natural join participant_contract_witnesses"

  // The contracts table _does not_ store agreement texts as they are
  // unnecessary for interpretation and validation. The contracts returned
  // from this table will _always_ have an empty agreement text.
  private def toContract(
      contractId: ContractId,
      templateId: String,
      createArgument: InputStream,
      deserializationTimer: Timer,
  ): Contract =
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = Timed.value(
        timer = deserializationTimer,
        value = ValueSerializer.deserializeValue(
          stream = createArgument,
          errorContext = s"Failed to deserialize create argument for contract ${contractId.coid}",
        ),
      ),
      agreementText = ""
    )

  private def toContract(
      templateId: String,
      createArgument: Value,
  ): Contract =
    Contract(
      template = Identifier.assertFromString(templateId),
      arg = createArgument,
      agreementText = ""
    )
}
