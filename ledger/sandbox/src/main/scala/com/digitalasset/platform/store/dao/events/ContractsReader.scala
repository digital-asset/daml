// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.io.InputStream
import java.time.Instant

import anorm.SqlParser.{binaryStream, str}
import anorm.{Row, RowParser, SimpleSql, SqlParser, SqlStringInterpolation}
import com.codahale.metrics.Timer
import com.daml.ledger.participant.state.index.v2.ContractStore
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
)(implicit ec: ExecutionContext)
    extends ContractStore {

  import ContractsReader._

  private val contractRowParser: RowParser[(String, InputStream)] =
    str("template_id") ~ binaryStream("create_argument") map SqlParser.flatten

  protected def lookupContractKeyQuery(submitter: Party, key: Key): SimpleSql[Row]

  override def lookupActiveContract(
      submitter: Party,
      contractId: ContractId,
  ): Future[Option[Contract]] =
    dispatcher
      .executeSql(metrics.daml.index.db.lookupActiveContractDao) { implicit connection =>
        SQL"select participant_contracts.contract_id, template_id, create_argument from #$contractsTable where contract_witness = $submitter and participant_contracts.contract_id = $contractId"
          .as(contractRowParser.singleOpt)
      }
      .map(_.map {
        case (templateId, createArgument) =>
          toContract(
            contractId = contractId,
            templateId = templateId,
            createArgument = createArgument,
            deserializationTimer = metrics.daml.index.db.lookupActiveContractDao.translationTimer,
          )
      })

  override def lookupContractKey(
      submitter: Party,
      key: Key,
  ): Future[Option[ContractId]] =
    dispatcher.executeSql(metrics.daml.index.db.lookupContractByKey) { implicit connection =>
      lookupContractKeyQuery(submitter, key).as(contractId("contract_id").singleOpt)
    }

  override def lookupMaximumLedgerTime(ids: Set[ContractId]): Future[Option[Instant]] =
    dispatcher
      .executeSql(metrics.daml.index.db.lookupMaximumLedgerTimeDao) { implicit connection =>
        committedContracts.lookupMaximumLedgerTime(ids)
      }
      .map(_.get)

}

object ContractsReader {

  private[dao] def apply(
      dispatcher: DbDispatcher,
      dbType: DbType,
      metrics: Metrics,
  )(implicit ec: ExecutionContext): ContractsReader = {
    val table = ContractsTable(dbType)
    dbType match {
      case DbType.Postgres => new Postgresql(table, dispatcher, metrics)
      case DbType.H2Database => new H2Database(table, dispatcher, metrics)
    }
  }

  private final class Postgresql(
      table: ContractsTable,
      dispatcher: DbDispatcher,
      metrics: Metrics,
  )(implicit ec: ExecutionContext)
      extends ContractsReader(table, dispatcher, metrics) {
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
  )(implicit ec: ExecutionContext)
      extends ContractsReader(table, dispatcher, metrics) {
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

}
