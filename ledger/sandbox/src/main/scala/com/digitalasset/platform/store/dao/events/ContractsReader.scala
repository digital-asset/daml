// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.time.Instant

import anorm.SqlParser.{binaryStream, str}
import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.serialization.ValueSerializer.{deserializeValue => deserialize}

import scala.concurrent.{ExecutionContext, Future}

/**
  * @see [[ContractsTable]]
  */
private[dao] sealed abstract class ContractsReader(
    val committedContracts: PostCommitValidationData,
    dispatcher: DbDispatcher,
    executionContext: ExecutionContext,
) extends ContractStore {

  import ContractsReader._

  protected def lookupContractKeyQuery(submitter: Party, key: Key): SimpleSql[Row]

  override def lookupActiveContract(
      submitter: Party,
      contractId: ContractId,
  ): Future[Option[Contract]] =
    dispatcher.executeSql("lookup_active_contract") { implicit connection =>
      SQL"select participant_contracts.contract_id, template_id, create_argument from #$contractsTable where contract_witness = $submitter and participant_contracts.contract_id = $contractId"
        .as(contractRowParser.singleOpt)
    }

  override def lookupContractKey(
      submitter: Party,
      key: Key,
  ): Future[Option[ContractId]] =
    dispatcher.executeSql("lookup_contract_by_key") { implicit connection =>
      lookupContractKeyQuery(submitter, key).as(contractId("contract_id").singleOpt)
    }

  override def lookupMaximumLedgerTime(ids: Set[ContractId]): Future[Option[Instant]] =
    dispatcher
      .executeSql("lookup_maximum_ledger_time") { implicit connection =>
        committedContracts.lookupMaximumLedgerTime(ids)
      }
      .map(_.get)(executionContext)

}

object ContractsReader {

  private[dao] def apply(
      dispatcher: DbDispatcher,
      executionContext: ExecutionContext,
      dbType: DbType,
  ): ContractsReader = {
    val table = ContractsTable(dbType)
    dbType match {
      case DbType.Postgres => new Postgresql(table, dispatcher, executionContext)
      case DbType.H2Database => new H2Database(table, dispatcher, executionContext)
    }
  }

  private final class Postgresql(
      table: ContractsTable,
      dispatcher: DbDispatcher,
      executionContext: ExecutionContext,
  ) extends ContractsReader(table, dispatcher, executionContext) {
    override protected def lookupContractKeyQuery(
        submitter: Party,
        key: Key,
    ): SimpleSql[Row] =
      SQL"select participant_contracts.contract_id from #$contractsTable where $submitter =ANY(create_stakeholders) and contract_witness = $submitter and create_key_hash = ${key.hash}"
  }

  private final class H2Database(
      table: ContractsTable,
      dispatcher: DbDispatcher,
      executionContext: ExecutionContext,
  ) extends ContractsReader(table, dispatcher, executionContext) {
    override protected def lookupContractKeyQuery(
        submitter: Party,
        key: Key,
    ): SimpleSql[Row] =
      SQL"select participant_contracts.contract_id from #$contractsTable where array_contains(create_stakeholders, $submitter) and contract_witness = $submitter and create_key_hash = ${key.hash}"
  }

  // The contracts table _does not_ store agreement texts as they are
  // unnecessary for interpretation and validation. The contracts returned
  // from this table will _always_ have an empty agreement text.
  private val contractRowParser: RowParser[Contract] =
    str("contract_id") ~ str("template_id") ~ binaryStream("create_argument") map {
      case contractId ~ templateId ~ createArgument =>
        Contract(
          template = Identifier.assertFromString(templateId),
          arg = deserialize(
            stream = createArgument,
            errorContext = s"Failed to deserialize create argument for contract $contractId",
          ),
          agreementText = ""
        )
    }

  private val contractsTable = "participant_contracts natural join participant_contract_witnesses"

}
