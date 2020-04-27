// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.{binaryStream, int, str}
import anorm.{Row, RowParser, SimpleSql, SqlStringInterpolation, ~}
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.serialization.ValueSerializer.{deserializeValue => deserialize}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
  * @see [[ContractsTable]]
  */
private[dao] sealed abstract class ContractsReader(
    dispatcher: DbDispatcher,
    executionContext: ExecutionContext,
) extends ContractStore {

  import ContractsReader._

  object InTransaction extends PostCommitValidationData {

    override def lookupContractKey(submitter: Party, key: Key)(
        implicit connection: Connection): Option[ContractId] =
      lookupContractKeyQuery(submitter, key).as(contractId("contract_id").singleOpt)

    override def lookupMaximumLedgerTime(ids: Set[ContractId])(
        implicit connection: Connection): Try[Option[Instant]] =
      SQL"select max(create_ledger_effective_time) as max_create_ledger_effective_time, count(*) as num_contracts from participant_contracts where participant_contracts.contract_id in ($ids)"
        .as(
          (instant("max_create_ledger_effective_time").? ~ int("num_contracts")).single
            .map {
              case result ~ numContracts if numContracts == ids.size => Success(result)
              case _ => Failure(notFound(ids))
            })

  }

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
      InTransaction.lookupContractKey(submitter, key)
    }

  override def lookupMaximumLedgerTime(ids: Set[ContractId]): Future[Option[Instant]] =
    dispatcher
      .executeSql("lookup_maximum_ledger_time") { implicit connection =>
        InTransaction.lookupMaximumLedgerTime(ids)
      }
      .map(_.get)(executionContext)

}

object ContractsReader {

  private[dao] def apply(
      dispatcher: DbDispatcher,
      executionContext: ExecutionContext,
      dbType: DbType,
  ): ContractsReader =
    dbType match {
      case DbType.Postgres => new Postgresql(dispatcher, executionContext)
      case DbType.H2Database => new H2Database(dispatcher, executionContext)
    }

  private final class Postgresql(dispatcher: DbDispatcher, executionContext: ExecutionContext)
      extends ContractsReader(dispatcher, executionContext) {
    override protected def lookupContractKeyQuery(
        submitter: Party,
        key: Key,
    ): SimpleSql[Row] =
      SQL"select participant_contracts.contract_id from #$contractsTable where $submitter =ANY(create_stakeholders) and contract_witness = $submitter and create_key_hash = ${key.hash}"
  }

  private final class H2Database(dispatcher: DbDispatcher, executionContext: ExecutionContext)
      extends ContractsReader(dispatcher, executionContext) {
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

  private def emptyContractIds: Throwable =
    new IllegalArgumentException(
      "Cannot lookup the maximum ledger time for an empty set of contract identifiers"
    )

  private def notFound(contractIds: Set[ContractId]): Throwable =
    new IllegalArgumentException(
      s"One or more of the following contract identifiers has been found: ${contractIds.map(_.coid).mkString(", ")}"
    )

}
