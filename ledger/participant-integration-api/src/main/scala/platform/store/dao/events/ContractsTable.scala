// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.int
import anorm.{BatchSql, NamedParameter, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain.PartyDetails
import com.daml.platform.apiserver.execution.MissingContracts
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.JdbcLedgerDao

import scala.util.{Failure, Success, Try}

private[events] abstract class ContractsTable extends PostCommitValidationData {

  private val deleteContractQuery =
    s"delete from participant_contracts where contract_id = {contract_id}"

  private def deleteContract(contractId: ContractId): Vector[NamedParameter] =
    Vector[NamedParameter]("contract_id" -> contractId)

  // This query blanks out contract keys for all previous contracts irrespective of how they were created
  // a) for disclosed contracts
  // b) for contracts where parties hosted by this participant are stakeholders
  // The contracts covered by case b) are an empty set, considering the guarantees of the committer that
  // all transactions communicated via the update stream adhere to the ledger model i.e. all conflict on
  // contract keys have been resolved at this point. Such contracts must have been archived already
  // either in a previous or current transaction. What we are left with effectively are contracts covered
  // by case a).
  private val nullifyPastKeysQuery =
    s"update participant_contracts set create_key_hash = null where create_key_hash = {create_key_hash}"

  private def nullifyPastKeys(contractKeyHash: String): Vector[NamedParameter] =
    Vector[NamedParameter]("create_key_hash" -> contractKeyHash)

  def toExecutables(
      info: TransactionIndexing.ContractsInfo,
      tx: TransactionIndexing.TransactionInfo,
      serialized: TransactionIndexing.Compressed.Contracts,
  ): ContractsTable.Executables

  protected def buildDeletes(info: TransactionIndexing.ContractsInfo): Option[BatchSql] = {
    val deletes = info.netArchives.iterator.map(deleteContract).toSeq
    batch(deleteContractQuery, deletes)
  }

  protected def buildNullifyPastKeys(info: TransactionIndexing.ContractsInfo): Option[BatchSql] = {
    val nullifyPastKey = info.netCreates
      .flatMap(create =>
        create.key
          .map(convertLfValueKey(create.templateId, _))
          .map(_.hash.bytes.toHexString)
          .toList
      )
      .iterator
      .map(nullifyPastKeys)
      .toSeq
    batch(nullifyPastKeysQuery, nullifyPastKey)
  }

  override def lookupContractKeyGlobally(
      key: Key
  )(implicit connection: Connection): Option[ContractId] =
    SQL"select participant_contracts.contract_id from participant_contracts where create_key_hash = ${key.hash}"
      .as(contractId("contract_id").singleOpt)

  override final def lookupMaximumLedgerTime(
      ids: Set[ContractId]
  )(implicit connection: Connection): Try[Option[Instant]] =
    SQL"select max(create_ledger_effective_time) as max_create_ledger_effective_time, count(*) as num_contracts from participant_contracts where participant_contracts.contract_id in ($ids)"
      .as(
        (instant("max_create_ledger_effective_time").? ~ int("num_contracts")).single
          .map {
            case result ~ numContracts if numContracts == ids.size => Success(result)
            case _ => Failure(MissingContracts(ids))
          }
      )

  override final def lookupParties(parties: Seq[Party])(implicit
      connection: Connection
  ): List[PartyDetails] =
    JdbcLedgerDao.selectParties(parties).map(JdbcLedgerDao.constructPartyDetails)
}

private[events] object ContractsTable {

  trait Executable {
    def execute()(implicit connection: Connection): Unit
  }

  final case class Executables(
      deleteContracts: Option[BatchSql],
      insertContracts: Executable,
      nullifyPastKeys: Option[BatchSql],
  )

  def apply(dbType: DbType): ContractsTable =
    dbType match {
      case DbType.Postgres => ContractsTablePostgres
      case DbType.H2Database => ContractsTableH2
      case DbType.Oracle => ContractsTableOracle
    }
}
