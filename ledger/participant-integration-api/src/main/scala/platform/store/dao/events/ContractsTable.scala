// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.SqlParser.int
import anorm.{BatchSql, NamedParameter, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain.PartyDetails
import com.daml.ledger.participant.state.v1.DivulgedContract
import com.daml.platform.store.Conversions._
import com.daml.platform.store.DbType
import com.daml.platform.store.dao.JdbcLedgerDao

import scala.util.{Failure, Success, Try}

private[events] sealed abstract class ContractsTable extends PostCommitValidationData {

  protected val insertContractQuery: String

  private val deleteContractQuery =
    s"delete from participant_contracts where contract_id = {contract_id}"

  private def deleteContract(contractId: ContractId): Vector[NamedParameter] =
    Vector[NamedParameter]("contract_id" -> contractId)

  private def insertContract(
      contractId: ContractId,
      templateId: Identifier,
      createArgument: Array[Byte],
      ledgerEffectiveTime: Option[Instant],
      stakeholders: Set[Party],
      key: Option[Key],
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "contract_id" -> contractId,
      "template_id" -> templateId,
      "create_argument" -> createArgument,
      "create_ledger_effective_time" -> ledgerEffectiveTime,
      "create_stakeholders" -> stakeholders.toArray[String],
      "create_key_hash" -> key.map(_.hash),
    )

  def toExecutables(
      tx: TransactionIndexing.TransactionInfo,
      info: TransactionIndexing.ContractsInfo,
      serialized: TransactionIndexing.Serialized,
  ): ContractsTable.Executables = {
    val deletes = info.netArchives.iterator.map(deleteContract).toSeq
    val localInserts =
      for {
        create <- info.netCreates.iterator
      } yield
        insertContract(
          contractId = create.coid,
          templateId = create.templateId,
          createArgument = serialized.createArgumentsByContract(create.coid),
          ledgerEffectiveTime = Some(tx.ledgerEffectiveTime),
          stakeholders = create.stakeholders,
          key = create.key.map(convert(create.templateId, _))
        )
    val divulgedInserts =
      for {
        DivulgedContract(contractId, contractInst) <- info.divulgedContracts.iterator
      } yield {
        insertContract(
          contractId = contractId,
          templateId = contractInst.template,
          createArgument = serialized.createArgumentsByContract(contractId),
          ledgerEffectiveTime = None,
          stakeholders = Set.empty,
          key = None,
        )
      }
    val inserts = localInserts.toVector ++ divulgedInserts.toVector
    ContractsTable.Executables(
      deleteContracts = batch(deleteContractQuery, deletes),
      insertContracts = batch(insertContractQuery, inserts),
    )
  }

  override final def lookupContractKeyGlobally(key: Key)(
      implicit connection: Connection): Option[ContractId] =
    SQL"select participant_contracts.contract_id from participant_contracts where create_key_hash = ${key.hash}"
      .as(contractId("contract_id").singleOpt)

  override final def lookupMaximumLedgerTime(ids: Set[ContractId])(
      implicit connection: Connection): Try[Option[Instant]] =
    if (ids.isEmpty) {
      Failure(ContractsTable.emptyContractIds)
    } else {
      SQL"select max(create_ledger_effective_time) as max_create_ledger_effective_time, count(*) as num_contracts from participant_contracts where participant_contracts.contract_id in ($ids)"
        .as(
          (instant("max_create_ledger_effective_time").? ~ int("num_contracts")).single
            .map {
              case result ~ numContracts if numContracts == ids.size => Success(result)
              case _ => Failure(ContractsTable.notFound(ids))
            })
    }

  override final def lookupParties(parties: Seq[Party])(
      implicit connection: Connection): List[PartyDetails] =
    JdbcLedgerDao.selectParties(parties).map(JdbcLedgerDao.constructPartyDetails)
}

private[events] object ContractsTable {

  final case class Executables(deleteContracts: Option[BatchSql], insertContracts: Option[BatchSql])

  def apply(dbType: DbType): ContractsTable =
    dbType match {
      case DbType.Postgres => Postgresql
      case DbType.H2Database => H2Database
    }

  object Postgresql extends ContractsTable {
    override protected val insertContractQuery: String =
      "insert into participant_contracts(contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders) values ({contract_id}, {template_id}, {create_argument}, {create_ledger_effective_time}, {create_key_hash}, {create_stakeholders}) on conflict do nothing"
  }

  object H2Database extends ContractsTable {
    override protected val insertContractQuery: String =
      s"merge into participant_contracts using dual on contract_id = {contract_id} when not matched then insert (contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders) values ({contract_id}, {template_id}, {create_argument}, {create_ledger_effective_time}, {create_key_hash}, {create_stakeholders})"
  }

  private def emptyContractIds: Throwable =
    new IllegalArgumentException(
      "Cannot lookup the maximum ledger time for an empty set of contract identifiers"
    )

  private def notFound(contractIds: Set[ContractId]): Throwable =
    new IllegalArgumentException(
      s"One or more of the following contract identifiers has been found: ${contractIds.map(_.coid).mkString(", ")}"
    )

}
