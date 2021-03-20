// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection
import java.time.Instant

import anorm.{BatchSql, NamedParameter, SqlStringInterpolation}
import com.daml.ledger.participant.state.v1.DivulgedContract
import com.daml.platform.store.Conversions._
import com.daml.platform.store.dao.events.ContractsTable.Executable
import com.daml.platform.store.serialization.Compression
import com.daml.platform.store.OracleArrayConversions._

object ContractsTableOracle extends ContractsTable {

  private val insertContractQuery: String =
    s"""merge into participant_contracts using dual
       | on (contract_id = {contract_id})
       | when not matched then
       | insert (contract_id, template_id, create_argument, create_argument_compression, create_ledger_effective_time, create_key_hash, create_stakeholders)
       | values ({contract_id}, {template_id}, {create_argument}, {create_argument_compression}, {create_ledger_effective_time}, {create_key_hash}, {create_stakeholders})""".stripMargin

  override def toExecutables(
      info: TransactionIndexing.ContractsInfo,
      tx: TransactionIndexing.TransactionInfo,
      serialized: TransactionIndexing.Compressed.Contracts,
  ): ContractsTable.Executables = ContractsTable.Executables(
    deleteContracts = buildDeletes(info),
    insertContracts = buildInserts(tx, info, serialized),
  )

  override def lookupContractKeyGlobally(
      key: Key
  )(implicit connection: Connection): Option[ContractId] =
    SQL"select participant_contracts.contract_id from participant_contracts where DBMS_LOB.compare(create_key_hash, ${key.hash}) = 0"
      .as(contractId("contract_id").singleOpt)

  private def insertContract(
      contractId: ContractId,
      templateId: Identifier,
      createArgument: Array[Byte],
      ledgerEffectiveTime: Option[Instant],
      stakeholders: Set[Party],
      key: Option[Key],
      createArgumentCompression: Compression.Algorithm,
  ): Vector[NamedParameter] =
    Vector[NamedParameter](
      "contract_id" -> contractId,
      "template_id" -> templateId,
      "create_argument" -> createArgument,
      "create_ledger_effective_time" -> ledgerEffectiveTime,
      "create_stakeholders" -> stakeholders.toArray[String],
      "create_key_hash" -> key.map(_.hash),
      "create_argument_compression" -> createArgumentCompression.id,
    )

  def buildInserts(
      tx: TransactionIndexing.TransactionInfo,
      info: TransactionIndexing.ContractsInfo,
      serialized: TransactionIndexing.Compressed.Contracts,
  ): Executable = {
    val localInserts =
      for {
        create <- info.netCreates.iterator
      } yield insertContract(
        contractId = create.coid,
        templateId = create.templateId,
        createArgument = serialized.createArguments(create.coid),
        ledgerEffectiveTime = Some(tx.ledgerEffectiveTime),
        stakeholders = create.stakeholders,
        key = create.versionedKey.map(convert(create.templateId, _)),
        createArgumentCompression = serialized.createArgumentsCompression,
      )
    val divulgedInserts =
      for {
        DivulgedContract(contractId, contractInst) <- info.divulgedContracts.iterator
      } yield {
        insertContract(
          contractId = contractId,
          templateId = contractInst.template,
          createArgument = serialized.createArguments(contractId),
          ledgerEffectiveTime = None,
          stakeholders = Set.empty,
          key = None,
          createArgumentCompression = serialized.createArgumentsCompression,
        )
      }
    val inserts = localInserts.toVector ++ divulgedInserts.toVector
    new InsertContractsExecutable(batch(insertContractQuery, inserts))
  }

  private class InsertContractsExecutable(inserts: Option[BatchSql]) extends Executable {
    override def execute()(implicit connection: Connection): Unit = inserts.foreach(_.execute())
  }
}
