// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.{Connection, Timestamp}

import anorm.{Row, SimpleSql, SqlQuery}
import com.daml.ledger.participant.state.v1.DivulgedContract
import com.daml.platform.store.dao.events.ContractsTable.Executable

object ContractsTablePostgres extends ContractsTable {

  private object Params {
    val contractIds = "contractIds"
    val templateIds = "templateIds"
    val createArgs = "createArgs"
    val timestamps = "timestamps"
    val hashes = "hashes"
    val stakeholders = "stakeholders"
  }

  private val insertContractQuery: SqlQuery = {
    import Params._
    anorm.SQL(s"""insert into participant_contracts(
       contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders
     )
     select
       contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, string_to_array(create_stakeholders,'|')
     from
       unnest({$contractIds}, {$templateIds}, {$createArgs}, {$timestamps}, {$hashes}, {$stakeholders})
       as t(contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders)
            on conflict do nothing;""")
  }

  override def toExecutables(
      info: TransactionIndexing.ContractsInfo,
      tx: TransactionIndexing.TransactionInfo,
      serialized: TransactionIndexing.Serialized,
  ): ContractsTable.Executables = {
    ContractsTable.Executables(
      deleteContracts = buildDeletes(info),
      insertContracts = buildInserts(tx, info, serialized),
    )
  }

  private def buildInserts(
      tx: TransactionIndexing.TransactionInfo,
      contractsInfo: TransactionIndexing.ContractsInfo,
      serialized: TransactionIndexing.Serialized,
  ): Executable = {
    import com.daml.platform.store.Conversions._

    val netCreatesSize = contractsInfo.netCreates.size
    val divulgedSize = contractsInfo.divulgedContracts.size
    val batchSize = netCreatesSize + divulgedSize

    val timestamp = java.sql.Timestamp.from(tx.ledgerEffectiveTime)
    val timestamps = Array.fill[Timestamp](netCreatesSize)(timestamp) ++ Array.fill[Timestamp](
      divulgedSize
    )(null)

    val contractIds, templateIds, stakeholders = Array.ofDim[String](batchSize)
    val createArgs, hashes = Array.ofDim[Array[Byte]](batchSize)

    contractsInfo.netCreates.iterator.zipWithIndex.foreach { case (create, idx) =>
      contractIds(idx) = create.coid.coid
      templateIds(idx) = create.templateId.toString
      stakeholders(idx) = create.stakeholders.mkString("|")
      createArgs(idx) = serialized.createArgumentsByContract(create.coid)
      hashes(idx) = create.key
        .map(convertLfValueKey(create.templateId, _))
        .map(_.hash.bytes.toByteArray)
        .orNull
    }

    contractsInfo.divulgedContracts.iterator.zipWithIndex.foreach {
      case (DivulgedContract(contractId, contractInst), idx) =>
        contractIds(idx + netCreatesSize) = contractId.coid
        templateIds(idx + netCreatesSize) = contractInst.template.toString
        stakeholders(idx + netCreatesSize) = ""
        createArgs(idx + netCreatesSize) = serialized.createArgumentsByContract(contractId)
        hashes(idx + netCreatesSize) = null
    }

    val inserts = insertContractQuery.on(
      Params.contractIds -> contractIds,
      Params.templateIds -> templateIds,
      Params.createArgs -> createArgs,
      Params.timestamps -> timestamps,
      Params.hashes -> hashes,
      Params.stakeholders -> stakeholders,
    )

    new InsertExecutable(inserts)
  }

  private class InsertExecutable(insertQuery: SimpleSql[Row]) extends Executable {
    override def execute()(implicit connection: Connection): Unit = {
      insertQuery.executeUpdate()
      ()
    }
  }
}
