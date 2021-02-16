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
    val createArgCompression = "createArgumentCompression"
  }

  private val insertContractQuery: SqlQuery = {
    import Params._
    anorm.SQL(s"""insert into participant_contracts(
       contract_id, template_id, create_argument, create_argument_compression, create_ledger_effective_time, create_key_hash, create_stakeholders
     )
     select
       contract_id, template_id, create_argument, create_argument_compression, create_ledger_effective_time, create_key_hash, string_to_array(create_stakeholders,'|')
     from
       unnest({$contractIds}, {$templateIds}, {$createArgs}, {$createArgCompression}, {$timestamps}, {$hashes}, {$stakeholders})
       as t(contract_id, template_id, create_argument, create_argument_compression, create_ledger_effective_time, create_key_hash, create_stakeholders)
            on conflict do nothing;""")
  }

  override def toExecutables(
      info: TransactionIndexing.ContractsInfo,
      serialized: TransactionIndexing.Compressed.Contracts,
  ): ContractsTable.Executables =
    ContractsTable.Executables(
      deleteContracts = buildDeletes(info),
      insertContracts = buildInserts(info, serialized),
    )

  private def buildInserts(
      contractsInfo: TransactionIndexing.ContractsInfo,
      serialized: TransactionIndexing.Compressed.Contracts,
  ): Option[Executable] = {
    import com.daml.platform.store.Conversions._
    import com.daml.platform.store.Conversions.IntToSmallIntConversions._

    val netCreates = contractsInfo.netCreates
    val netCreatesSize = netCreates.size
    val divulgedSize = contractsInfo.divulgedContracts.size
    val batchSize = netCreatesSize + divulgedSize

    val timestamps = Array.fill[Timestamp](netCreatesSize + divulgedSize)(null)

    val contractIds, templateIds, stakeholders = Array.ofDim[String](batchSize)
    val createArgs, hashes = Array.ofDim[Array[Byte]](batchSize)

    netCreates.iterator.zipWithIndex.foreach { case ((create, _), idx) =>
      contractIds(idx) = create.coid.coid
      templateIds(idx) = create.templateId.toString
      stakeholders(idx) = create.stakeholders.mkString("|")
      createArgs(idx) = serialized.createArguments(create.coid)
      timestamps(idx) = Timestamp.from(netCreates(create))
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
        createArgs(idx + netCreatesSize) = serialized.createArguments(contractId)
        hashes(idx + netCreatesSize) = null
    }

    val createArgCompressions =
      Array.fill[Option[Int]](batchSize)(serialized.createArgumentsCompression.id)

    if (batchSize > 0) Some {
      new InsertExecutable(
        insertContractQuery.on(
          Params.contractIds -> contractIds,
          Params.templateIds -> templateIds,
          Params.createArgs -> createArgs,
          Params.timestamps -> timestamps,
          Params.hashes -> hashes,
          Params.stakeholders -> stakeholders,
          Params.createArgCompression -> createArgCompressions,
        )
      )
    }
    else None
  }

  private class InsertExecutable(insertQuery: SimpleSql[Row]) extends Executable {
    override def execute()(implicit connection: Connection): Unit = {
      insertQuery.executeUpdate()
      ()
    }
  }

}
