// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.{Connection, Timestamp}
import anorm.{Row, SimpleSql, SqlQuery}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.transaction.{TransactionCoder, TransactionOuterClass}
import com.daml.lf.value.ValueCoder
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
      tx: TransactionIndexing.TransactionInfo,
      serialized: TransactionIndexing.Compressed.Contracts,
  ): ContractsTable.Executables = {
    ContractsTable.Executables(
      deleteContracts = buildDeletes(info),
      insertContracts = buildInserts(tx, info, serialized),
      nullifyPastKeys = buildNullifyPastKeys(info),
    )
  }

  private def buildInserts(
      tx: TransactionIndexing.TransactionInfo,
      contractsInfo: TransactionIndexing.ContractsInfo,
      serialized: TransactionIndexing.Compressed.Contracts,
  ): Executable = {
    import com.daml.platform.store.JdbcArrayConversions.IntToSmallIntConversions._
    import com.daml.platform.store.JdbcArrayConversions._

    val netCreatesSize = contractsInfo.netCreates.size
    val divulgedSize = contractsInfo.divulgedContracts.size
    val batchSize = netCreatesSize + divulgedSize

    val timestamp = java.sql.Timestamp.from(tx.ledgerEffectiveTime)
    val timestamps = Array.fill[Timestamp](netCreatesSize)(timestamp) ++ Array.fill[Timestamp](
      divulgedSize
    )(null)

    val contractIds, hashes, templateIds, stakeholders = Array.ofDim[String](batchSize)
    val createArgs = Array.ofDim[Array[Byte]](batchSize)

    contractsInfo.netCreates.iterator.zipWithIndex.foreach { case (create, idx) =>
      contractIds(idx) = create.coid.coid
      templateIds(idx) = create.templateId.toString
      stakeholders(idx) = create.stakeholders.mkString("|")
      createArgs(idx) = serialized.createArguments(create.coid)
      hashes(idx) = create.key
        .map(convertLfValueKey(create.templateId, _))
        .map(_.hash.bytes.toHexString)
        .orNull
    }

    contractsInfo.divulgedContracts.iterator.zipWithIndex.foreach {
      case (state.DivulgedContract(contractId, rawContractInstance), idx) =>
        val contractInstance = decodeContractInstance(
          TransactionOuterClass.ContractInstance.parseFrom(rawContractInstance)
        )
        contractIds(idx + netCreatesSize) = contractId.coid
        templateIds(idx + netCreatesSize) = contractInstance.template.toString
        stakeholders(idx + netCreatesSize) = ""
        createArgs(idx + netCreatesSize) = serialized.createArguments(contractId)
        hashes(idx + netCreatesSize) = null
    }

    val createArgCompressions =
      Array.fill[Option[Int]](batchSize)(serialized.createArgumentsCompression.id)

    val inserts = insertContractQuery.on(
      Params.contractIds -> contractIds,
      Params.templateIds -> templateIds,
      Params.createArgs -> createArgs,
      Params.timestamps -> timestamps,
      Params.hashes -> hashes,
      Params.stakeholders -> stakeholders,
      Params.createArgCompression -> createArgCompressions,
    )

    new InsertExecutable(inserts)
  }

  // FIXME, deduplicate
  private def decodeContractInstance(
      coinst: TransactionOuterClass.ContractInstance
  ): com.daml.lf.value.Value.VersionedContractInstance =
    assertDecode(TransactionCoder.decodeVersionedContractInstance(ValueCoder.CidDecoder, coinst))

  private def assertDecode[X](x: Either[ValueCoder.DecodeError, X]): X =
    x.fold(err => throw new IllegalStateException(err.errorMessage), identity)

  private class InsertExecutable(insertQuery: SimpleSql[Row]) extends Executable {
    override def execute()(implicit connection: Connection): Unit = {
      insertQuery.executeUpdate()
      ()
    }
  }
}
