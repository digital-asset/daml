package com.daml.platform.store.dao.events

import java.sql.{Connection, PreparedStatement}

import com.daml.ledger.participant.state.v1.DivulgedContract

object InsertsPhase2 {
  protected val TableName = "participant_contract_witnesses"
  protected val IdColumn = "contract_id"
  protected val WitnessColumn = "contract_witness"

  def toExecutable(tx: TransactionIndexing.TransactionInfo, contractsInfo: TransactionIndexing.ContractsInfo, serialized: TransactionIndexing.Serialized, witnesses: TransactionIndexing.ContractWitnessesInfo): InsertsPhase2 = {
    val inserts = (conn: Connection) => {
      val netCreatesSize = contractsInfo.netCreates.size
      val divulgedSize = contractsInfo.divulgedContracts.size
      val batchSize = netCreatesSize + divulgedSize

      val timestamp = java.sql.Timestamp.from(tx.ledgerEffectiveTime)
      val timestamps = Array.fill[AnyRef](netCreatesSize)(timestamp) ++ Array.fill[AnyRef](divulgedSize)(null)

      val contractIds, templateIds, stakeholders = Array.ofDim[String](batchSize)
      val createArgs, hashes = Array.ofDim[Array[Byte]](batchSize)

      contractsInfo.netCreates.iterator.zipWithIndex.foreach { case (create, idx) =>
        contractIds(idx) = create.coid.coid
        templateIds(idx) = create.templateId.toString
        stakeholders(idx) = create.stakeholders.mkString("|")
        createArgs(idx) = serialized.createArgumentsByContract(create.coid)
        hashes(idx) = create.key.map(convert(create.templateId, _)).map(_.hash.bytes.toByteArray).orNull
      }

      contractsInfo.divulgedContracts.iterator.zipWithIndex.foreach {
        case (DivulgedContract(contractId, contractInst), idx) =>
          contractIds(idx + netCreatesSize) = contractId.coid
          templateIds(idx + netCreatesSize) = contractInst.template.toString
          stakeholders(idx + netCreatesSize) = ""
          createArgs(idx + netCreatesSize) = serialized.createArgumentsByContract(contractId)
          hashes(idx + netCreatesSize) = null
      }
      val flattened: Iterator[(ContractId, String)] = Relation.flatten(witnesses.netVisibility)
      val (witnessesContractIds, parties) = flattened.map {
        case (id, party) => id.coid -> party
      }.toArray.unzip

      val preparedStatement = conn.prepareStatement(insertContractQuery)
      preparedStatement.setObject(1, contractIds)
      preparedStatement.setObject(2, templateIds)
      preparedStatement.setObject(3, createArgs)
      preparedStatement.setArray(4, conn.createArrayOf("TIMESTAMP", timestamps))
      preparedStatement.setObject(5, hashes)
      preparedStatement.setObject(6, stakeholders)
      preparedStatement.setObject(7, witnessesContractIds)
      preparedStatement.setObject(8, parties)
      preparedStatement
    }

    InsertsPhase2(inserts)
  }

  val insertContractQuery =
    s"""insert into participant_contracts(
       contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders
     )
     select
       contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, string_to_array(create_stakeholders,'|')
     from
       unnest(?, ?, ?, ?, ?, ?)
       as t(contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders)
     on conflict do nothing;
     insert into $TableName($IdColumn, $WitnessColumn)
            select $IdColumn, $WitnessColumn
            from unnest(?, ?) as t($IdColumn, $WitnessColumn)
            on conflict do nothing;"""
}

case class InsertsPhase2(insert: Connection => PreparedStatement)
