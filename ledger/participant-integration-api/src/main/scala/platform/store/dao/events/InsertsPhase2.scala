package com.daml.platform.store.dao.events

import java.sql.{Connection, PreparedStatement, Timestamp}

import anorm.{Row, SimpleSql, ToStatement}
import com.daml.ledger.participant.state.v1.DivulgedContract

object InsertsPhase2 {
  protected val TableName = "participant_contract_witnesses"
  protected val IdColumn = "contract_id"
  protected val WitnessColumn = "contract_witness"

  private implicit object ByteArrayArrayToStatement extends ToStatement[Array[Array[Byte]]] {
    override def set(s: PreparedStatement, index: Int, v: Array[Array[Byte]]): Unit =
      s.setObject(index, v)
  }

  private val insertContractQuery =
    anorm.SQL("""insert into participant_contracts(
       contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders
     )
     select
       contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, string_to_array(create_stakeholders,'|')
     from
       unnest({contractIds}, {templateIds}, {createArgs}, {timestamps}, {hashes}, {stakeholders})
       as t(contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders)
            on conflict do nothing;
     insert into participant_contract_witnesses(contract_id, contract_witness)
            select contract_id, contract_witness
            from unnest({witnessesContractIds}, {parties}) as t(contract_id, contract_witness)
            on conflict do nothing;
     """)

  def toExecutable(
      tx: TransactionIndexing.TransactionInfo,
      contractsInfo: TransactionIndexing.ContractsInfo,
      serialized: TransactionIndexing.Serialized,
      witnesses: TransactionIndexing.ContractWitnessesInfo): InsertsPhase2 = {
    val inserts = (_: Connection) => { // TODO conn not needed
      val netCreatesSize = contractsInfo.netCreates.size
      val divulgedSize = contractsInfo.divulgedContracts.size
      val batchSize = netCreatesSize + divulgedSize

      val timestamp = java.sql.Timestamp.from(tx.ledgerEffectiveTime)
      val timestamps = Array.fill[Timestamp](netCreatesSize)(timestamp) ++ Array.fill[Timestamp](
        divulgedSize)(null)

      val contractIds, templateIds, stakeholders = Array.ofDim[String](batchSize)
      val createArgs, hashes = Array.ofDim[Array[Byte]](batchSize)

      contractsInfo.netCreates.iterator.zipWithIndex.foreach {
        case (create, idx) =>
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
      val flattened: Iterator[(ContractId, String)] = Relation.flatten(witnesses.netVisibility)
      val (witnessesContractIds, parties) = flattened
        .map {
          case (id, party) => id.coid -> party
        }
        .toArray
        .unzip
//      val timestampsArr = conn.createArrayOf("TIMESTAMP", timestamps) // TODO MAYBE TIMESTAMPS SHOULD BE CONVERTED INTO STRINGS ???
//      timestampsArr

      insertContractQuery.on(
        "contractIds" -> contractIds,
        "templateIds" -> templateIds,
        "createArgs" -> createArgs,
        "timestamps" -> timestamps,
        "hashes" -> hashes,
        "stakeholders" -> stakeholders,
        "witnessesContractIds" -> witnessesContractIds,
        "parties" -> parties
      )
    }

    InsertsPhase2(inserts)
  }

//
//  val insertContractQuery =
//    SQL"""insert into participant_contracts(
//       contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders
//     )
//     select
//       contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, string_to_array(create_stakeholders,'|')
//     from
//       unnest(contractIds, templateIds, createArgs, timestampsArr, hashes, stakeholders)
//       as t(contract_id, template_id, create_argument, create_ledger_effective_time, create_key_hash, create_stakeholders)
//     on conflict do nothing;
//     insert into participant_contract_witnesses(contract_id, contract_witness)
//            select contract_id, contract_witness
//            from unnest(witnessesContractIds, parties) as t(contract_id, contract_witness)
//            on conflict do nothing;"""
}

case class InsertsPhase2(insert: Connection => SimpleSql[Row])
