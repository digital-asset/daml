// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package db.migration.postgres

import java.sql.Connection
import java.time.Instant

import anorm.BatchSql
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.BlindingInfo
import com.daml.platform.events.EventIdFormatter
import com.daml.platform.store.serialization.{ContractSerializer, TransactionSerializer}
import db.migration.postgres.v27_backfill_participant_contracts._
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import scala.collection.mutable.ListBuffer

class V27__Backfill_Participant_Contracts extends BaseJavaMigration {

  private val SELECT_TRANSACTIONS = "select * from ledger_entries where typ='transaction'"
  private val SELECT_CONTRACTS_DIVULGED_IN_TRANSACTION =
    """select contract_divulgences.contract_id, contract_data.contract
      |from contract_divulgences
      |inner join contract_data on contract_data.id = contract_divulgences.contract_id
      |where contract_divulgences.transaction_id = ?""".stripMargin

  override def migrate(context: Context): Unit = {
    val conn = context.getConnection
    var loadTransactions: java.sql.Statement = null
    var loadContractsDivulgedInTransaction: java.sql.PreparedStatement = null
    var rows: java.sql.ResultSet = null
    var divulgedContractsRows: java.sql.ResultSet = null
    try {
      loadContractsDivulgedInTransaction =
        conn.prepareStatement(SELECT_CONTRACTS_DIVULGED_IN_TRANSACTION)
      loadTransactions = conn.createStatement()

      rows = loadTransactions.executeQuery(SELECT_TRANSACTIONS)

      while (rows.next()) {
        val transactionId = LedgerString.assertFromString(rows.getString("transaction_id"))
        val let = rows.getTimestamp("effective_at").toInstant

        val transaction = TransactionSerializer
          .deserializeTransaction(rows.getBinaryStream("transaction"))
          .getOrElse(sys.error(s"failed to deserialize transaction $transactionId"))
          .mapNodeId(EventIdFormatter.split(_).map(_.nodeId).get)

        loadContractsDivulgedInTransaction.setString(1, transactionId)
        divulgedContractsRows = loadContractsDivulgedInTransaction.executeQuery()
        val divulgedContractsBuilder = ListBuffer.newBuilder[(ContractId, Contract)]
        while (divulgedContractsRows.next()) {
          val rawContractId = divulgedContractsRows.getString("contract_id")
          val contractBytes = divulgedContractsRows.getBinaryStream("contract")
          val contract =
            ContractSerializer
              .deserializeContractInstance(contractBytes)
              .getOrElse(sys.error(
                s"failed to deserialize contract $rawContractId divulged in transaction $transactionId"))
          divulgedContractsBuilder += ((ContractId.assertFromString(rawContractId), contract))
        }
        val divulgedContracts = divulgedContractsBuilder.result()
        writeContracts(
          transaction = transaction,
          ledgerEffectiveTime = let,
          divulgedContracts = divulgedContracts,
        )(conn)
      }
    } finally {
      if (loadTransactions != null) {
        loadTransactions.close()
      }
      if (loadContractsDivulgedInTransaction != null) {
        loadContractsDivulgedInTransaction.close()
      }
      if (rows != null) {
        rows.close()
      }
      if (divulgedContractsRows != null) {
        divulgedContractsRows.close()
      }
    }
  }

  private def divulgedContracts(
      disclosure: DisclosureRelation,
      toBeInserted: Set[ContractId],
  ): PartialFunction[(NodeId, Node), (ContractId, Set[Party])] = {
    case (nodeId, c: Create) if toBeInserted(c.coid) =>
      c.coid -> disclosure(nodeId)
    case (nodeId, e: Exercise) if toBeInserted(e.targetCoid) =>
      e.targetCoid -> disclosure(nodeId)
    case (nodeId, f: Fetch) if toBeInserted(f.coid) =>
      f.coid -> disclosure(nodeId)
    case (nodeId, l: LookupByKey) if l.result.fold(false)(toBeInserted) =>
      l.result.get -> disclosure(nodeId)
  }

  private def divulgence(
      transaction: Transaction,
      disclosure: DisclosureRelation,
      toBeInserted: Set[ContractId],
  ): WitnessRelation[ContractId] =
    if (toBeInserted.isEmpty) {
      Map.empty
    } else {
      transaction.nodes.iterator
        .collect(divulgedContracts(disclosure, toBeInserted))
        .foldLeft[WitnessRelation[ContractId]](Map.empty)(Relation.merge)
    }

  private def prepareWitnessesBatch(
      insertions: Set[ContractId],
      deletions: Set[ContractId],
      transaction: Transaction,
      blinding: BlindingInfo,
  ): Option[BatchSql] = {
    val localDivulgence = divulgence(transaction, blinding.disclosure, insertions)
    val fullDivulgence = Relation.union(
      localDivulgence,
      blinding.globalDivulgence.filterKeys(!deletions.contains(_))
    )
    val insertWitnessesBatch = V27WitnessesTable.prepareBatchInsert(fullDivulgence)
    if (localDivulgence.nonEmpty) {
      require(insertWitnessesBatch.nonEmpty, "Illegal: inserting a contract without witnesses")
    }
    insertWitnessesBatch
  }

  private def writeContracts(
      transaction: Transaction,
      ledgerEffectiveTime: Instant,
      divulgedContracts: Iterable[(ContractId, Contract)],
  )(implicit connection: Connection): Unit = {

    val blinding = Blinding.blind(transaction)

    val contractBatches = V27ContractsTable.prepareBatchInsert(
      ledgerEffectiveTime = ledgerEffectiveTime,
      transaction = transaction,
      divulgedContracts = divulgedContracts,
    )

    for ((deleted, deleteContractsBatch) <- contractBatches.deletions) {
      val deleteWitnessesBatch = V27WitnessesTable.prepareBatchDelete(deleted.toSeq)
      require(deleteWitnessesBatch.nonEmpty, "Illegal: deleting witnesses without a contract")
      // Delete the witnesses first to respect the foreign key constraint of the underlying storage
      deleteWitnessesBatch.get.execute()
      deleteContractsBatch.execute()
    }

    for ((_, insertContractsBatch) <- contractBatches.insertions) {
      insertContractsBatch.execute()
    }

    // Insert the witnesses last to respect the foreign key constraint of the underlying storage.
    // Compute and insert new witnesses regardless of whether the current transaction adds new
    // contracts because it may be the case that we are only adding new witnesses to existing
    // contracts (e.g. via a fetch).
    val insertWitnessesBatch = prepareWitnessesBatch(
      insertions = contractBatches.insertions.fold(Set.empty[ContractId])(_._1),
      deletions = contractBatches.deletions.fold(Set.empty[ContractId])(_._1),
      transaction = transaction,
      blinding = blinding,
    )
    insertWitnessesBatch.foreach(_.execute())
  }

}
