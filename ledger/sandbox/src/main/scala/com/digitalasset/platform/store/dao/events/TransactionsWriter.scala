// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection

import anorm.BatchSql
import com.daml.ledger.participant.state.v1.Update.TransactionAccepted
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.{EventId, TransactionId}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.BlindingInfo
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.events.EventIdFormatter
import com.daml.platform.store.DbType

private[dao] object TransactionsWriter {

  final class PreparedInsert private[TransactionsWriter] (
      eventBatches: EventsTable.PreparedBatches,
      flatTransactionWitnessesBatch: Option[BatchSql],
      complementWitnessesBatch: Option[BatchSql],
      contractBatches: ContractsTable#PreparedBatches,
      contractWitnessesBatches: WitnessesTable[ContractId]#PreparedBatches,
  ) {
    def write(metrics: Metrics)(implicit connection: Connection): Unit = {
      import metrics.daml.index.db.storeTransactionDbMetrics

      Timed.value(storeTransactionDbMetrics.eventsBatch, eventBatches.foreach(_.execute()))
      flatTransactionWitnessesBatch.foreach(_.execute())
      complementWitnessesBatch.foreach(_.execute())

      // Delete the witnesses of contracts that being removed first, to
      // respect the foreign key constraint of the underlying storage
      Timed.value(
        storeTransactionDbMetrics.deleteContractWitnessesBatch,
        contractWitnessesBatches.deletions.map(_.execute()))
      for ((_, deleteContractsBatch) <- contractBatches.deletions) {
        Timed.value(storeTransactionDbMetrics.deleteContractsBatch, deleteContractsBatch.execute())
      }
      for ((_, insertContractsBatch) <- contractBatches.insertions) {
        Timed.value(storeTransactionDbMetrics.insertContractsBatch, insertContractsBatch.execute())
      }

      // Insert the witnesses last to respect the foreign key constraint of the underlying storage.
      // Compute and insert new witnesses regardless of whether the current transaction adds new
      // contracts because it may be the case that we are only adding new witnesses to existing
      // contracts (e.g. via divulging a contract with fetch).
      Timed.value(
        storeTransactionDbMetrics.insertContractWitnessesBatch,
        contractWitnessesBatches.insertions.foreach(_.execute()))
    }
  }

}

private[dao] final class TransactionsWriter(
    dbType: DbType,
    metrics: Metrics,
    lfValueTranslation: LfValueTranslation,
) {

  private val contractsTable = ContractsTable(dbType)
  private val contractWitnessesTable = WitnessesTable.ForContracts(dbType)

  private def computeDisclosureForFlatTransaction(
      transactionId: TransactionId,
      transaction: Transaction,
  ): WitnessRelation[EventId] =
    transaction.nodes.collect {
      case (nodeId, c: Create) =>
        EventIdFormatter.fromTransactionId(transactionId, nodeId) -> c.stakeholders
      case (nodeId, e: Exercise) if e.consuming =>
        EventIdFormatter.fromTransactionId(transactionId, nodeId) -> e.stakeholders
    }

  private def computeDisclosureForTransactionTree(
      transactionId: TransactionId,
      transaction: Transaction,
      blinding: BlindingInfo,
  ): WitnessRelation[EventId] = {
    val disclosed =
      transaction.nodes.collect {
        case p @ (_, _: Create) => p
        case p @ (_, _: Exercise) => p
      }.keySet
    blinding.disclosure.collect {
      case (nodeId, party) if disclosed(nodeId) =>
        EventIdFormatter.fromTransactionId(transactionId, nodeId) -> party
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

  private def fullDivulgence(
      toBeInserted: Set[ContractId],
      toBeDeleted: Set[ContractId],
      transient: Set[ContractId],
      transaction: Transaction,
      blinding: BlindingInfo,
  ): WitnessRelation[ContractId] = {
    val localDivulgence = divulgence(transaction, blinding.disclosure, toBeInserted)
    val fullDivulgence = Relation.union(
      localDivulgence,
      blinding.globalDivulgence.filterKeys(cid => !toBeDeleted(cid) && !transient(cid))
    )
    fullDivulgence
  }

  private case class AccumulatingBatches private[TransactionsWriter] (
      eventBatches: EventsTable.AccumulatingBatches,
      eventFlatWitnessesBatches: WitnessesTable.ForFlatTransactions.AccumulatingBatches,
      eventTreeWitnessesBatches: WitnessesTable.ForTransactionTrees.AccumulatingBatches,
      contractBatches: contractsTable.AccumulatingBatches,
      contractWitnessesBatches: contractWitnessesTable.AccumulatingBatches,
  ) {
    def add(offset: Offset, tx: TransactionAccepted): AccumulatingBatches = tx match {
      case TransactionAccepted(
          submitterInfo,
          transactionMeta,
          transaction,
          transactionId,
          _,
          divulgedContracts
          ) =>
        val blinding = Blinding.blind(transaction)

        // --------------------------------------------------------------------
        // Event data - append only
        // --------------------------------------------------------------------
        val newEventFlatWitnessesBatches = eventFlatWitnessesBatches.add(
          computeDisclosureForFlatTransaction(transactionId, transaction),
        )

        val newEventTreeWitnessesBatches = eventTreeWitnessesBatches.add(
          computeDisclosureForTransactionTree(transactionId, transaction, blinding),
        )

        val newEventBatches = EventsTable.prepareBatchInsert(
          submitterInfo = submitterInfo,
          workflowId = transactionMeta.workflowId,
          transactionId = transactionId,
          ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
          offset = offset,
          transaction = transaction,
          previous = eventBatches
        )

        // --------------------------------------------------------------------
        // Contract data - insert and delete
        // --------------------------------------------------------------------

        // First, collect contract insertions and deletions from the current transaction only
        val transactionContractBatches = contractsTable.prepareBatchInsert(
          ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime.toInstant,
          transaction = transaction,
          divulgedContracts = divulgedContracts,
        )

        // Then, compute witnesses changes from the above
        val contractWitnessesInsertions = fullDivulgence(
          toBeInserted = transactionContractBatches.insertions.keySet,
          toBeDeleted = transactionContractBatches.deletions.keySet,
          transient = transactionContractBatches.transientContracts,
          transaction = transaction,
          blinding = blinding,
        )
        val contractWitnessesDeletions = transactionContractBatches.deletions.keySet

        // Finally, merge all contract and witness changes with previously accumulated ones
        val newContractWitnessesBatches = contractWitnessesBatches.add(
          contractWitnessesInsertions,
          contractWitnessesDeletions,
        )
        val newContractBatches = contractBatches.add(transactionContractBatches)

        copy(
          eventBatches = newEventBatches,
          eventFlatWitnessesBatches = newEventFlatWitnessesBatches,
          eventTreeWitnessesBatches = newEventTreeWitnessesBatches,
          contractBatches = newContractBatches,
          contractWitnessesBatches = newContractWitnessesBatches,
        )
    }

    def prepare(): RawBatches = {
      new RawBatches(
        eventBatches = eventBatches.prepare,
        eventFlatWitnessesBatches = eventFlatWitnessesBatches,
        eventTreeWitnessesBatches = eventTreeWitnessesBatches,
        contractBatches = contractBatches.prepare,
        contractWitnessesBatches = contractWitnessesBatches,
      )
    }
  }

  private object AccumulatingBatches {
    def empty: AccumulatingBatches = AccumulatingBatches(
      eventBatches = EventsTable.AccumulatingBatches.empty,
      eventFlatWitnessesBatches = WitnessesTable.ForFlatTransactions.AccumulatingBatches.empty,
      eventTreeWitnessesBatches = WitnessesTable.ForTransactionTrees.AccumulatingBatches.empty,
      contractBatches = contractsTable.AccumulatingBatches.empty,
      contractWitnessesBatches = contractWitnessesTable.AccumulatingBatches.empty,
    )
  }

  private final class RawBatches private[TransactionsWriter] (
      eventBatches: EventsTable.RawBatches,
      eventFlatWitnessesBatches: WitnessesTable.ForFlatTransactions.AccumulatingBatches,
      eventTreeWitnessesBatches: WitnessesTable.ForTransactionTrees.AccumulatingBatches,
      contractBatches: contractsTable.RawBatches,
      contractWitnessesBatches: contractWitnessesTable.AccumulatingBatches,
  ) {
    def applySerialization(): SerializedBatches = {
      new SerializedBatches(
        eventBatches = eventBatches.applySerialization(lfValueTranslation),
        contractBatches = contractBatches.applySerialization(lfValueTranslation),
        eventFlatWitnessesBatches = eventFlatWitnessesBatches,
        eventTreeWitnessesBatches = eventTreeWitnessesBatches,
        contractWitnessesBatches = contractWitnessesBatches,
      )
    }
  }

  private final class SerializedBatches private[TransactionsWriter] (
      eventBatches: EventsTable.SerializedBatches,
      eventFlatWitnessesBatches: WitnessesTable.ForFlatTransactions.AccumulatingBatches,
      eventTreeWitnessesBatches: WitnessesTable.ForTransactionTrees.AccumulatingBatches,
      contractBatches: contractsTable.SerializedBatches,
      contractWitnessesBatches: contractWitnessesTable.AccumulatingBatches,
  ) {
    def applyBatching(): TransactionsWriter.PreparedInsert =
      new TransactionsWriter.PreparedInsert(
        eventBatches = eventBatches.applyBatching(),
        contractBatches = contractBatches.applyBatching(),
        flatTransactionWitnessesBatch = eventFlatWitnessesBatches.prepare.insertions,
        complementWitnessesBatch = eventTreeWitnessesBatches.prepare.insertions,
        contractWitnessesBatches = contractWitnessesBatches.prepare,
      )
  }

  def prepare(
      updates: List[(Offset, TransactionAccepted)],
  ): TransactionsWriter.PreparedInsert = {

    // Collect data from all transactions
    val raw = updates.foldLeft(AccumulatingBatches.empty) {
      case (acc, (offset, update)) => acc.add(offset, update)
    }

    // Turn data into partial SQL arguments
    val prepared = raw.prepare()

    // Apply serialization, returning full SQL arguments
    val serialized = Timed.value(
      metrics.daml.index.db.storeTransactionDbMetrics.translationTimer,
      prepared.applySerialization())

    // Convert lists of arguments into BatchSql objects
    serialized.applyBatching()
  }

}
