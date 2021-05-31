// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.participant.state.v1.{DivulgedContract, Offset, SubmitterInfo}
import com.daml.lf.transaction.BlindingInfo
import com.daml.platform.indexer.OffsetStep
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.AsyncTestSuite

import scala.concurrent.Future

trait JdbcAtomicTransactionInsertion {
  self: JdbcLedgerDaoSuite with AsyncTestSuite =>

  private[dao] def store(
      submitterInfo: Option[SubmitterInfo],
      tx: LedgerEntry.Transaction,
      offsetStep: OffsetStep,
      divulgedContracts: List[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  ): Future[(Offset, LedgerEntry.Transaction)] = {
    val preparedTransactionInsert = ledgerDao.prepareTransactionInsert(
      submitterInfo,
      tx.workflowId,
      tx.transactionId,
      tx.ledgerEffectiveTime,
      offsetStep.offset,
      tx.transaction,
      divulgedContracts,
      blindingInfo,
    )
    for {
      _ <- ledgerDao.storeTransaction(
        preparedTransactionInsert,
        submitterInfo = submitterInfo,
        transactionId = tx.transactionId,
        transaction = tx.transaction,
        recordTime = tx.recordedAt,
        offsetStep = offsetStep,
        divulged = divulgedContracts,
        ledgerEffectiveTime = tx.ledgerEffectiveTime,
      )
    } yield offsetStep.offset -> tx
  }
}
