// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import com.daml.ledger.participant.state.v1.{DivulgedContract, Offset, SubmitterInfo}
import com.daml.lf.transaction.BlindingInfo
import com.daml.platform.indexer.{CurrentOffset, IncrementalOffsetStep, OffsetStep}
import com.daml.platform.store.dao.events.TransactionEntry
import com.daml.platform.store.entries.LedgerEntry
import org.scalatest.AsyncTestSuite

import scala.concurrent.Future

trait JdbcPipelinedTransactionInsertion {
  self: JdbcLedgerDaoSuite with AsyncTestSuite =>

  private[dao] def store(
      submitterInfo: Option[SubmitterInfo],
      tx: LedgerEntry.Transaction,
      offsetStep: OffsetStep,
      divulgedContracts: List[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  ): Future[(Offset, LedgerEntry.Transaction)] = {
    val _ = submitterInfo // TODO Tudor cleanup
    storeBatch(
      List((offsetStep -> tx, divulgedContracts, blindingInfo))
    ).map(_ => offsetStep.offset -> tx)
  }

  protected final def storeBatch(
      entries: List[
        ((OffsetStep, LedgerEntry.Transaction), List[DivulgedContract], Option[BlindingInfo])
      ]
  ): Future[Unit] = {
    val daoEntries = entries.map { case ((offsetStep, entry), divulgedContracts, blindingInfo) =>
      val maybeSubmitterInfo = submitterInfo(entry)
      TransactionEntry(
        maybeSubmitterInfo,
        entry.workflowId,
        entry.transactionId,
        entry.ledgerEffectiveTime,
        offsetStep,
        entry.transaction,
        divulgedContracts,
        blindingInfo,
      )
    }

    val preparedTransactionInsert =
      ledgerDao.prepareTransactionInsert(batchOffsetStep(entries.map(_._1._1)), daoEntries)
    for {
      _ <- ledgerDao.storeTransactionState(preparedTransactionInsert)
      _ <- ledgerDao.storeTransactionEvents(preparedTransactionInsert)
      _ <- ledgerDao.completeTransaction(preparedTransactionInsert)
    } yield ()
  }

  // TODO Tudor - duplicated (search for method name)
  private def batchOffsetStep(updates: Seq[OffsetStep]): OffsetStep =
    updates match {
      case Seq() => throw new RuntimeException("TransactionAccepted batch cannot be empty")
      case Seq(offsetStep) => offsetStep
      case multipleUpdates =>
        (multipleUpdates.head, multipleUpdates.last) match {
          case (
                IncrementalOffsetStep(lastUpdatedOffset, _),
                IncrementalOffsetStep(_, lastOffset),
              ) =>
            IncrementalOffsetStep(lastUpdatedOffset, lastOffset)
          case (_, IncrementalOffsetStep(_, lastOffset)) =>
            CurrentOffset(lastOffset)
          case invalidOffsetCombination =>
            throw new RuntimeException(
              s"Invalid batch offset combination encountered when constructing batch offset step: $invalidOffsetCombination"
            )
        }
    }
}
