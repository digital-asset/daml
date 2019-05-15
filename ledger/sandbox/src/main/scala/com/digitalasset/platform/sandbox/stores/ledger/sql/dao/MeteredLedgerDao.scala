// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref.LedgerId
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry

import scala.collection.immutable
import scala.concurrent.Future

private class MeteredLedgerDao(ledgerDao: LedgerDao, mm: MetricsManager) extends LedgerDao {

  override def lookupLedgerId(): Future[Option[LedgerId]] =
    mm.timedFuture("LedgerDao:lookupLedgerId", ledgerDao.lookupLedgerId())

  override def lookupLedgerEnd(): Future[Long] =
    mm.timedFuture("LedgerDao:lookupLedgerEnd", ledgerDao.lookupLedgerEnd())

  override def lookupActiveContract(
      contractId: Value.AbsoluteContractId): Future[Option[Contract]] =
    mm.timedFuture("LedgerDao:lookupActiveContract", ledgerDao.lookupActiveContract(contractId))

  override def lookupLedgerEntry(offset: Long): Future[Option[LedgerEntry]] =
    mm.timedFuture("LedgerDao:lookupLedgerEntry", ledgerDao.lookupLedgerEntry(offset))

  override def lookupKey(key: Node.GlobalKey): Future[Option[Value.AbsoluteContractId]] =
    mm.timedFuture("LedgerDao:lookupKey", ledgerDao.lookupKey(key))

  override def getActiveContractSnapshot()(implicit mat: Materializer): Future[LedgerSnapshot] =
    ledgerDao.getActiveContractSnapshot()

  override def getLedgerEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, LedgerEntry), NotUsed] =
    ledgerDao.getLedgerEntries(startInclusive, endExclusive)

  override def storeLedgerEntry(
      offset: Long,
      newLedgerEnd: Long,
      ledgerEntry: PersistenceEntry): Future[PersistenceResponse] =
    mm.timedFuture(
      "storeLedgerEntry",
      ledgerDao.storeLedgerEntry(offset, newLedgerEnd, ledgerEntry))

  override def storeInitialState(
      activeContracts: immutable.Seq[Contract],
      ledgerEntries: immutable.Seq[(LedgerOffset, LedgerEntry)],
      newLedgerEnd: LedgerOffset
  ): Future[Unit] =
    mm.timedFuture(
      "storeInitialState",
      ledgerDao.storeInitialState(activeContracts, ledgerEntries, newLedgerEnd))

  override def initializeLedger(ledgerId: String, ledgerEnd: LedgerOffset): Future[Unit] =
    ledgerDao.initializeLedger(ledgerId, ledgerEnd)

  override def reset(): Future[Unit] =
    ledgerDao.reset()

  override def close(): Unit = {
    ledgerDao.close()
  }
}

object MeteredLedgerDao {
  def apply(ledgerDao: LedgerDao)(implicit mm: MetricsManager): LedgerDao =
    new MeteredLedgerDao(ledgerDao, mm)
}
