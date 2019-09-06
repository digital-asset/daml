// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.TransactionId
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry

import scala.collection.immutable
import scala.concurrent.Future

private class MeteredLedgerReadDao(ledgerDao: LedgerReadDao, mm: MetricsManager)
    extends LedgerReadDao
    with AutoCloseable {
  override def lookupLedgerId(): Future[Option[LedgerId]] =
    mm.timedFuture("LedgerDao:lookupLedgerId", ledgerDao.lookupLedgerId())

  override def lookupLedgerEnd(): Future[Long] =
    mm.timedFuture("LedgerDao:lookupLedgerEnd", ledgerDao.lookupLedgerEnd())

  override def lookupExternalLedgerEnd(): Future[Option[LedgerString]] =
    mm.timedFuture("LedgerDao:lookupExternalLedgerEnd", ledgerDao.lookupExternalLedgerEnd())

  override def lookupActiveOrDivulgedContract(
      contractId: Value.AbsoluteContractId): Future[Option[Contract]] =
    mm.timedFuture(
      "LedgerDao:lookupActiveContract",
      ledgerDao.lookupActiveOrDivulgedContract(contractId))

  override def lookupLedgerEntry(offset: Long): Future[Option[LedgerEntry]] =
    mm.timedFuture("LedgerDao:lookupLedgerEntry", ledgerDao.lookupLedgerEntry(offset))

  override def lookupTransaction(
      transactionId: TransactionId): Future[Option[(LedgerOffset, LedgerEntry.Transaction)]] =
    mm.timedFuture("LedgerDao:lookupTransaction", ledgerDao.lookupTransaction(transactionId))

  override def lookupKey(key: Node.GlobalKey): Future[Option[Value.AbsoluteContractId]] =
    mm.timedFuture("LedgerDao:lookupKey", ledgerDao.lookupKey(key))

  override def getActiveContractSnapshot(untilExclusive: LedgerOffset)(
      implicit mat: Materializer): Future[LedgerSnapshot] =
    ledgerDao.getActiveContractSnapshot(untilExclusive)

  override def getLedgerEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, LedgerEntry), NotUsed] =
    ledgerDao.getLedgerEntries(startInclusive, endExclusive)

  override def getParties: Future[List[PartyDetails]] =
    mm.timedFuture("getParties", ledgerDao.getParties)

  override def listLfPackages: Future[Map[PackageId, PackageDetails]] =
    mm.timedFuture("listLfPackages", ledgerDao.listLfPackages)

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    mm.timedFuture("getLfArchive", ledgerDao.getLfArchive(packageId))

  override def close(): Unit = {
    ledgerDao.close()
  }
}

private class MeteredLedgerDao(ledgerDao: LedgerDao, mm: MetricsManager)
    extends MeteredLedgerReadDao(ledgerDao, mm)
    with LedgerDao {
  override def storeLedgerEntry(
      offset: Long,
      newLedgerEnd: Long,
      externalOffset: Option[ExternalOffset],
      ledgerEntry: PersistenceEntry): Future[PersistenceResponse] =
    mm.timedFuture(
      "storeLedgerEntry",
      ledgerDao.storeLedgerEntry(offset, newLedgerEnd, externalOffset, ledgerEntry))

  override def storeInitialState(
      activeContracts: immutable.Seq[Contract],
      ledgerEntries: immutable.Seq[(LedgerOffset, LedgerEntry)],
      newLedgerEnd: LedgerOffset
  ): Future[Unit] =
    mm.timedFuture(
      "storeInitialState",
      ledgerDao.storeInitialState(activeContracts, ledgerEntries, newLedgerEnd))

  override def initializeLedger(ledgerId: LedgerId, ledgerEnd: LedgerOffset): Future[Unit] =
    ledgerDao.initializeLedger(ledgerId, ledgerEnd)

  override def reset(): Future[Unit] =
    ledgerDao.reset()

  override def getParties: Future[List[PartyDetails]] =
    mm.timedFuture("getParties", ledgerDao.getParties)

  override def storeParty(
      party: Party,
      displayName: Option[String],
      externalOffset: Option[ExternalOffset]): Future[PersistenceResponse] =
    mm.timedFuture("storeParty", ledgerDao.storeParty(party, displayName, externalOffset))

  override def uploadLfPackages(
      uploadId: String,
      packages: List[(Archive, PackageDetails)],
      externalOffset: Option[ExternalOffset]): Future[Map[PersistenceResponse, Int]] =
    mm.timedFuture(
      "uploadLfPackages",
      ledgerDao.uploadLfPackages(uploadId, packages, externalOffset))

  override def close(): Unit = {
    ledgerDao.close()
  }
}

object MeteredLedgerDao {
  def apply(ledgerDao: LedgerDao)(implicit mm: MetricsManager): LedgerDao =
    new MeteredLedgerDao(ledgerDao, mm)

  def apply(ledgerDao: LedgerReadDao)(implicit mm: MetricsManager): LedgerReadDao =
    new MeteredLedgerReadDao(ledgerDao, mm)
}
