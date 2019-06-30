// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
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

  override def initializeLedger(ledgerId: LedgerId, ledgerEnd: LedgerOffset): Future[Unit] =
    ledgerDao.initializeLedger(ledgerId, ledgerEnd)

  override def reset(): Future[Unit] =
    ledgerDao.reset()

  override def getParties: Future[List[PartyDetails]] =
    mm.timedFuture("getParties", ledgerDao.getParties)

  override def storeParty(party: Party, displayName: Option[String]): Future[PersistenceResponse] =
    mm.timedFuture("storeParty", ledgerDao.storeParty(party, displayName))

  override def listLfPackages: Future[Map[PackageId, PackageDetails]] =
    mm.timedFuture("listLfPackages", ledgerDao.listLfPackages)

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    mm.timedFuture("getLfArchive", ledgerDao.getLfArchive(packageId))

  override def uploadLfPackages(
      uploadId: String,
      packages: List[(Archive, PackageDetails)]): Future[PersistenceResponse] =
    mm.timedFuture("uploadLfPackages", ledgerDao.uploadLfPackages(uploadId, packages))

  override def close(): Unit = {
    ledgerDao.close()
  }
}

object MeteredLedgerDao {
  def apply(ledgerDao: LedgerDao)(implicit mm: MetricsManager): LedgerDao =
    new MeteredLedgerDao(ledgerDao, mm)
}
