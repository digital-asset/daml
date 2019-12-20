// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.v1.Configuration
import com.digitalasset.daml.lf.archive.Decode
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party, TransactionIdString}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.LedgerReadDao
import com.digitalasset.platform.sandbox.stores.ledger._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class BaseLedger(val ledgerId: LedgerId, headAtInitialization: Long, ledgerDao: LedgerReadDao)(
    implicit mat: Materializer)
    extends ReadOnlyLedger {

  implicit private val DEC: ExecutionContext = DirectExecutionContext

  protected final val dispatcher: Dispatcher[Long] = Dispatcher[Long](
    "sql-ledger",
    0l,
    headAtInitialization
  )

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupKey(key: Node.GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    ledgerDao.lookupKey(key, forParty)

  override def ledgerEntries(offset: Option[Long]): Source[(Long, LedgerEntry), NotUsed] =
    dispatcher.startingAt(offset.getOrElse(0), RangeSource(ledgerDao.getLedgerEntries(_, _)))

  override def ledgerEnd: Long = dispatcher.getHead()

  override def snapshot(filter: TemplateAwareFilter): Future[LedgerSnapshot] =
    // instead of looking up the latest ledger end, we can only take the latest known ledgerEnd in the scope of SqlLedger.
    // If we don't do that, we can miss contracts from a partially inserted batch insert of ledger entries
    // scenario:
    // 1. batch insert transactions A and B at offsets 5 and 6 respectively; A is a huge transaction, B is a small transaction
    // 2. B is inserted earlier than A and the ledger_end column in the parameters table is updated
    // 3. A GetActiveContractsRequest comes in and we look at the latest ledger_end offset in the database. We will see 6 (from transaction B).
    // 4. If we finish streaming the active contracts up to offset 6 before transaction A is properly inserted into the DB, the client will not see the contracts from transaction A
    // The fix to that is to use the latest known headRef, which is updated AFTER a batch has been inserted completely.
    ledgerDao
      .getActiveContractSnapshot(ledgerEnd, filter)
      .map(s => LedgerSnapshot(s.offset, s.acs))(DEC)

  override def lookupContract(
      contractId: AbsoluteContractId,
      forParty: Party): Future[Option[ActiveLedgerState.Contract]] =
    ledgerDao
      .lookupActiveOrDivulgedContract(contractId, forParty)

  override def lookupTransaction(
      transactionId: TransactionIdString): Future[Option[(Long, LedgerEntry.Transaction)]] =
    ledgerDao
      .lookupTransaction(transactionId)

  override def parties: Future[List[domain.PartyDetails]] =
    ledgerDao.getParties

  override def partyEntries(beginOffset: Long): Source[(Long, PartyLedgerEntry), NotUsed] =
    dispatcher.startingAt(beginOffset, RangeSource(ledgerDao.getPartyEntries))

  override def listLfPackages(): Future[Map[PackageId, v2.PackageDetails]] =
    ledgerDao.listLfPackages

  override def getLfArchive(packageId: PackageId): Future[Option[DamlLf.Archive]] =
    ledgerDao.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    ledgerDao
      .getLfArchive(packageId)
      .flatMap(archiveO =>
        Future.fromTry(Try(archiveO.map(archive => Decode.decodeArchive(archive)._2))))(DEC)

  override def packageEntries(beginOffset: Long): Source[(Long, PackageLedgerEntry), NotUsed] =
    dispatcher.startingAt(beginOffset, RangeSource(ledgerDao.getPackageEntries))

  override def lookupLedgerConfiguration(): Future[Option[(Long, Configuration)]] =
    ledgerDao.lookupLedgerConfiguration()

  override def configurationEntries(
      offset: Option[Long]): Source[(Long, ConfigurationEntry), NotUsed] =
    dispatcher.startingAt(offset.getOrElse(0), RangeSource(ledgerDao.getConfigurationEntries(_, _)))

  override def close(): Unit = {
    dispatcher.close()
    ledgerDao.close()
  }

}
