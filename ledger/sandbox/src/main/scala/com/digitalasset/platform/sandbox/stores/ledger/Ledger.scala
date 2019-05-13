// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.backend.api.v1.{
  SubmissionResult,
  TransactionId,
  TransactionSubmission
}
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.stores.ActiveContracts.ActiveContract
import com.digitalasset.platform.sandbox.stores.ActiveContractsInMemory
import com.digitalasset.platform.sandbox.stores.ledger.inmemory.InMemoryLedger
import com.digitalasset.platform.sandbox.stores.ledger.sql.{SqlLedger, SqlStartMode}

import scala.collection.immutable
import scala.concurrent.Future

/** Defines all the functionalities a Ledger needs to provide */
trait Ledger extends AutoCloseable {

  def ledgerId: String

  def ledgerEntries(offset: Option[Long]): Source[(Long, LedgerEntry), NotUsed]

  def ledgerEnd: Long

  def snapshot(): Future[LedgerSnapshot]

  def lookupContract(contractId: Value.AbsoluteContractId): Future[Option[ActiveContract]]

  def lookupKey(key: GlobalKey): Future[Option[AbsoluteContractId]]

  def publishHeartbeat(time: Instant): Future[Unit]

  def publishTransaction(transactionSubmission: TransactionSubmission): Future[SubmissionResult]

  def lookupTransaction(
      transactionId: TransactionId): Future[Option[(Long, LedgerEntry.Transaction)]]
}

object Ledger {

  type LedgerFactory = (ActiveContractsInMemory, Seq[LedgerEntry]) => Ledger

  /**
    * Creates an in-memory ledger
    *
    * @param ledgerId      the id to be used for the ledger
    * @param timeProvider  the provider of time
    * @param acs           the starting ACS store
    * @param ledgerEntries the starting entries
    * @return an in-memory Ledger
    */
  def inMemory(
      ledgerId: String,
      timeProvider: TimeProvider,
      acs: ActiveContractsInMemory,
      ledgerEntries: Seq[LedgerEntry]): Ledger =
    new InMemoryLedger(ledgerId, timeProvider, acs, ledgerEntries)

  /**
    * Creates a Postgres backed ledger
    *
    * @param jdbcUrl       the jdbc url string containing the username and password as well
    * @param ledgerId      the id to be used for the ledger
    * @param timeProvider  the provider of time
    * @param ledgerEntries the starting entries
    * @param queueDepth    the depth of the buffer for persisting entries. When gets full, the system will signal back-pressure upstream
    * @param startMode     whether the ledger should be reset, or continued where it was
    * @return a Postgres backed Ledger
    */
  def postgres(
      jdbcUrl: String,
      ledgerId: String,
      timeProvider: TimeProvider,
      ledgerEntries: Seq[LedgerEntry],
      queueDepth: Int,
      startMode: SqlStartMode
  )(implicit mat: Materializer, mm: MetricsManager): Future[Ledger] =
    //TODO (robert): casting from Seq to immutable.Seq, make ledgerEntries immutable throughout the Sandbox?
    SqlLedger(
      jdbcUrl,
      Some(ledgerId),
      timeProvider,
      immutable.Seq(ledgerEntries: _*),
      queueDepth,
      startMode)

  /** Wraps the given Ledger adding metrics around important calls */
  def metered(ledger: Ledger)(implicit mm: MetricsManager): Ledger = MeteredLedger(ledger)

}
