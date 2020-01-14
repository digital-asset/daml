// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party, TransactionIdString}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
import com.digitalasset.ledger.api.health.ReportsHealth
import com.digitalasset.platform.logging.LoggingContext
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.resources.{Resource, ResourceOwner}
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState.Contract
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger.inmemory.InMemoryLedger
import com.digitalasset.platform.sandbox.stores.ledger.sql.{
  ReadOnlySqlLedger,
  SqlLedger,
  SqlStartMode
}
import com.digitalasset.platform.sandbox.stores.{InMemoryActiveLedgerState, InMemoryPackageStore}

import scala.concurrent.Future

trait Ledger extends ReadOnlyLedger with WriteLedger

trait WriteLedger extends ReportsHealth with AutoCloseable {

  def publishHeartbeat(time: Instant): Future[Unit]

  def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Future[SubmissionResult]

  // Party management
  def publishPartyAllocation(
      submissionId: SubmissionId,
      party: Party,
      displayName: Option[String]): Future[SubmissionResult]

  // Package management
  def uploadPackages(
      submissionId: SubmissionId,
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]): Future[SubmissionResult]

  // Configuration management
  def publishConfiguration(
      maxRecordTime: Timestamp,
      submissionId: String,
      config: Configuration
  ): Future[SubmissionResult]

}

/** Defines all the functionalities a Ledger needs to provide */
trait ReadOnlyLedger extends ReportsHealth with AutoCloseable {

  def ledgerId: LedgerId

  def ledgerEntries(offset: Option[Long]): Source[(Long, LedgerEntry), NotUsed]

  def ledgerEnd: Long

  def snapshot(filter: TemplateAwareFilter): Future[LedgerSnapshot]

  def lookupContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party): Future[Option[Contract]]

  def lookupKey(key: GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]]

  def lookupTransaction(
      transactionId: TransactionIdString): Future[Option[(Long, LedgerEntry.Transaction)]]

  // Party management
  def parties: Future[List[PartyDetails]]

  def partyEntries(beginOffset: Long): Source[(Long, PartyLedgerEntry), NotUsed]

  // Package management
  def listLfPackages(): Future[Map[PackageId, PackageDetails]]

  def getLfArchive(packageId: PackageId): Future[Option[Archive]]

  def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]]

  def packageEntries(beginOffset: Long): Source[(Long, PackageLedgerEntry), NotUsed]

  // Configuration management
  def lookupLedgerConfiguration(): Future[Option[(Long, Configuration)]]
  def configurationEntries(
      startInclusive: Option[Long]): Source[(Long, ConfigurationEntry), NotUsed]
}

object Ledger {

  type LedgerFactory = (InMemoryActiveLedgerState, Seq[LedgerEntry]) => Ledger

  /**
    * Creates an in-memory ledger
    *
    * @param ledgerId      the id to be used for the ledger
    * @param participantId the id of the participant
    * @param timeProvider  the provider of time
    * @param acs           the starting ACS store
    * @param ledgerEntries the starting entries
    * @return an in-memory Ledger
    */
  def inMemory(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
  ): ResourceOwner[Ledger] =
    ResourceOwner.successful(
      new InMemoryLedger(ledgerId, participantId, timeProvider, acs, packages, ledgerEntries))

  /**
    * Creates a JDBC backed ledger
    *
    * @param jdbcUrl       the jdbc url string containing the username and password as well
    * @param ledgerId      the id to be used for the ledger
    * @param participantId the participant identifier
    * @param timeProvider  the provider of time
    * @param acs           the starting ACS store
    * @param ledgerEntries the starting entries
    * @param queueDepth    the depth of the buffer for persisting entries. When gets full, the system will signal back-pressure upstream
    * @param startMode     whether the ledger should be reset, or continued where it was
    * @return a Postgres backed Ledger
    */
  def jdbcBacked(
      jdbcUrl: String,
      ledgerId: LedgerId,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      queueDepth: Int,
      startMode: SqlStartMode,
      metrics: MetricRegistry
  )(implicit mat: Materializer, ctx: LoggingContext): ResourceOwner[Ledger] =
    SqlLedger.owner(
      jdbcUrl,
      Some(ledgerId),
      participantId,
      timeProvider,
      acs,
      packages,
      ledgerEntries,
      queueDepth,
      startMode,
      metrics,
    )

  /**
    * Creates a JDBC backed read only ledger
    *
    * @param jdbcUrl       the jdbc url string containing the username and password as well
    * @param ledgerId      the id to be used for the ledger
    * @return a jdbc backed Ledger
    */
  def jdbcBackedReadOnly(
      jdbcUrl: String,
      ledgerId: LedgerId,
      metrics: MetricRegistry
  )(implicit mat: Materializer, ctx: LoggingContext): Resource[ReadOnlyLedger] =
    ReadOnlySqlLedger(jdbcUrl, Some(ledgerId), metrics)

  /** Wraps the given Ledger adding metrics around important calls */
  def metered(ledger: Ledger, metrics: MetricRegistry): Ledger = MeteredLedger(ledger, metrics)

  /** Wraps the given Ledger adding metrics around important calls */
  def meteredReadOnly(ledger: ReadOnlyLedger, metrics: MetricRegistry): ReadOnlyLedger =
    MeteredReadOnlyLedger(ledger, metrics)

}
