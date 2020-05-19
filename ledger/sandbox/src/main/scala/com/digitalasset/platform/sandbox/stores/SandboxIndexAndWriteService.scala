// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores

import java.time.Instant
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.api.util.TimeProvider
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.v1.{
  ApplicationId => _,
  LedgerId => _,
  TransactionId => _,
  _
}
import com.daml.ledger.participant.state.{v1 => ParticipantState}
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{ImmArray, Time}
import com.daml.lf.transaction.TransactionCommitter
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.ServerRole
import com.daml.platform.index.LedgerBackedIndexService
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.LedgerIdGenerator
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.sandbox.stores.ledger._
import com.daml.platform.sandbox.stores.ledger.inmemory.InMemoryLedger
import com.daml.platform.sandbox.stores.ledger.sql.{SqlLedger, SqlStartMode}
import com.daml.resources.{Resource, ResourceOwner}
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

trait IndexAndWriteService {
  def indexService: IndexService

  def writeService: WriteService
}

object SandboxIndexAndWriteService {
  //TODO: internalise the template store as well
  private val logger = LoggerFactory.getLogger(SandboxIndexAndWriteService.getClass)

  def postgres(
      ledgerId: LedgerIdMode,
      participantId: ParticipantId,
      jdbcUrl: String,
      initialConfig: ParticipantState.Configuration,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      startMode: SqlStartMode,
      queueDepth: Int,
      transactionCommitter: TransactionCommitter,
      templateStore: InMemoryPackageStore,
      eventsPageSize: Int,
      metrics: Metrics,
  )(implicit mat: Materializer, logCtx: LoggingContext): ResourceOwner[IndexAndWriteService] =
    SqlLedger
      .owner(
        serverRole = ServerRole.Sandbox,
        jdbcUrl = jdbcUrl,
        ledgerId = ledgerId,
        participantId = participantId,
        timeProvider = timeProvider,
        acs = acs,
        packages = templateStore,
        initialLedgerEntries = ledgerEntries,
        queueDepth = queueDepth,
        transactionCommitter = transactionCommitter,
        startMode = startMode,
        eventsPageSize = eventsPageSize,
        metrics = metrics,
      )
      .flatMap(ledger =>
        owner(MeteredLedger(ledger, metrics), participantId, initialConfig, timeProvider))

  def inMemory(
      ledgerId: LedgerIdMode,
      participantId: ParticipantId,
      intialConfig: ParticipantState.Configuration,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      transactionCommitter: TransactionCommitter,
      templateStore: InMemoryPackageStore,
      metrics: Metrics,
  )(implicit mat: Materializer): ResourceOwner[IndexAndWriteService] = {
    val ledger =
      new InMemoryLedger(
        ledgerId.or(LedgerIdGenerator.generateRandomId()),
        participantId,
        timeProvider,
        acs,
        transactionCommitter,
        templateStore,
        ledgerEntries,
      )
    owner(MeteredLedger(ledger, metrics), participantId, intialConfig, timeProvider)
  }

  private def owner(
      ledger: Ledger,
      participantId: ParticipantId,
      initialConfig: Configuration,
      timeProvider: TimeProvider,
  )(implicit mat: Materializer): ResourceOwner[IndexAndWriteService] = {
    val indexSvc = new LedgerBackedIndexService(ledger, participantId) {
      override def getLedgerConfiguration(): Source[LedgerConfiguration, NotUsed] =
        Source
          .single(LedgerConfiguration(initialConfig.maxDeduplicationTime))
          .concat(Source.future(Promise[LedgerConfiguration]().future)) // we should keep the stream open!
    }
    val writeSvc = new LedgerBackedWriteService(ledger, timeProvider)

    for {
      _ <- new HeartbeatScheduler(
        TimeProvider.UTC,
        10.minutes,
        "deduplication cache maintenance",
        ledger.removeExpiredDeduplicationData)
    } yield
      new IndexAndWriteService {
        override val indexService: IndexService = indexSvc

        override val writeService: WriteService = writeSvc
      }
  }

  private class HeartbeatScheduler(
      timeProvider: TimeProvider,
      interval: FiniteDuration,
      name: String,
      onTimeChange: Instant => Future[Unit],
  )(implicit mat: Materializer)
      extends ResourceOwner[Unit] {

    override def acquire()(implicit executionContext: ExecutionContext): Resource[Unit] =
      timeProvider match {
        case timeProvider: TimeProvider.UTC.type =>
          Resource(Future {
            logger.debug(s"Scheduling $name in intervals of {}", interval)
            Source
              .tick(0.seconds, interval, ())
              .mapAsync[Unit](1)(
                _ => onTimeChange(timeProvider.getCurrentTime)
              )
              .to(Sink.ignore)
              .run()
          })(
            cancellable =>
              Future {
                val _ = cancellable.cancel()
            }
          ).map(_ => ())
        case _ =>
          Resource.unit
      }
  }
}

class LedgerBackedWriteService(ledger: Ledger, timeProvider: TimeProvider) extends WriteService {

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def submitTransaction(
      submitterInfo: ParticipantState.SubmitterInfo,
      transactionMeta: ParticipantState.TransactionMeta,
      transaction: SubmittedTransaction): CompletionStage[ParticipantState.SubmissionResult] =
    FutureConverters.toJava(ledger.publishTransaction(submitterInfo, transactionMeta, transaction))

  override def allocateParty(
      hint: Option[Party],
      displayName: Option[String],
      submissionId: SubmissionId): CompletionStage[SubmissionResult] = {
    val party = hint.getOrElse(PartyIdGenerator.generateRandomId())
    FutureConverters.toJava(ledger.publishPartyAllocation(submissionId, party, displayName))
  }

  // WritePackagesService
  override def uploadPackages(
      submissionId: SubmissionId,
      payload: List[Archive],
      sourceDescription: Option[String]
  ): CompletionStage[SubmissionResult] =
    FutureConverters.toJava(
      ledger.uploadPackages(submissionId, timeProvider.getCurrentTime, sourceDescription, payload))

  // WriteConfigService
  override def submitConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: SubmissionId,
      config: Configuration): CompletionStage[SubmissionResult] =
    FutureConverters.toJava(ledger.publishConfiguration(maxRecordTime, submissionId, config))
}
