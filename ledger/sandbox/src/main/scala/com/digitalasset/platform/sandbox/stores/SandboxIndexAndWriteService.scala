// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.time.Instant
import java.util.concurrent.CompletionStage

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.v1.{
  ApplicationId => _,
  LedgerId => _,
  TransactionId => _,
  _
}
import com.daml.ledger.participant.state.{v1 => ParticipantState}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.index.LedgerBackedIndexService
import com.digitalasset.platform.packages.InMemoryPackageStore
import com.digitalasset.platform.sandbox.LedgerIdGenerator
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger._
import com.digitalasset.platform.sandbox.stores.ledger.inmemory.InMemoryLedger
import com.digitalasset.platform.sandbox.stores.ledger.sql.{SqlLedger, SqlStartMode}
import com.digitalasset.resources.{Resource, ResourceOwner}
import org.slf4j.LoggerFactory

import scala.compat.java8.FutureConverters
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

trait IndexAndWriteService {
  def indexService: IndexService

  def writeService: WriteService

  def publishHeartbeat(instant: Instant): Future[Unit]
}

object SandboxIndexAndWriteService {
  //TODO: internalise the template store as well
  private val logger = LoggerFactory.getLogger(SandboxIndexAndWriteService.getClass)

  private val Name: String = "sandbox"

  def postgres(
      ledgerId: LedgerIdMode,
      participantId: ParticipantId,
      jdbcUrl: String,
      timeModel: ParticipantState.TimeModel,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      startMode: SqlStartMode,
      queueDepth: Int,
      templateStore: InMemoryPackageStore,
      metrics: MetricRegistry,
  )(implicit mat: Materializer, logCtx: LoggingContext): ResourceOwner[IndexAndWriteService] =
    SqlLedger
      .owner(
        Name,
        jdbcUrl,
        ledgerId,
        participantId,
        timeProvider,
        acs,
        templateStore,
        ledgerEntries,
        queueDepth,
        startMode,
        metrics,
      )
      .flatMap(ledger =>
        owner(MeteredLedger(ledger, metrics), participantId, timeModel, timeProvider))

  def inMemory(
      ledgerId: LedgerIdMode,
      participantId: ParticipantId,
      timeModel: ParticipantState.TimeModel,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      templateStore: InMemoryPackageStore,
      metrics: MetricRegistry,
  )(implicit mat: Materializer): ResourceOwner[IndexAndWriteService] = {
    val ledger =
      new InMemoryLedger(
        ledgerId.or(LedgerIdGenerator.generateRandomId()),
        participantId,
        timeProvider,
        acs,
        templateStore,
        ledgerEntries)
    owner(MeteredLedger(ledger, metrics), participantId, timeModel, timeProvider)
  }

  private def owner(
      ledger: Ledger,
      participantId: ParticipantId,
      timeModel: ParticipantState.TimeModel,
      timeProvider: TimeProvider,
  )(implicit mat: Materializer): ResourceOwner[IndexAndWriteService] = {
    val indexSvc = new LedgerBackedIndexService(ledger, participantId) {
      override def getLedgerConfiguration(): Source[LedgerConfiguration, NotUsed] =
        Source
          .single(LedgerConfiguration(timeModel.minTtl, timeModel.maxTtl))
          .concat(Source.future(Promise[LedgerConfiguration]().future)) // we should keep the stream open!
    }
    val writeSvc = new LedgerBackedWriteService(ledger, timeProvider)

    for {
      _ <- new HeartbeatScheduler(timeProvider, 1.seconds, "heartbeats", ledger.publishHeartbeat)
      _ <- new HeartbeatScheduler(
        TimeProvider.UTC,
        10.minutes,
        "deduplication cache maintenance",
        ledger.removeExpiredDeduplicationData)
    } yield
      new IndexAndWriteService {
        override val indexService: IndexService = indexSvc

        override val writeService: WriteService = writeSvc

        override def publishHeartbeat(instant: Instant): Future[Unit] =
          ledger.publishHeartbeat(instant)
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
