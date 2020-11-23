// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores

import java.time.Instant

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.{ParticipantId, WriteService}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.ImmArray
import com.daml.lf.transaction.TransactionCommitter
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.ServerRole
import com.daml.platform.index.LedgerBackedIndexService
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.LedgerIdGenerator
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.sandbox.stores.ledger.inmemory.InMemoryLedger
import com.daml.platform.sandbox.stores.ledger.sql.{SqlLedger, SqlStartMode}
import com.daml.platform.sandbox.stores.ledger.{Ledger, MeteredLedger}
import com.daml.platform.store.dao.events.LfValueTranslation
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.{DurationInt, FiniteDuration}

private[sandbox] trait IndexAndWriteService {
  def indexService: IndexService

  def writeService: WriteService
}

private[sandbox] object SandboxIndexAndWriteService {
  //TODO: internalise the template store as well
  private val logger = LoggerFactory.getLogger(SandboxIndexAndWriteService.getClass)

  def postgres(
      name: LedgerName,
      providedLedgerId: LedgerIdMode,
      participantId: ParticipantId,
      jdbcUrl: String,
      timeProvider: TimeProvider,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      startMode: SqlStartMode,
      queueDepth: Int,
      transactionCommitter: TransactionCommitter,
      templateStore: InMemoryPackageStore,
      eventsPageSize: Int,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslation.Cache,
      validatePartyAllocation: Boolean = false,
  )(
      implicit mat: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[IndexAndWriteService] =
    new SqlLedger.Owner(
      name = name,
      serverRole = ServerRole.Sandbox,
      jdbcUrl = jdbcUrl,
      providedLedgerId = providedLedgerId,
      participantId = domain.ParticipantId(participantId),
      timeProvider = timeProvider,
      packages = templateStore,
      initialLedgerEntries = ledgerEntries,
      queueDepth = queueDepth,
      transactionCommitter = transactionCommitter,
      startMode = startMode,
      eventsPageSize = eventsPageSize,
      metrics = metrics,
      lfValueTranslationCache,
      validatePartyAllocation,
    ).flatMap(ledger => owner(MeteredLedger(ledger, metrics), participantId, timeProvider))

  def inMemory(
      name: LedgerName,
      providedLedgerId: LedgerIdMode,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      transactionCommitter: TransactionCommitter,
      templateStore: InMemoryPackageStore,
      metrics: Metrics,
  )(
      implicit mat: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[IndexAndWriteService] = {
    val ledger =
      new InMemoryLedger(
        providedLedgerId.or(LedgerIdGenerator.generateRandomId(name)),
        timeProvider,
        acs,
        transactionCommitter,
        templateStore,
        ledgerEntries,
      )
    owner(MeteredLedger(ledger, metrics), participantId, timeProvider)
  }

  private def owner(
      ledger: Ledger,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
  )(
      implicit mat: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[IndexAndWriteService] = {
    val indexSvc = new LedgerBackedIndexService(ledger, participantId)
    val writeSvc = new LedgerBackedWriteService(ledger, timeProvider)

    for {
      _ <- new HeartbeatScheduler(
        TimeProvider.UTC,
        10.minutes,
        "deduplication cache maintenance",
        ledger.removeExpiredDeduplicationData,
      )
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

    override def acquire()(implicit context: ResourceContext): Resource[Unit] =
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
