// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores

import java.time.Instant

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.engine.Engine
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
import com.daml.platform.store.LfValueTranslationCache
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}

private[sandbox] trait IndexAndWriteService {
  def indexService: IndexService

  def writeService: state.WriteService
}

private[sandbox] object SandboxIndexAndWriteService {
  //TODO: internalise the template store as well
  private val logger = LoggerFactory.getLogger(SandboxIndexAndWriteService.getClass)

  def postgres(
      name: LedgerName,
      providedLedgerId: LedgerIdMode,
      participantId: Ref.ParticipantId,
      jdbcUrl: String,
      databaseConnectionPoolSize: Int,
      databaseConnectionTimeout: FiniteDuration,
      timeProvider: TimeProvider,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      startMode: SqlStartMode,
      queueDepth: Int,
      transactionCommitter: TransactionCommitter,
      templateStore: InMemoryPackageStore,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      engine: Engine,
      enableAppendOnlySchema: Boolean,
      enableCompression: Boolean,
      validatePartyAllocation: Boolean = false,
      allowExistingSchema: Boolean,
  )(implicit
      mat: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[IndexAndWriteService] =
    new SqlLedger.Owner(
      name = name,
      serverRole = ServerRole.Sandbox,
      jdbcUrl = jdbcUrl,
      databaseConnectionPoolSize = databaseConnectionPoolSize,
      databaseConnectionTimeout = databaseConnectionTimeout,
      providedLedgerId = providedLedgerId,
      participantId = domain.ParticipantId(participantId),
      timeProvider = timeProvider,
      packages = templateStore,
      initialLedgerEntries = ledgerEntries,
      queueDepth = queueDepth,
      transactionCommitter = transactionCommitter,
      startMode = startMode,
      eventsPageSize = eventsPageSize,
      eventsProcessingParallelism = eventsProcessingParallelism,
      servicesExecutionContext = servicesExecutionContext,
      metrics = metrics,
      lfValueTranslationCache = lfValueTranslationCache,
      engine = engine,
      validatePartyAllocation = validatePartyAllocation,
      enableAppendOnlySchema = enableAppendOnlySchema,
      enableCompression = enableCompression,
      allowExistingSchema = allowExistingSchema,
    ).flatMap(ledger =>
      owner(
        ledger = MeteredLedger(ledger, metrics),
        participantId = participantId,
        timeProvider = timeProvider,
        enablePruning = enableAppendOnlySchema,
      )
    )

  def inMemory(
      name: LedgerName,
      providedLedgerId: LedgerIdMode,
      participantId: Ref.ParticipantId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      ledgerEntries: ImmArray[LedgerEntryOrBump],
      transactionCommitter: TransactionCommitter,
      templateStore: InMemoryPackageStore,
      metrics: Metrics,
      engine: Engine,
  )(implicit
      mat: Materializer,
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
        engine,
      )
    owner(
      ledger = MeteredLedger(ledger, metrics),
      participantId = participantId,
      timeProvider = timeProvider,
      enablePruning = false,
    )
  }

  private def owner(
      ledger: Ledger,
      participantId: Ref.ParticipantId,
      timeProvider: TimeProvider,
      enablePruning: Boolean,
  )(implicit
      mat: Materializer,
      loggingContext: LoggingContext,
  ): ResourceOwner[IndexAndWriteService] = {
    val indexSvc = new LedgerBackedIndexService(ledger, participantId)
    val writeSvc = new LedgerBackedWriteService(ledger, timeProvider, enablePruning)

    for {
      _ <- new HeartbeatScheduler(
        TimeProvider.UTC,
        10.minutes,
        "deduplication cache maintenance",
        ledger.removeExpiredDeduplicationData,
      )
    } yield new IndexAndWriteService {
      override val indexService: IndexService = indexSvc

      override val writeService: state.WriteService = writeSvc
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
              .mapAsync[Unit](1)(_ => onTimeChange(timeProvider.getCurrentTime))
              .to(Sink.ignore)
              .run()
          })(cancellable =>
            Future {
              val _ = cancellable.cancel()
            }
          ).map(_ => ())
        case _ =>
          Resource.unit
      }
  }
}
