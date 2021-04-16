// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import java.sql.SQLException
import java.time.Instant

import akka.Done
import akka.actor.Cancellable
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.common.{LedgerIdNotFoundException, MismatchException}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.dao
import com.daml.platform.store.dao.LedgerReadDao
import com.daml.platform.store.{BaseLedger, LfValueTranslationCache}
import com.daml.resources.ProgramResource.StartupException
import com.daml.timer.RetryStrategy

import com.daml.platform.store.appendonlydao

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

private[platform] object ReadOnlySqlLedger {
  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  final class Owner(
      serverRole: ServerRole,
      jdbcUrl: String,
      initialLedgerId: LedgerId,
      databaseConnectionPoolSize: Int,
      eventsPageSize: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: ValueEnricher,
      // TODO append-only: remove after removing support for the current (mutating) schema
      enableAppendOnlySchema: Boolean,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      enableMutableContractStateCache: Boolean,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[ReadOnlySqlLedger] {

    private val logger = ContextualizedLogger.get(this.getClass)

    override def acquire()(implicit context: ResourceContext): Resource[ReadOnlySqlLedger] =
      for {
        ledgerDao <- ledgerDaoOwner(servicesExecutionContext).acquire()
        ledgerId <- Resource.fromFuture(verifyLedgerId(ledgerDao, initialLedgerId))
        ledger <- ledgerOwner(ledgerDao, ledgerId).acquire()
      } yield ledger

    private def ledgerOwner(ledgerDao: LedgerReadDao, ledgerId: LedgerId) =
      if (enableMutableContractStateCache)
        new ReadOnlySqlLedgerWithMutableCache.Owner(
          ledgerDao,
          ledgerId,
          metrics,
          maxContractStateCacheSize,
          maxContractKeyStateCacheSize,
        )
      else
        new ReadOnlySqlLedgerWithTranslationCache.Owner(
          ledgerDao,
          ledgerId,
          lfValueTranslationCache,
        )

    private def verifyLedgerId(
        ledgerDao: LedgerReadDao,
        initialLedgerId: LedgerId,
    )(implicit
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[LedgerId] = {
      val predicate: PartialFunction[Throwable, Boolean] = {
        // If the index database is not yet fully initialized,
        // querying for the ledger ID will throw different errors,
        // depending on the database, and how far the initialization is.
        case _: SQLException => true
        case _: LedgerIdNotFoundException => true
        case _: MismatchException.LedgerId => false
        case _ => false
      }
      val retryDelay = 5.seconds
      val maxAttempts = 100
      RetryStrategy.constant(attempts = Some(maxAttempts), waitTime = retryDelay)(predicate) {
        (attempt, _) =>
          ledgerDao
            .lookupLedgerId()
            .flatMap {
              case Some(`initialLedgerId`) =>
                logger.info(s"Found existing ledger with ID: $initialLedgerId")
                Future.successful(initialLedgerId)
              case Some(foundLedgerId) =>
                Future.failed(
                  new MismatchException.LedgerId(foundLedgerId, initialLedgerId)
                    with StartupException
                )
              case None =>
                logger.info(
                  s"Ledger ID not found in the index database on attempt $attempt/$maxAttempts. Retrying again in $retryDelay."
                )
                Future.failed(new LedgerIdNotFoundException(attempt))
            }
      }
    }

    private def ledgerDaoOwner(
        servicesExecutionContext: ExecutionContext
    ): ResourceOwner[LedgerReadDao] =
      if (enableAppendOnlySchema)
        appendonlydao.JdbcLedgerDao.readOwner(
          serverRole,
          jdbcUrl,
          databaseConnectionPoolSize,
          eventsPageSize,
          servicesExecutionContext,
          metrics,
          lfValueTranslationCache,
          Some(enricher),
        )
      else
        dao.JdbcLedgerDao.readOwner(
          serverRole,
          jdbcUrl,
          databaseConnectionPoolSize,
          eventsPageSize,
          servicesExecutionContext,
          metrics,
          lfValueTranslationCache,
          Some(enricher),
        )
  }
}

private[index] abstract class ReadOnlySqlLedger(
    ledgerId: LedgerId,
    ledgerDao: LedgerReadDao,
    contractStore: ContractStore,
    dispatcher: Dispatcher[Offset],
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends BaseLedger(ledgerId, ledgerDao, contractStore, dispatcher) {

  // Periodically remove all expired deduplication cache entries.
  // The current approach is not ideal for multiple ReadOnlySqlLedgers sharing
  // the same database (as is the case for a horizontally scaled ledger API server).
  // In that case, an external process periodically clearing expired entries would be better.
  //
  // Deduplication entries are added by the submission service, which might use
  // a different clock than the current clock (e.g., horizontally scaled ledger API server).
  // This is not an issue, because applications are not expected to submit towards the end
  // of the deduplication time window.
  private val (deduplicationCleanupKillSwitch, deduplicationCleanupDone) =
    Source
      .tick[Unit](0.millis, 10.minutes, ())
      .mapAsync[Unit](1)(_ => ledgerDao.removeExpiredDeduplicationData(Instant.now()))
      .viaMat(KillSwitches.single)(Keep.right[Cancellable, UniqueKillSwitch])
      .toMat(Sink.ignore)(Keep.both[UniqueKillSwitch, Future[Done]])
      .run()

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def close(): Unit = {
    deduplicationCleanupKillSwitch.shutdown()

    Await.result(deduplicationCleanupDone, 10.seconds)

    super.close()
  }
}
