// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream._
import com.daml.error.definitions.IndexErrors.IndexDbException
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.PruneBuffers
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.common.{LedgerIdNotFoundException, MismatchException}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.{
  JdbcLedgerDao,
  LedgerDaoTransactionsReader,
  LedgerReadDao,
}
import com.daml.platform.store.cache.{LedgerEndCache, MutableLedgerEndCache}
import com.daml.platform.store.interning.{
  StringInterning,
  StringInterningView,
  UpdatingStringInterningView,
}
import com.daml.platform.store.{BaseLedger, DbSupport, LfValueTranslationCache}
import com.daml.resources.ProgramResource.StartupException
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

private[platform] object ReadOnlySqlLedger {

  private val logger = ContextualizedLogger.get(this.getClass)

  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  final class Owner(
      dbSupport: DbSupport,
      initialLedgerId: LedgerId,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      acsIdPageSize: Int,
      acsIdFetchingParallelism: Int,
      acsContractFetchingParallelism: Int,
      acsGlobalParallelism: Int,
      acsIdQueueLimit: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      enricher: ValueEnricher,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Long,
      enableInMemoryFanOutForLedgerApi: Boolean,
      participantId: Ref.ParticipantId,
      errorFactories: ErrorFactories,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[ReadOnlySqlLedger] {

    override def acquire()(implicit context: ResourceContext): Resource[ReadOnlySqlLedger] = {
      val ledgerEndCache = MutableLedgerEndCache()
      val stringInterningStorageBackend =
        dbSupport.storageBackendFactory.createStringInterningStorageBackend
      val stringInterningView = new StringInterningView(
        loadPrefixedEntries = (fromExclusive, toInclusive) =>
          implicit loggingContext =>
            dbSupport.dbDispatcher.executeSql(metrics.daml.index.db.loadStringInterningEntries) {
              stringInterningStorageBackend.loadStringInterningEntries(
                fromExclusive,
                toInclusive,
              )
            }
      )
      val ledgerDao = createLedgerDao(
        servicesExecutionContext,
        errorFactories,
        ledgerEndCache,
        stringInterningView,
        dbSupport,
      )
      for {
        ledgerId <- Resource.fromFuture(verifyLedgerId(ledgerDao, initialLedgerId))
        ledger <- ledgerOwner(ledgerDao, ledgerId, ledgerEndCache, stringInterningView).acquire()
      } yield ledger
    }

    private def ledgerOwner(
        ledgerDao: LedgerReadDao,
        ledgerId: LedgerId,
        ledgerEndCache: MutableLedgerEndCache,
        updatingStringInterningView: UpdatingStringInterningView,
    ) =
      new ReadOnlySqlLedgerWithMutableCache.Owner(
        ledgerDao,
        ledgerEndCache,
        updatingStringInterningView,
        enricher,
        ledgerId,
        metrics,
        maxContractStateCacheSize,
        maxContractKeyStateCacheSize,
        maxTransactionsInMemoryFanOutBufferSize,
        enableInMemoryFanOutForLedgerApi,
        servicesExecutionContext = servicesExecutionContext,
      )

    private def verifyLedgerId(
        ledgerDao: LedgerReadDao,
        initialLedgerId: LedgerId,
    )(implicit
        executionContext: ExecutionContext,
        loggingContext: LoggingContext,
    ): Future[LedgerId] = {
      // If the index database is not yet fully initialized,
      // querying for the ledger ID will throw different errors,
      // depending on the database, and how far the initialization is.
      val isRetryable: PartialFunction[Throwable, Boolean] = {
        case _: IndexDbException => true
        case _: LedgerIdNotFoundException => true
        case _: MismatchException.LedgerId => false
        case _ => false
      }
      val retryDelay = 100.millis
      val maxAttempts = 3000 // give up after 5min
      RetryStrategy.constant(attempts = Some(maxAttempts), waitTime = retryDelay)(isRetryable) {
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

    private def createLedgerDao(
        servicesExecutionContext: ExecutionContext,
        errorFactories: ErrorFactories,
        ledgerEndCache: LedgerEndCache,
        stringInterning: StringInterning,
        dbSupport: DbSupport,
    ): LedgerReadDao =
      JdbcLedgerDao.read(
        dbSupport = dbSupport,
        eventsPageSize = eventsPageSize,
        eventsProcessingParallelism = eventsProcessingParallelism,
        acsIdPageSize = acsIdPageSize,
        acsIdFetchingParallelism = acsIdFetchingParallelism,
        acsContractFetchingParallelism = acsContractFetchingParallelism,
        acsGlobalParallelism = acsGlobalParallelism,
        acsIdQueueLimit = acsIdQueueLimit,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        lfValueTranslationCache = lfValueTranslationCache,
        enricher = Some(enricher),
        participantId = participantId,
        ledgerEndCache = ledgerEndCache,
        stringInterning = stringInterning,
        errorFactories = errorFactories,
        materializer = mat,
      )
  }

}

private[index] abstract class ReadOnlySqlLedger(
    ledgerId: LedgerId,
    ledgerDao: LedgerReadDao,
    ledgerDaoTransactionsReader: LedgerDaoTransactionsReader,
    contractStore: ContractStore,
    pruneBuffers: PruneBuffers,
    dispatcher: Dispatcher[Offset],
) extends BaseLedger(
      ledgerId,
      ledgerDao,
      ledgerDaoTransactionsReader,
      contractStore,
      pruneBuffers,
      dispatcher,
    ) {
  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()
}
