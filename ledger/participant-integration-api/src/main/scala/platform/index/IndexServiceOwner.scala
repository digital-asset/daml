// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.error.definitions.IndexErrors.IndexDbException
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.InMemoryState
import com.daml.platform.apiserver.TimedIndexService
import com.daml.platform.common.{LedgerIdNotFoundException, MismatchException}
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.store.cache._
import com.daml.platform.store.dao.events.{BufferedTransactionsReader, LfValueTranslation}
import com.daml.platform.store.dao.{BufferedCommandCompletionsReader, JdbcLedgerDao, LedgerReadDao}
import com.daml.platform.store.interning.StringInterning
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.resources.ProgramResource.StartupException
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace

final class IndexServiceOwner(
    config: IndexServiceConfig,
    dbSupport: DbSupport,
    initialLedgerId: LedgerId,
    servicesExecutionContext: ExecutionContext,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    enricher: ValueEnricher,
    participantId: Ref.ParticipantId,
    inMemoryState: InMemoryState,
)(implicit
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) extends ResourceOwner[IndexService] {
  private val initializationRetryDelay = 100.millis
  private val initializationMaxAttempts = 3000 // give up after 5min

  private val logger = ContextualizedLogger.get(getClass)

  def acquire()(implicit context: ResourceContext): Resource[IndexService] = {
    val ledgerDao = createLedgerReadDao(
      ledgerEndCache = inMemoryState.ledgerEndCache,
      stringInterning = inMemoryState.stringInterningView,
    )

    for {
      ledgerId <- Resource.fromFuture(verifyLedgerId(ledgerDao))
      _ <- Resource.fromFuture(waitForinMemoryStateInitialization())

      contractStore = new MutableCacheBackedContractStore(
        metrics,
        ledgerDao.contractsReader,
        contractStateCaches = inMemoryState.contractStateCaches,
      )(servicesExecutionContext, loggingContext)

      lfValueTranslation = new LfValueTranslation(
        cache = lfValueTranslationCache,
        metrics = metrics,
        enricherO = Some(enricher),
        loadPackage = (packageId, loggingContext) =>
          ledgerDao.getLfArchive(packageId)(loggingContext),
      )

      bufferedTransactionsReader = BufferedTransactionsReader(
        delegate = ledgerDao.transactionsReader,
        transactionsBuffer = inMemoryState.transactionsBuffer,
        lfValueTranslation = lfValueTranslation,
        metrics = metrics,
        eventProcessingParallelism = config.eventsProcessingParallelism,
      )(servicesExecutionContext)

      bufferedCommandCompletionsReader = BufferedCommandCompletionsReader(
        inMemoryFanoutBuffer = inMemoryState.transactionsBuffer,
        delegate = ledgerDao.completions,
        metrics = metrics,
      )(servicesExecutionContext)

      indexService = new IndexServiceImpl(
        ledgerId = ledgerId,
        participantId = participantId,
        ledgerDao = ledgerDao,
        transactionsReader = bufferedTransactionsReader,
        commandCompletionsReader = bufferedCommandCompletionsReader,
        contractStore = contractStore,
        pruneBuffers = inMemoryState.transactionsBuffer.prune,
        dispatcher = () => inMemoryState.dispatcherState.getDispatcher,
        metrics = metrics,
      )
    } yield new TimedIndexService(indexService, metrics)
  }

  private def waitForinMemoryStateInitialization()(implicit
      executionContext: ExecutionContext
  ): Future[Unit] =
    RetryStrategy.constant(
      attempts = Some(initializationMaxAttempts),
      waitTime = initializationRetryDelay,
    ) { case InMemoryStateNotInitialized => true } { (attempt, _) =>
      if (!inMemoryState.initialized) {
        logger.info(
          s"Participant in-memory state not initialized on attempt $attempt/$initializationMaxAttempts. Retrying again in $initializationRetryDelay."
        )
        Future.failed(InMemoryStateNotInitialized)
      } else {
        Future.unit
      }
    }

  private def verifyLedgerId(
      ledgerDao: LedgerReadDao
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

    RetryStrategy.constant(
      attempts = Some(initializationMaxAttempts),
      waitTime = initializationRetryDelay,
    )(isRetryable) { (attempt, _) =>
      ledgerDao
        .lookupLedgerId()
        .flatMap {
          case Some(`initialLedgerId`) =>
            logger.info(s"Found existing ledger with ID: $initialLedgerId")
            Future.successful(initialLedgerId)
          case Some(foundLedgerId) =>
            Future.failed(
              new MismatchException.LedgerId(foundLedgerId, initialLedgerId) with StartupException
            )
          case None =>
            logger.info(
              s"Ledger ID not found in the index database on attempt $attempt/$initializationMaxAttempts. Retrying again in $initializationRetryDelay."
            )
            Future.failed(new LedgerIdNotFoundException(attempt))
        }
    }
  }

  private def createLedgerReadDao(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
  ): LedgerReadDao =
    JdbcLedgerDao.read(
      dbSupport = dbSupport,
      eventsPageSize = config.eventsPageSize,
      eventsProcessingParallelism = config.eventsProcessingParallelism,
      acsIdPageSize = config.acsIdPageSize,
      acsIdPageBufferSize = config.acsIdPageBufferSize,
      acsIdPageWorkingMemoryBytes = config.acsIdPageWorkingMemoryBytes,
      acsIdFetchingParallelism = config.acsIdFetchingParallelism,
      acsContractFetchingParallelism = config.acsContractFetchingParallelism,
      acsGlobalParallelism = config.acsGlobalParallelism,
      servicesExecutionContext = servicesExecutionContext,
      metrics = metrics,
      lfValueTranslationCache = lfValueTranslationCache,
      enricher = Some(enricher),
      participantId = participantId,
      ledgerEndCache = ledgerEndCache,
      stringInterning = stringInterning,
    )

  private object InMemoryStateNotInitialized extends NoStackTrace
}
