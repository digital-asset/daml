// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.error.definitions.IndexErrors.IndexDbException
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.ParticipantInMemoryState
import com.daml.platform.common.{LedgerIdNotFoundException, MismatchException}
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.store.cache._
import com.daml.platform.store.dao.events.{BufferedTransactionsReader, LfValueTranslation}
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerReadDao}
import com.daml.platform.store.interning.StringInterning
import com.daml.platform.store.{DbSupport, LfValueTranslationCache}
import com.daml.resources.ProgramResource.StartupException
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NoStackTrace

private[platform] case class IndexServiceBuilder(
    config: IndexServiceConfig,
    dbSupport: DbSupport,
    initialLedgerId: LedgerId,
    servicesExecutionContext: ExecutionContext,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    enricher: ValueEnricher,
    participantId: Ref.ParticipantId,
    participantInMemoryState: ParticipantInMemoryState,
)(implicit
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) {
  private val logger = ContextualizedLogger.get(getClass)

  def owner(): ResourceOwner[IndexService] = {
    val ledgerDao = createLedgerReadDao(
      ledgerEndCache = participantInMemoryState.ledgerEndCache,
      stringInterning = participantInMemoryState.stringInterningView,
    )

    for {
      _ <- ResourceOwner.forFuture(() => waitForInMemoryStateInitialization)
      ledgerId <- ResourceOwner.forFuture(() => verifyLedgerId(ledgerDao))
      contractStore = new MutableCacheBackedContractStore(
        metrics,
        ledgerDao.contractsReader,
        contractStateCaches = participantInMemoryState.contractStateCaches,
      )(servicesExecutionContext, loggingContext)
      bufferedTransactionsReader = BufferedTransactionsReader(
        delegate = ledgerDao.transactionsReader,
        transactionsBuffer = participantInMemoryState.transactionsBuffer,
        lfValueTranslation = new LfValueTranslation(
          cache = lfValueTranslationCache,
          metrics = metrics,
          enricherO = Some(enricher),
          loadPackage =
            (packageId, loggingContext) => ledgerDao.getLfArchive(packageId)(loggingContext),
        ),
        metrics = metrics,
        eventProcessingParallelism = config.eventsProcessingParallelism,
      )(servicesExecutionContext)
    } yield new IndexServiceImpl(
      ledgerId = ledgerId,
      participantId = participantId,
      ledgerDao = ledgerDao,
      transactionsReader = bufferedTransactionsReader,
      contractStore = contractStore,
      pruneBuffers = participantInMemoryState.transactionsBuffer.prune,
      dispatcher = participantInMemoryState.dispatcher _,
      metrics = metrics,
    )
  }

  private def waitForInMemoryStateInitialization(implicit
      executionContext: ExecutionContext
  ): Future[Unit] = {
    val retryDelay = 100.millis
    val maxAttempts = 3000

    RetryStrategy.constant(
      attempts = Some(maxAttempts),
      waitTime = retryDelay,
    ) { case ParticipantInMemoryStateNotInitialized => true } { (attempt, _) =>
      if (!participantInMemoryState.initialized) {
        logger.info(
          s"Participant in-memory state not initialized on attempt $attempt/$maxAttempts. Retrying again in $retryDelay."
        )
        Future.failed(ParticipantInMemoryStateNotInitialized)
      } else {
        Future.unit
      }
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
                new MismatchException.LedgerId(foundLedgerId, initialLedgerId) with StartupException
              )
            case None =>
              logger.info(
                s"Ledger ID not found in the index database on attempt $attempt/$maxAttempts. Retrying again in $retryDelay."
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

  private object ParticipantInMemoryStateNotInitialized extends NoStackTrace
}
