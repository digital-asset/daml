// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index
import com.daml.error.definitions.IndexErrors.IndexDbException
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.common.{LedgerIdNotFoundException, MismatchException}
import com.daml.platform.store.LfValueTranslationCache
import com.daml.platform.store.appendonlydao.LedgerReadDao
import com.daml.platform.store.appendonlydao.events.{
  BufferedCommandCompletionsReader,
  BufferedTransactionsReader,
  LfValueTranslation,
}
import com.daml.platform.store.cache.MutableCacheBackedContractStore
import com.daml.resources.ProgramResource.StartupException
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

private[platform] case class IndexServiceBuilder(
    initialLedgerId: LedgerId,
    servicesExecutionContext: ExecutionContext,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    enricher: ValueEnricher,
    participantId: Ref.ParticipantId,
    ledgerDao: LedgerReadDao,
    participantInMemoryState: ParticipantInMemoryState,
)(implicit
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) {
  private val logger = ContextualizedLogger.get(getClass)

  def owner(): ResourceOwner[IndexService] = {
    for {
      ledgerId <- ResourceOwner.forFuture(() => verifyLedgerId(ledgerDao))

      ledgerEnd <- ResourceOwner.forFuture(() => ledgerDao.lookupLedgerEnd())
      _ = participantInMemoryState.updateLedgerApiLedgerEnd(
        ledgerEnd.lastOffset,
        ledgerEnd.lastEventSeqId,
      )

      contractStore = new MutableCacheBackedContractStore(
        metrics,
        ledgerDao.contractsReader,
        participantInMemoryState.mutableContractStateCaches,
      )(servicesExecutionContext, loggingContext)

      _ = participantInMemoryState.mutableContractStateCaches.init(ledgerEnd.lastOffset)

      completionsReader = new BufferedCommandCompletionsReader(
        completionsBuffer = participantInMemoryState.completionsBuffer,
        delegate = ledgerDao.completions,
        metrics = metrics,
      )

      transactionsReader = BufferedTransactionsReader(
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
      )(servicesExecutionContext)
    } yield new IndexServiceImpl(
      ledgerId,
      participantId,
      ledgerDao,
      transactionsReader,
      completionsReader,
      contractStore,
      (offset: Offset) => {
        participantInMemoryState.transactionsBuffer.prune(offset)
        participantInMemoryState.completionsBuffer.prune(offset)
      },
      participantInMemoryState.dispatcher,
    )
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
}
