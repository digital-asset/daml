// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index
import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.Source
import com.daml.error.definitions.IndexErrors.IndexDbException
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.common.{LedgerIdNotFoundException, MismatchException}
import com.daml.platform.store.appendonlydao.events.{
  BufferedCommandCompletionsReader,
  BufferedTransactionsReader,
  LfValueTranslation,
}
import com.daml.platform.store.appendonlydao.{
  LedgerDaoCommandCompletionsReader,
  LedgerDaoTransactionsReader,
  LedgerReadDao,
}
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{EventsBuffer, LedgerEndCache, MutableCacheBackedContractStore}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker
import com.daml.platform.store.interning.StringInterningView
import com.daml.platform.store.{DbSupport, EventSequentialId, LfValueTranslationCache}
import com.daml.platform.{PruneBuffers, PruneBuffersNoOp}
import com.daml.resources.ProgramResource.StartupException
import com.daml.timer.RetryStrategy

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

private[platform] case class IndexServiceBuilder(
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
    updatesSource: Source[((Offset, Long), TransactionLogUpdate), NotUsed],
    stringInterningView: StringInterningView,
    ledgerEnd: LedgerEnd,
    ledgerEndCache: LedgerEndCache,
    generalDispatcher: Dispatcher[Offset],
    ledgerDao: LedgerReadDao,
)(implicit
    mat: Materializer,
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) {
  private val logger = ContextualizedLogger.get(getClass)

  def owner(): ResourceOwner[IndexService] = {
    for {
      ledgerId <- ResourceOwner.forFuture(() => verifyLedgerId(ledgerDao))
      prefetchingDispatcher <- dispatcherOffsetSeqIdOwner(ledgerEnd)
      contractStore = MutableCacheBackedContractStore(
        ledgerDao.contractsReader,
        ledgerEnd.lastEventSeqId,
        metrics,
        maxContractStateCacheSize,
        maxContractKeyStateCacheSize,
      )(servicesExecutionContext, loggingContext)
      (transactionsReader, completionsReader, pruneBuffers) <- cacheComponentsAndSubscription(
        contractStore,
        ledgerDao,
        prefetchingDispatcher,
        ledgerEndCache,
        updatesSource,
      )
    } yield new IndexServiceImpl(
      ledgerId,
      participantId,
      ledgerDao,
      transactionsReader,
      completionsReader,
      contractStore,
      pruneBuffers,
      generalDispatcher,
    )
  }

  private def cacheComponentsAndSubscription(
      contractStore: MutableCacheBackedContractStore,
      ledgerReadDao: LedgerReadDao,
      cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
      ledgerEndCache: LedgerEndCache,
      updatesSource: Source[((Offset, Long), TransactionLogUpdate), NotUsed],
  ): ResourceOwner[(LedgerDaoTransactionsReader, LedgerDaoCommandCompletionsReader, PruneBuffers)] =
    if (enableInMemoryFanOutForLedgerApi) {
      val completionsBuffer = new EventsBuffer[TransactionLogUpdate](
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        bufferQualifier = "completions",
        ignoreMarker = _.isInstanceOf[LedgerEndMarker],
      )

      val bufferedCompletionsReader = new BufferedCommandCompletionsReader(
        completionsBuffer = completionsBuffer,
        delegate = ledgerReadDao.completions,
        metrics = metrics,
      )

      val transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        bufferQualifier = "transactions",
        ignoreMarker = !_.isInstanceOf[TransactionLogUpdate.TransactionAccepted],
      )

      val bufferedTransactionsReader = BufferedTransactionsReader(
        delegate = ledgerReadDao.transactionsReader,
        transactionsBuffer = transactionsBuffer,
        lfValueTranslation = new LfValueTranslation(
          cache = lfValueTranslationCache,
          metrics = metrics,
          enricherO = Some(enricher),
          loadPackage =
            (packageId, loggingContext) => ledgerReadDao.getLfArchive(packageId)(loggingContext),
        ),
        metrics = metrics,
      )(loggingContext, servicesExecutionContext)

      for {
        _ <- ResourceOwner.forCloseable(() =>
          BuffersUpdater(
            subscribeToTransactionLogUpdates = _ => updatesSource,
            updateTransactionsBuffer = transactionsBuffer.push,
            updateCompletionsBuffer = completionsBuffer.push,
            updateMutableCache = contractStore.push,
            executionContext = servicesExecutionContext,
          )
        )
      } yield (
        bufferedTransactionsReader,
        bufferedCompletionsReader,
        (offset: Offset) => {
          transactionsBuffer.prune(offset)
          completionsBuffer.prune(offset)
        },
      )
    } else
      new MutableCacheBackedContractStore.CacheUpdateSubscription(
        contractStore = contractStore,
        subscribeToContractStateEvents = () => {
          val subscriptionStartExclusive = ledgerEndCache()
          logger.info(s"Subscribing to contract state events after $subscriptionStartExclusive")
          cacheUpdatesDispatcher
            .startingAt(
              subscriptionStartExclusive,
              RangeSource(
                ledgerReadDao.transactionsReader.getContractStateEvents(_, _)
              ),
            )
            .map(_._2)
        },
      ).map(_ => (ledgerReadDao.transactionsReader, ledgerReadDao.completions, PruneBuffersNoOp))

  private def dispatcherOffsetSeqIdOwner(
      ledgerEnd: LedgerEnd
  ): ResourceOwner[Dispatcher[(Offset, Long)]] =
    Dispatcher.owner(
      name = "cache-updates",
      zeroIndex = (Offset.beforeBegin, EventSequentialId.beforeBegin),
      headAtInitialization = (ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId),
    )

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
