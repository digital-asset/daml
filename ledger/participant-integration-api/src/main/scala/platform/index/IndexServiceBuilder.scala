// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index
import akka.stream._
import com.daml.error.definitions.IndexErrors.IndexDbException
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.data.Ref
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.{PruneBuffers, PruneBuffersNoOp}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.common.{LedgerIdNotFoundException, MismatchException}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.events.{BufferedTransactionsReader, LfValueTranslation}
import com.daml.platform.store.appendonlydao.{
  JdbcLedgerDao,
  LedgerDaoTransactionsReader,
  LedgerReadDao,
}
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{
  EventsBuffer,
  LedgerEndCache,
  MutableCacheBackedContractStore,
  MutableLedgerEndCache,
}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.{
  StringInterning,
  StringInterningView,
  UpdatingStringInterningView,
}
import com.daml.platform.store.{DbSupport, EventSequentialId, LfValueTranslationCache}
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
    errorFactories: ErrorFactories,
)(implicit
    mat: Materializer,
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) {
  private val logger = ContextualizedLogger.get(getClass)

  def owner(): ResourceOwner[IndexService] = {
    val ledgerEndCache = MutableLedgerEndCache()
    val stringInterningView = createStringInterningView()
    val ledgerDao = createLedgerReadDao(ledgerEndCache, stringInterningView)
    for {
      ledgerId <- ResourceOwner.forFuture(() => verifyLedgerId(ledgerDao))
      ledgerEnd <- ResourceOwner.forFuture(() => ledgerDao.lookupLedgerEnd())
      _ = ledgerEndCache.set((ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId))
      _ <- ResourceOwner.forFuture(() =>
        stringInterningView.update(ledgerEnd.lastStringInterningId)
      )
      prefetchingDispatcher <- dispatcherOffsetSeqIdOwner(ledgerEnd)
      generalDispatcher <- dispatcherOwner(ledgerEnd.lastOffset)
      instrumentedSignalNewLedgerHead = buildInstrumentedSignalNewLedgerHead(
        ledgerEndCache,
        generalDispatcher,
      )
      contractStore = mutableCacheBackedContractStore(
        ledgerDao,
        ledgerEnd,
        instrumentedSignalNewLedgerHead,
      )
      (transactionsReader, pruneBuffers) <- cacheComponentsAndSubscription(
        contractStore,
        ledgerDao,
        prefetchingDispatcher,
        ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId,
      )
      _ <- ledgerEndDispatcherSubscription(
        ledgerDao,
        stringInterningView,
        instrumentedSignalNewLedgerHead,
        prefetchingDispatcher,
      )
    } yield new IndexServiceImpl(
      ledgerId,
      participantId,
      ledgerDao,
      transactionsReader,
      contractStore,
      pruneBuffers,
      generalDispatcher,
      errorFactories,
    )
  }

  private def ledgerEndDispatcherSubscription(
      ledgerDao: LedgerReadDao,
      updatingStringInterningView: UpdatingStringInterningView,
      instrumentedSignalNewLedgerHead: InstrumentedSignalNewLedgerHead,
      prefetchingDispatcher: Dispatcher[(Offset, Long)],
  ) =
    ResourceOwner
      .forReleasable(() =>
        LedgerEndPoller(
          ledgerDao,
          newLedgerHead =>
            for {
              _ <- updatingStringInterningView.update(newLedgerHead.lastStringInterningId)
            } yield {
              instrumentedSignalNewLedgerHead.startTimer(newLedgerHead.lastOffset)
              prefetchingDispatcher.signalNewHead(
                newLedgerHead.lastOffset -> newLedgerHead.lastEventSeqId
              )
            },
        )
      )(_.release())

  private def buildInstrumentedSignalNewLedgerHead(
      ledgerEndCache: MutableLedgerEndCache,
      generalDispatcher: Dispatcher[Offset],
  ) =
    new InstrumentedSignalNewLedgerHead((offset, eventSeqId) => {
      ledgerEndCache.set((offset, eventSeqId))
      // the order here is very important: first we need to make data available for point-wise lookups
      // and SQL queries, and only then we can make it available on the streams.
      // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
      generalDispatcher.signalNewHead(offset)
    })(metrics.daml.execution.cache.dispatcherLag)

  private def mutableCacheBackedContractStore(
      ledgerDao: LedgerReadDao,
      ledgerEnd: LedgerEnd,
      dispatcherLagMeter: InstrumentedSignalNewLedgerHead,
  ) =
    MutableCacheBackedContractStore(
      ledgerDao.contractsReader,
      dispatcherLagMeter,
      ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId,
      metrics,
      maxContractStateCacheSize,
      maxContractKeyStateCacheSize,
    )(servicesExecutionContext, loggingContext)

  private def createStringInterningView() = {
    val stringInterningStorageBackend =
      dbSupport.storageBackendFactory.createStringInterningStorageBackend

    new StringInterningView(
      loadPrefixedEntries = (fromExclusive, toInclusive) =>
        implicit loggingContext =>
          dbSupport.dbDispatcher.executeSql(metrics.daml.index.db.loadStringInterningEntries) {
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          }
    )
  }

  private def cacheComponentsAndSubscription(
      contractStore: MutableCacheBackedContractStore,
      ledgerReadDao: LedgerReadDao,
      cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
      startExclusive: (Offset, Long),
  ): ResourceOwner[(LedgerDaoTransactionsReader, PruneBuffers)] =
    if (enableInMemoryFanOutForLedgerApi) {
      val transactionsBuffer = new EventsBuffer[Offset, TransactionLogUpdate](
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        bufferQualifier = "transactions",
        isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
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
        _ <- ResourceOwner.forReleasable(() =>
          BuffersUpdater(
            subscribeToTransactionLogUpdates = maybeOffsetSeqId => {
              val subscriptionStartExclusive @ (offsetStart, eventSeqIdStart) =
                maybeOffsetSeqId.getOrElse(startExclusive)
              logger.info(
                s"Subscribing for transaction log updates after ${offsetStart.toHexString} -> $eventSeqIdStart"
              )
              cacheUpdatesDispatcher
                .startingAt(
                  subscriptionStartExclusive,
                  RangeSource(
                    ledgerReadDao.transactionsReader.getTransactionLogUpdates(_, _)
                  ),
                )
            },
            updateTransactionsBuffer = transactionsBuffer.push,
            updateMutableCache = contractStore.push,
          )
        )(_.release())
      } yield (bufferedTransactionsReader, transactionsBuffer.prune _)
    } else
      ResourceOwner
        .forReleasable { () =>
          MutableContractStateCacheUpdater(
            contractStore = contractStore,
            subscribeToContractStateEvents = (cacheIndex: (Offset, Long)) =>
              cacheUpdatesDispatcher
                .startingAt(
                  cacheIndex,
                  RangeSource(
                    ledgerReadDao.transactionsReader.getContractStateEvents(_, _)
                  ),
                )
                .map(_._2),
          )
        }(_.release())
        .map(_ => (ledgerReadDao.transactionsReader, PruneBuffersNoOp))

  private def dispatcherOffsetSeqIdOwner(
      ledgerEnd: LedgerEnd
  ): ResourceOwner[Dispatcher[(Offset, Long)]] =
    Dispatcher.owner(
      name = "cache-updates",
      zeroIndex = (Offset.beforeBegin, EventSequentialId.beforeBegin),
      headAtInitialization = (ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId),
    )

  private def dispatcherOwner(ledgerEnd: Offset): ResourceOwner[Dispatcher[Offset]] =
    Dispatcher.owner(
      name = "sql-ledger",
      zeroIndex = Offset.beforeBegin,
      headAtInitialization = ledgerEnd,
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

  private def createLedgerReadDao(
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
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
