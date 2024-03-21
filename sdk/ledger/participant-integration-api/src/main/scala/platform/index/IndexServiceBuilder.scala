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
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.store.dao.events.{BufferedTransactionsReader, LfValueTranslation}
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerDaoTransactionsReader, LedgerReadDao}
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{
  ContractStateCaches,
  EventsBuffer,
  LedgerEndCache,
  MutableCacheBackedContractStore,
  MutableLedgerEndCache,
}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.{
  LoadStringInterningEntries,
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
    config: IndexServiceConfig,
    dbSupport: DbSupport,
    initialLedgerId: LedgerId,
    servicesExecutionContext: ExecutionContext,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    enricher: ValueEnricher,
    participantId: Ref.ParticipantId,
    sharedStringInterningViewO: Option[StringInterningView],
)(implicit
    mat: Materializer,
    loggingContext: LoggingContext,
    executionContext: ExecutionContext,
) {
  private val logger = ContextualizedLogger.get(getClass)

  def owner(): ResourceOwner[IndexService] = {
    val ledgerEndCache = MutableLedgerEndCache()
    val isSharedStringInterningView = sharedStringInterningViewO.nonEmpty
    val stringInterningView = sharedStringInterningViewO.getOrElse(new StringInterningView())
    val ledgerDao = createLedgerReadDao(ledgerEndCache, stringInterningView)
    for {
      ledgerId <- ResourceOwner.forFuture(() => verifyLedgerId(ledgerDao))
      ledgerEnd <- ResourceOwner.forFuture(() => ledgerDao.lookupLedgerEnd())
      _ = ledgerEndCache.set((ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId))
      _ <-
        if (isSharedStringInterningView) {
          // The participant-wide (shared) StringInterningView is updated by the Indexer
          ResourceOwner.unit
        } else {
          ResourceOwner.forFuture(() =>
            stringInterningView.update(ledgerEnd.lastStringInterningId)(loadStringInterningEntries)
          )
        }
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
        config,
        contractStore,
        ledgerDao,
        prefetchingDispatcher,
        ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId,
        ledgerEndCache,
      )
      _ <- cachesUpdaterSubscription(
        isSharedStringInterningView,
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
      metrics,
    )
  }

  private def cachesUpdaterSubscription(
      sharedStringInterningView: Boolean,
      ledgerDao: LedgerReadDao,
      updatingStringInterningView: UpdatingStringInterningView,
      instrumentedSignalNewLedgerHead: InstrumentedSignalNewLedgerHead,
      prefetchingDispatcher: Dispatcher[(Offset, Long)],
  ): ResourceOwner[Unit] =
    ResourceOwner
      .forReleasable(() =>
        new LedgerEndPoller(
          ledgerDao,
          newLedgerHead =>
            for {
              _ <-
                if (sharedStringInterningView) {
                  // The participant-wide (shared) StringInterningView is updated by the Indexer
                  Future.unit
                } else {
                  updatingStringInterningView
                    .update(newLedgerHead.lastStringInterningId)(loadStringInterningEntries)
                }
            } yield {
              instrumentedSignalNewLedgerHead.startTimer(newLedgerHead.lastOffset)
              prefetchingDispatcher.signalNewHead(
                newLedgerHead.lastOffset -> newLedgerHead.lastEventSeqId
              )
            },
        )
      )(_.release())
      .map(_ => ())

  private def loadStringInterningEntries: LoadStringInterningEntries = {
    (fromExclusive: Int, toInclusive: Int) => implicit loggingContext: LoggingContext =>
      dbSupport.dbDispatcher
        .executeSql(metrics.daml.index.db.loadStringInterningEntries) {
          dbSupport.storageBackendFactory.createStringInterningStorageBackend
            .loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
        }
  }

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
    new MutableCacheBackedContractStore(
      metrics,
      ledgerDao.contractsReader,
      dispatcherLagMeter,
      contractStateCaches = ContractStateCaches.build(
        ledgerEnd.lastOffset,
        config.maxContractStateCacheSize,
        config.maxContractKeyStateCacheSize,
        metrics,
      )(servicesExecutionContext, loggingContext),
    )(servicesExecutionContext, loggingContext)

  private def cacheComponentsAndSubscription(
      config: IndexServiceConfig,
      contractStore: MutableCacheBackedContractStore,
      ledgerReadDao: LedgerReadDao,
      cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
      startExclusive: (Offset, Long),
      ledgerEndCache: LedgerEndCache,
  ): ResourceOwner[(LedgerDaoTransactionsReader, PruneBuffers)] =
    if (config.enableInMemoryFanOutForLedgerApi) {
      val transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
        maxBufferSize = config.maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        bufferQualifier = "transactions",
        isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
        maxBufferedChunkSize = config.bufferedStreamsPageSize,
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
        eventProcessingParallelism = config.eventsProcessingParallelism,
      )(servicesExecutionContext)

      for {
        _ <- BuffersUpdater.owner(
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
          metrics = metrics,
        )
      } yield (bufferedTransactionsReader, transactionsBuffer.prune _)
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
      ).map(_ => (ledgerReadDao.transactionsReader, PruneBuffersNoOp))

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
      shutdownTimeout = config.apiStreamShutdownTimeout,
      onShutdownTimeout = () =>
        logger.warn(
          s"Shutdown of API streams did not finish in ${config.apiStreamShutdownTimeout.toSeconds} seconds. System shutdown continues."
        ),
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
}
