// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import java.util.concurrent.TimeUnit

import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.codahale.metrics.Timer
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.engine.ValueEnricher
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.{PruneBuffers, PruneBuffersNoOp}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.index.ReadOnlySqlLedgerWithMutableCache.DispatcherLagMeter
import com.daml.platform.store.appendonlydao.{LedgerDaoTransactionsReader, LedgerReadDao}
import com.daml.platform.store.{EventSequentialId, LfValueTranslationCache}
import com.daml.platform.store.appendonlydao.events.{BufferedTransactionsReader, LfValueTranslation}
import com.daml.platform.store.cache.MutableCacheBackedContractStore.SignalNewLedgerHead
import com.daml.platform.store.cache.{
  EventsBuffer,
  MutableCacheBackedContractStore,
  MutableLedgerEndCache,
}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.UpdatingStringInterningView
import com.daml.scalautil.Statement.discard

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

private[index] object ReadOnlySqlLedgerWithMutableCache {
  final class Owner(
      ledgerDao: LedgerReadDao,
      ledgerEndCache: MutableLedgerEndCache,
      updatingStringInterningView: UpdatingStringInterningView,
      enricher: ValueEnricher,
      ledgerId: LedgerId,
      metrics: Metrics,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Long,
      enableInMemoryFanOutForLedgerApi: Boolean,
      servicesExecutionContext: ExecutionContext,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[ReadOnlySqlLedgerWithMutableCache] {
    private val logger = ContextualizedLogger.get(getClass)

    override def acquire()(implicit
        context: ResourceContext
    ): Resource[ReadOnlySqlLedgerWithMutableCache] =
      for {
        ledgerEnd <- Resource.fromFuture(
          ledgerDao.lookupLedgerEnd()
        )
        _ = ledgerEndCache.set((ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId))
        _ <- Resource.fromFuture(
          updatingStringInterningView.update(ledgerEnd.lastStringInterningId)
        )
        prefetchingDispatcher <- dispatcherOffsetSeqIdOwner(
          ledgerEnd.lastOffset,
          ledgerEnd.lastEventSeqId,
        ).acquire()
        generalDispatcher <- dispatcherOwner(ledgerEnd.lastOffset).acquire()
        dispatcherLagMeter <- Resource.successful(
          new DispatcherLagMeter((offset, eventSeqId) => {
            ledgerEndCache.set((offset, eventSeqId))
            // the order here is very important: first we need to make data available for point-wise lookups
            // and SQL queries, and only then we can make it available on the streams.
            // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
            generalDispatcher.signalNewHead(offset)
          })(
            metrics.daml.execution.cache.dispatcherLag
          )
        )
        ledger <- ledgerOwner(
          prefetchingDispatcher,
          generalDispatcher,
          dispatcherLagMeter,
          ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId,
        ).acquire()
      } yield ledger

    private def dispatcherOffsetSeqIdOwner(ledgerEnd: Offset, evtSeqId: Long) = {
      Dispatcher.owner(
        name = "transaction-log-updates",
        zeroIndex = (Offset.beforeBegin, EventSequentialId.beforeBegin),
        headAtInitialization = (ledgerEnd, evtSeqId),
      )
    }

    private def dispatcherOwner(ledgerEnd: Offset): ResourceOwner[Dispatcher[Offset]] =
      Dispatcher.owner(
        name = "sql-ledger",
        zeroIndex = Offset.beforeBegin,
        headAtInitialization = ledgerEnd,
      )

    private def ledgerOwner(
        cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        dispatcherLagMeter: DispatcherLagMeter,
        startExclusive: (Offset, Long),
    ) =
      if (enableInMemoryFanOutForLedgerApi)
        ledgerWithMutableCacheAndInMemoryFanOut(
          cacheUpdatesDispatcher,
          generalDispatcher,
          dispatcherLagMeter,
          startExclusive,
        )
      else
        ledgerWithMutableCache(
          cacheUpdatesDispatcher,
          generalDispatcher,
          dispatcherLagMeter,
          startExclusive,
        )

    private def ledgerWithMutableCache(
        cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        dispatcherLagMeter: DispatcherLagMeter,
        startExclusive: (Offset, Long),
    ): ResourceOwner[ReadOnlySqlLedgerWithMutableCache] =
      for {
        contractStore <- new MutableCacheBackedContractStore.OwnerWithSubscription(
          subscribeToContractStateEvents = cacheIndex =>
            cacheUpdatesDispatcher
              .startingAt(
                cacheIndex,
                RangeSource(
                  ledgerDao.transactionsReader.getContractStateEvents(_, _)
                ),
              )
              .map(_._2),
          contractsReader = ledgerDao.contractsReader,
          signalNewLedgerHead = dispatcherLagMeter,
          startIndexExclusive = startExclusive,
          metrics = metrics,
          maxContractsCacheSize = maxContractStateCacheSize,
          maxKeyCacheSize = maxContractKeyStateCacheSize,
          executionContext = servicesExecutionContext,
        )
        ledger <- ResourceOwner.forCloseable(() =>
          new ReadOnlySqlLedgerWithMutableCache(
            ledgerId,
            ledgerDao,
            ledgerDao.transactionsReader,
            contractStore,
            PruneBuffersNoOp,
            cacheUpdatesDispatcher,
            generalDispatcher,
            dispatcherLagMeter,
            updatingStringInterningView,
          )
        )
      } yield ledger

    private def ledgerWithMutableCacheAndInMemoryFanOut(
        cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        dispatcherLagMeter: DispatcherLagMeter,
        startExclusive: (Offset, Long),
    ): ResourceOwner[ReadOnlySqlLedgerWithMutableCache] = {

      val transactionsBuffer = new EventsBuffer[Offset, TransactionLogUpdate](
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        bufferQualifier = "transactions",
        isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
      )

      val contractStore = MutableCacheBackedContractStore(
        ledgerDao.contractsReader,
        dispatcherLagMeter,
        startExclusive,
        metrics,
        maxContractStateCacheSize,
        maxContractKeyStateCacheSize,
      )(servicesExecutionContext, loggingContext)

      val bufferedTransactionsReader = BufferedTransactionsReader(
        delegate = ledgerDao.transactionsReader,
        transactionsBuffer = transactionsBuffer,
        lfValueTranslation = new LfValueTranslation(
          cache = LfValueTranslationCache.Cache.none,
          metrics = metrics,
          enricherO = Some(enricher),
          loadPackage =
            (packageId, loggingContext) => ledgerDao.getLfArchive(packageId)(loggingContext),
        ),
        metrics = metrics,
      )(loggingContext, servicesExecutionContext)

      for {
        _ <- ResourceOwner.forCloseable(() =>
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
                    ledgerDao.transactionsReader.getTransactionLogUpdates(_, _)
                  ),
                )
            },
            updateTransactionsBuffer = transactionsBuffer.push,
            updateMutableCache = contractStore.push,
            executionContext = servicesExecutionContext,
          )
        )
        ledger <- ResourceOwner.forCloseable(() =>
          new ReadOnlySqlLedgerWithMutableCache(
            ledgerId = ledgerId,
            ledgerDao = ledgerDao,
            ledgerDaoTransactionsReader = bufferedTransactionsReader,
            pruneBuffers = transactionsBuffer.prune,
            contractStore = contractStore,
            contractStateEventsDispatcher = cacheUpdatesDispatcher,
            dispatcher = generalDispatcher,
            dispatcherLagger = dispatcherLagMeter,
            updatingStringInterningView = updatingStringInterningView,
          )
        )
      } yield ledger
    }
  }

  /** Computes the lag between the contract state events dispatcher and the general dispatcher.
    *
    * Internally uses a size bound for preventing memory leaks if misused.
    *
    * @param delegate The ledger head dispatcher delegate.
    * @param timer The timer measuring the delta.
    */
  private class DispatcherLagMeter(delegate: SignalNewLedgerHead, maxSize: Long = 1000L)(
      timer: Timer
  ) extends SignalNewLedgerHead {
    private val ledgerHeads = mutable.Map.empty[Offset, Long]

    override def apply(offset: Offset, sequentialEventId: Long): Unit = {
      delegate(offset, sequentialEventId)
      ledgerHeads.synchronized {
        ledgerHeads.remove(offset).foreach { startNanos =>
          val endNanos = System.nanoTime()
          timer.update(endNanos - startNanos, TimeUnit.NANOSECONDS)
        }
      }
    }

    private[ReadOnlySqlLedgerWithMutableCache] def startTimer(head: Offset): Unit =
      ledgerHeads.synchronized {
        ensureBounded()
        discard(ledgerHeads.getOrElseUpdate(head, System.nanoTime()))
      }

    private def ensureBounded(): Unit =
      if (ledgerHeads.size > maxSize) {
        // If maxSize is reached, remove randomly ANY element.
        ledgerHeads.headOption.foreach(head => ledgerHeads.remove(head._1))
      } else ()
  }
}

private final class ReadOnlySqlLedgerWithMutableCache(
    ledgerId: LedgerId,
    ledgerDao: LedgerReadDao,
    ledgerDaoTransactionsReader: LedgerDaoTransactionsReader,
    contractStore: MutableCacheBackedContractStore,
    pruneBuffers: PruneBuffers,
    contractStateEventsDispatcher: Dispatcher[(Offset, Long)],
    dispatcher: Dispatcher[Offset],
    dispatcherLagger: DispatcherLagMeter,
    updatingStringInterningView: UpdatingStringInterningView,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends ReadOnlySqlLedger(
      ledgerId,
      ledgerDao,
      ledgerDaoTransactionsReader,
      contractStore,
      pruneBuffers,
      dispatcher,
    ) {

  protected val (ledgerEndUpdateKillSwitch, ledgerEndUpdateDone) =
    RestartSource
      .withBackoff(
        RestartSettings(minBackoff = 1.second, maxBackoff = 10.seconds, randomFactor = 0.2)
      )(() =>
        Source
          .tick(0.millis, 100.millis, ())
          .mapAsync(1) {
            implicit val ec: ExecutionContext = mat.executionContext
            _ =>
              for {
                ledgerEnd <- ledgerDao.lookupLedgerEnd()
                _ <- updatingStringInterningView.update(ledgerEnd.lastStringInterningId)
              } yield ledgerEnd
          }
      )
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.foreach { case newLedgerHead =>
        dispatcherLagger.startTimer(newLedgerHead.lastOffset)
        contractStateEventsDispatcher.signalNewHead(
          newLedgerHead.lastOffset -> newLedgerHead.lastEventSeqId
        )
      })(
        Keep.both[UniqueKillSwitch, Future[Done]]
      )
      .run()

  override def close(): Unit = {
    ledgerEndUpdateKillSwitch.shutdown()

    Await.ready(ledgerEndUpdateDone, 10.seconds)

    super.close()
  }
}
