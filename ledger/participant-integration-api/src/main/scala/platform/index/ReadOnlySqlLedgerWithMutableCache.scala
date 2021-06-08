// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import java.util.concurrent.TimeUnit

import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.codahale.metrics.Timer
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.index.ReadOnlySqlLedgerWithMutableCache.DispatcherLagMeter
import com.daml.platform.store.cache.MutableCacheBackedContractStore.SignalNewLedgerHead
import com.daml.platform.store.cache.{EventsBuffer, MutableCacheBackedContractStore}
import com.daml.platform.store.dao.LedgerReadDao
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.scalautil.Statement.discard

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

private[index] object ReadOnlySqlLedgerWithMutableCache {
  final class Owner(
      ledgerDao: LedgerReadDao,
      ledgerId: LedgerId,
      metrics: Metrics,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Long,
      enableInMemoryFanOutForLedgerApi: Boolean,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[ReadOnlySqlLedgerWithMutableCache] {

    override def acquire()(implicit
        context: ResourceContext
    ): Resource[ReadOnlySqlLedgerWithMutableCache] =
      for {
        (ledgerEndOffset, ledgerEndSequentialId) <- Resource.fromFuture(
          ledgerDao.lookupLedgerEndOffsetAndSequentialId()
        )
        prefetchingDispatcher <- dispatcherOffsetSeqIdOwner(
          ledgerEndOffset,
          ledgerEndSequentialId,
        ).acquire()
        generalDispatcher <- dispatcherOwner(ledgerEndOffset).acquire()
        dispatcherLagMeter <- Resource.successful(
          new DispatcherLagMeter(generalDispatcher.signalNewHead)(
            metrics.daml.execution.cache.dispatcherLag
          )
        )
        ledger <- ledgerOwner(
          prefetchingDispatcher,
          generalDispatcher,
          dispatcherLagMeter,
        )
      } yield ledger

    private def dispatcherOffsetSeqIdOwner(ledgerEnd: Offset, evtSeqId: Long) = {
      Dispatcher.owner(
        name = "transaction-log-updates",
        zeroIndex =
          (Offset.beforeBegin, 0L), // TODO: append-only: FIXME consolidating parameters table
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
    )(implicit
        resourceContext: ResourceContext
    ) =
      if (enableInMemoryFanOutForLedgerApi)
        ledgerWithMutableCacheAndInMemoryFanOut(
          cacheUpdatesDispatcher,
          generalDispatcher,
          dispatcherLagMeter,
        )
      else
        ledgerWithMutableCache(cacheUpdatesDispatcher, generalDispatcher, dispatcherLagMeter)

    private def ledgerWithMutableCache(
        cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        dispatcherLagMeter: DispatcherLagMeter,
    )(implicit resourceContext: ResourceContext) =
      for {
        contractStore <- MutableCacheBackedContractStore
          .ownerWithSubscription(
            subscribeToContractStateEvents = (offset, eventSequentialId) =>
              cacheUpdatesDispatcher
                .startingAt(
                  offset -> eventSequentialId,
                  RangeSource(
                    ledgerDao.transactionsReader.getContractStateEvents(_, _)
                  ),
                )
                .map(_._2),
            contractsReader = ledgerDao.contractsReader,
            signalNewLedgerHead = dispatcherLagMeter,
            metrics = metrics,
            maxContractsCacheSize = maxContractStateCacheSize,
            maxKeyCacheSize = maxContractKeyStateCacheSize,
          )
        ledger <- ResourceOwner
          .forCloseable(() =>
            new ReadOnlySqlLedgerWithMutableCache(
              ledgerId,
              ledgerDao,
              contractStore,
              cacheUpdatesDispatcher,
              generalDispatcher,
              dispatcherLagMeter,
            )
          )
          .acquire()
      } yield ledger

    private def ledgerWithMutableCacheAndInMemoryFanOut(
        cacheUpdatesDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        dispatcherLagMeter: DispatcherLagMeter,
    )(implicit resourceContext: ResourceContext) = {
      // TODO: in-memory fan-out - wire-up prunning to transactionsBuffer
      val transactionsBuffer = new EventsBuffer[Offset, TransactionLogUpdate](
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        bufferQualifier = "transactions",
        isRangeEndMarker = _.isInstanceOf[TransactionLogUpdate.LedgerEndMarker],
      )
      for {
        contractStore <- MutableCacheBackedContractStore.owner(
          contractsReader = ledgerDao.contractsReader,
          signalNewLedgerHead = dispatcherLagMeter,
          metrics = metrics,
          maxContractsCacheSize = maxContractStateCacheSize,
          maxKeyCacheSize = maxContractKeyStateCacheSize,
        )
        _ <- ResourceOwner
          .forCloseable(() =>
            BuffersUpdater(
              subscribeToTransactionLogUpdates = (offset, eventSequentialId) =>
                cacheUpdatesDispatcher
                  .startingAt(
                    offset -> eventSequentialId,
                    RangeSource(
                      ledgerDao.transactionsReader.getTransactionLogUpdates(_, _)
                    ),
                  ),
              updateTransactionsBuffer = transactionsBuffer.push,
              updateMutableCache = contractStore.push,
            )
          )
          .acquire()
        ledger <- ResourceOwner
          .forCloseable(() =>
            new ReadOnlySqlLedgerWithMutableCache(
              ledgerId,
              ledgerDao,
              contractStore,
              cacheUpdatesDispatcher,
              generalDispatcher,
              dispatcherLagMeter,
            )
          )
          .acquire()
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

    override def apply(offset: Offset): Unit = {
      delegate(offset)
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
    contractStore: MutableCacheBackedContractStore,
    contractStateEventsDispatcher: Dispatcher[(Offset, Long)],
    dispatcher: Dispatcher[Offset],
    dispatcherLagger: DispatcherLagMeter,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends ReadOnlySqlLedger(ledgerId, ledgerDao, contractStore, dispatcher) {

  protected val (ledgerEndUpdateKillSwitch, ledgerEndUpdateDone) =
    RestartSource
      .withBackoff(
        RestartSettings(minBackoff = 1.second, maxBackoff = 10.seconds, randomFactor = 0.2)
      )(() =>
        Source
          .tick(0.millis, 100.millis, ())
          .mapAsync(1)(_ => ledgerDao.lookupLedgerEndOffsetAndSequentialId())
      )
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.foreach { case newLedgerHead @ (offset, _) =>
        dispatcherLagger.startTimer(offset)
        contractStateEventsDispatcher.signalNewHead(newLedgerHead)
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
