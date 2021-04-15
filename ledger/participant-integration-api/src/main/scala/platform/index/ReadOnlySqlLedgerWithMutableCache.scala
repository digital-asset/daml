// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.participant.state.v1.Offset
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.store.appendonlydao.EventSequentialId
import com.daml.platform.store.cache.MutableCacheBackedContractStore
import com.daml.platform.store.dao.LedgerReadDao

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

private[index] object ReadOnlySqlLedgerWithMutableCache {
  final class Owner(
      ledgerDao: LedgerReadDao,
      ledgerId: LedgerId,
      metrics: Metrics,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[ReadOnlySqlLedgerWithMutableCache] {

    override def acquire()(implicit
        context: ResourceContext
    ): Resource[ReadOnlySqlLedgerWithMutableCache] =
      for {
        (ledgerEndOffset, ledgerEndSequentialId) <- Resource.fromFuture(
          ledgerDao.lookupLedgerEndOffsetAndSequentialId()
        )
        contractStateEventsDispatcher <- dispatcherOffsetSeqIdOwner(
          ledgerEndOffset,
          ledgerEndSequentialId,
        ).acquire()
        generalDispatcher <- dispatcherOwner(ledgerEndOffset).acquire()
        contractStore <- contractStoreOwner(generalDispatcher, contractStateEventsDispatcher)
        ledger <- ledgerOwner(contractStateEventsDispatcher, generalDispatcher, contractStore)
          .acquire()
      } yield ledger

    private def ledgerOwner(
        contractStateEventsDispatcher: Dispatcher[(Offset, Long)],
        generalDispatcher: Dispatcher[Offset],
        contractStore: MutableCacheBackedContractStore,
    ) = {
      ResourceOwner
        .forCloseable(() =>
          new ReadOnlySqlLedgerWithMutableCache(
            ledgerId,
            ledgerDao,
            contractStore,
            contractStateEventsDispatcher,
            generalDispatcher,
          )
        )
    }

    private def contractStoreOwner(
        generalDispatcher: Dispatcher[Offset],
        contractStateEventsDispatcher: Dispatcher[(Offset, Long)],
    )(implicit
        context: ResourceContext
    ) =
      MutableCacheBackedContractStore.owner(
        contractsReader = ledgerDao.contractsReader,
        signalNewLedgerHead = generalDispatcher.signalNewHead,
        subscribeToContractStateEvents = (offset: Offset, eventSequentialId: Long) =>
          contractStateEventsDispatcher
            .startingAt(
              offset -> eventSequentialId,
              RangeSource(
                ledgerDao.transactionsReader.getContractStateEvents(_, _)
              ),
            )
            .map(_._2),
        metrics = metrics,
        maxContractsCacheSize = maxContractStateCacheSize,
        maxKeyCacheSize = maxContractKeyStateCacheSize,
      )

    private def dispatcherOffsetSeqIdOwner(ledgerEnd: Offset, evtSeqId: Long) = {
      implicit val ordering: Ordering[(Offset, Long)] = Ordering.fromLessThan {
        case ((fOffset, fSeqId), (sOffset, sSeqId)) =>
          (fOffset < sOffset) || (fOffset == sOffset && fSeqId < sSeqId)
      }
      Dispatcher.owner(
        name = "contract-state-events",
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
  }
}

private final class ReadOnlySqlLedgerWithMutableCache(
    ledgerId: LedgerId,
    ledgerDao: LedgerReadDao,
    contractStore: MutableCacheBackedContractStore,
    contractStateEventsDispatcher: Dispatcher[(Offset, Long)],
    dispatcher: Dispatcher[Offset],
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
      .toMat(Sink.foreach(contractStateEventsDispatcher.signalNewHead))(
        Keep.both[UniqueKillSwitch, Future[Done]]
      )
      .run()

  override def close(): Unit = {
    ledgerEndUpdateKillSwitch.shutdown()

    Await.ready(ledgerEndUpdateDone, 10.seconds)

    super.close()
  }
}
