// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.daml.ledger.api.domain.LedgerId
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.ContractStore
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.platform.PruneBuffersNoOp
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.store.LfValueTranslationCache
import com.daml.platform.store.appendonlydao.LedgerReadDao
import com.daml.platform.store.cache.{MutableLedgerEndCache, TranslationCacheBackedContractStore}
import com.daml.platform.store.interning.UpdatingStringInterningView

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

private[index] object ReadOnlySqlLedgerWithTranslationCache {

  final class Owner(
      ledgerDao: LedgerReadDao,
      ledgerEndCache: MutableLedgerEndCache,
      updatingStringInterningView: UpdatingStringInterningView,
      ledgerId: LedgerId,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[ReadOnlySqlLedgerWithTranslationCache] {

    override def acquire()(implicit
        context: ResourceContext
    ): Resource[ReadOnlySqlLedgerWithTranslationCache] =
      for {
        ledgerEnd <- Resource.fromFuture(ledgerDao.lookupLedgerEnd())
        _ = ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
        dispatcher <- dispatcherOwner(ledgerEnd.lastOffset).acquire()
        contractsStore <- contractStoreOwner()
        ledger <- ledgerOwner(dispatcher, contractsStore).acquire()
      } yield ledger

    private def ledgerOwner(dispatcher: Dispatcher[Offset], contractsStore: ContractStore) =
      ResourceOwner
        .forCloseable(() =>
          new ReadOnlySqlLedgerWithTranslationCache(
            ledgerId,
            ledgerDao,
            ledgerEndCache,
            contractsStore,
            dispatcher,
            updatingStringInterningView,
          )
        )

    private def contractStoreOwner(): Resource[ContractStore] =
      TranslationCacheBackedContractStore
        .owner(lfValueTranslationCache, ledgerDao.contractsReader)

    private def dispatcherOwner(ledgerEnd: Offset): ResourceOwner[Dispatcher[Offset]] =
      Dispatcher.owner(
        name = "sql-ledger",
        zeroIndex = Offset.beforeBegin,
        headAtInitialization = ledgerEnd,
      )
  }
}

private final class ReadOnlySqlLedgerWithTranslationCache(
    ledgerId: LedgerId,
    ledgerDao: LedgerReadDao,
    ledgerEndCache: MutableLedgerEndCache,
    contractStore: ContractStore,
    dispatcher: Dispatcher[Offset],
    updatingStringInterningView: UpdatingStringInterningView,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends ReadOnlySqlLedger(
      ledgerId,
      ledgerDao,
      ledgerDao.transactionsReader,
      contractStore,
      PruneBuffersNoOp,
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
      .toMat(Sink.foreach { ledgerEnd =>
        ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
        // the order here is very important: first we need to make data available for point-wise lookups
        // and SQL queries, and only then we can make it available on the streams.
        // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
        dispatcher.signalNewHead(ledgerEnd.lastOffset)
      })(
        Keep.both[UniqueKillSwitch, Future[Done]]
      )
      .run()

  override def close(): Unit = {
    ledgerEndUpdateKillSwitch.shutdown()

    Await.result(ledgerEndUpdateDone, 10.seconds)

    super.close()
  }
}
