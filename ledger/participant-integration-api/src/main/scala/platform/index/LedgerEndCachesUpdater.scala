// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.{Done, NotUsed}
import akka.stream.{KillSwitches, Materializer, RestartSettings, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import com.daml.ledger.offset.Offset
import com.daml.logging.LoggingContext
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.store.appendonlydao.LedgerReadDao
import com.daml.platform.store.interning.UpdatingStringInterningView
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/** Periodically polls the ledger end from the [[LedgerReadDao]]
  * and updates the caches backing the Ledger API.
  */
private[index] class LedgerEndCachesUpdater(
    ledgerReadDao: LedgerReadDao,
    updatingStringInterningView: UpdatingStringInterningView,
    instrumentedSignalNewLedgerHead: InstrumentedSignalNewLedgerHead,
    contractStateEventsDispatcher: Dispatcher[(Offset, Long)],
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends AutoCloseable {

  private val (ledgerEndUpdateKillSwitch, ledgerEndUpdateDone) =
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
                ledgerEnd <- ledgerReadDao.lookupLedgerEnd()
                _ <- updatingStringInterningView.update(ledgerEnd.lastStringInterningId)
              } yield ledgerEnd
          }
      )
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.foreach { newLedgerHead =>
        instrumentedSignalNewLedgerHead.startTimer(newLedgerHead.lastOffset)
        contractStateEventsDispatcher.signalNewHead(
          newLedgerHead.lastOffset -> newLedgerHead.lastEventSeqId
        )
      })(Keep.both[UniqueKillSwitch, Future[Done]])
      .run()

  def close(): Unit = {
    ledgerEndUpdateKillSwitch.shutdown()
    Await.ready(ledgerEndUpdateDone, 10.seconds)
    ()
  }
}
