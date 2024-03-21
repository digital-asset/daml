// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.actor.Cancellable
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.stream.{KillSwitches, Materializer, RestartSettings, UniqueKillSwitch}
import akka.{Done, NotUsed}
import com.daml.logging.LoggingContext
import com.daml.platform.store.dao.LedgerReadDao
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/** Periodically polls the ledger end from the [[LedgerReadDao]]
  * and updates the caches backing the Ledger API.
  */
private[index] class LedgerEndPoller(
    ledgerReadDao: LedgerReadDao,
    consume: LedgerEnd => Future[Unit],
)(implicit mat: Materializer, loggingContext: LoggingContext) {

  private val restartSettings =
    RestartSettings(minBackoff = 1.second, maxBackoff = 10.seconds, randomFactor = 0.2)

  private val poller: () => Source[LedgerEnd, Cancellable] =
    () =>
      Source
        .tick(0.millis, 100.millis, ())
        .mapAsync(parallelism = 1) { _ => ledgerReadDao.lookupLedgerEnd() }

  private val (ledgerEndUpdateKillSwitch, ledgerEndUpdateDone) = RestartSource
    .withBackoff(restartSettings)(poller)
    .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
    .toMat(Sink.foreachAsync(parallelism = 1)(consume))(Keep.both[UniqueKillSwitch, Future[Done]])
    .run()

  def release(): Future[Unit] = {
    ledgerEndUpdateKillSwitch.shutdown()
    ledgerEndUpdateDone.map(_ => ())(ExecutionContext.parasitic)
  }
}
