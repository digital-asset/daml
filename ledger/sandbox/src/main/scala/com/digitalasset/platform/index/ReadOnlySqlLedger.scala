// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.time.Instant

import akka.actor.Cancellable
import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import akka.{Done, NotUsed}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.Offset
import com.digitalasset.dec.{DirectExecutionContext => DEC}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.common.LedgerIdMismatchException
import com.digitalasset.platform.store.dao.{JdbcLedgerDao, LedgerReadDao}
import com.digitalasset.platform.store.{BaseLedger, ReadOnlyLedger}
import com.digitalasset.resources.ProgramResource.StartupException
import com.digitalasset.resources.ResourceOwner

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object ReadOnlySqlLedger {

  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  def owner(
      name: String,
      jdbcUrl: String,
      ledgerId: LedgerId,
      metrics: MetricRegistry,
  )(implicit mat: Materializer, logCtx: LoggingContext): ResourceOwner[ReadOnlyLedger] =
    for {
      ledgerReadDao <- JdbcLedgerDao.readOwner(name, jdbcUrl, metrics)
      factory = new Factory(ledgerReadDao)
      ledger <- ResourceOwner.forFutureCloseable(() => factory.createReadOnlySqlLedger(ledgerId))
    } yield ledger

  private class Factory(ledgerDao: LedgerReadDao)(implicit logCtx: LoggingContext) {

    private val logger = ContextualizedLogger.get(this.getClass)

    /**
      * Creates a DB backed Ledger implementation.
      *
      * @return a compliant read-only Ledger implementation
      */
    def createReadOnlySqlLedger(initialLedgerId: LedgerId)(
        implicit mat: Materializer
    ): Future[ReadOnlySqlLedger] = {
      implicit val ec: ExecutionContext = DEC
      for {
        ledgerId <- initialize(initialLedgerId)
        ledgerEnd <- ledgerDao.lookupLedgerEnd()
      } yield new ReadOnlySqlLedger(ledgerId, ledgerEnd, ledgerDao)
    }

    private def initialize(initialLedgerId: LedgerId): Future[LedgerId] =
      ledgerDao
        .lookupLedgerId()
        .flatMap {
          case Some(foundLedgerId @ `initialLedgerId`) =>
            logger.info(s"Found existing ledger with ID: $foundLedgerId")
            Future.successful(foundLedgerId)
          case Some(foundLedgerId) =>
            Future.failed(
              new LedgerIdMismatchException(foundLedgerId, initialLedgerId) with StartupException)
          case None =>
            Future.successful(initialLedgerId)
        }(DEC)
  }

}

private class ReadOnlySqlLedger(
    ledgerId: LedgerId,
    headAtInitialization: Offset,
    ledgerDao: LedgerReadDao,
)(implicit mat: Materializer)
    extends BaseLedger(ledgerId, headAtInitialization, ledgerDao) {

  private val (ledgerEndUpdateKillSwitch, ledgerEndUpdateDone) =
    RestartSource
      .withBackoff(minBackoff = 1.second, maxBackoff = 10.seconds, randomFactor = 0.2)(
        () =>
          Source
            .tick(0.millis, 100.millis, ())
            .mapAsync(1)(_ => ledgerDao.lookupLedgerEnd()))
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .toMat(Sink.foreach(dispatcher.signalNewHead))(Keep.both[UniqueKillSwitch, Future[Done]])
      .run()

  // Periodically remove all expired deduplication cache entries.
  // The current approach is not ideal for multiple ReadOnlySqlLedgers sharing
  // the same database (as is the case for a horizontally scaled ledger API server).
  // In that case, an external process periodically clearing expired entries would be better.
  //
  // Deduplication entries are added by the submission service, which might use
  // a different clock than the current clock (e.g., horizontally scaled ledger API server).
  // This is not an issue, because applications are not expected to submit towards the end
  // of the deduplication time window.
  private val (deduplicationCleanupKillSwitch, deduplicationCleanupDone) =
    Source
      .tick[Unit](0.millis, 10.minutes, ())
      .mapAsync[Unit](1)(_ => ledgerDao.removeExpiredDeduplicationData(Instant.now()))
      .viaMat(KillSwitches.single)(Keep.right[Cancellable, UniqueKillSwitch])
      .toMat(Sink.ignore)(Keep.both[UniqueKillSwitch, Future[Done]])
      .run()

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def close(): Unit = {
    // Terminate the dispatcher first so that it doesn't trigger new queries.
    super.close()
    deduplicationCleanupKillSwitch.shutdown()
    ledgerEndUpdateKillSwitch.shutdown()
    Await.result(deduplicationCleanupDone, 10.seconds)
    Await.result(ledgerEndUpdateDone, 10.seconds)
    ()
  }
}
