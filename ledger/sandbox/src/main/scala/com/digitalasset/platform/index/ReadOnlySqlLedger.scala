// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.digitalasset.dec.{DirectExecutionContext => DEC}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.common.LedgerIdMismatchException
import com.digitalasset.platform.store.dao.{
  DbDispatcher,
  JdbcLedgerDao,
  LedgerReadDao,
  MeteredLedgerReadDao
}
import com.digitalasset.platform.store.{BaseLedger, DbType, ReadOnlyLedger}
import com.digitalasset.resources.ProgramResource.StartupException
import com.digitalasset.resources.{Resource, ResourceOwner}
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object ReadOnlySqlLedger {

  val maxConnections = 16

  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  def apply(
      jdbcUrl: String,
      ledgerId: LedgerId,
      metrics: MetricRegistry,
  )(implicit mat: Materializer, logCtx: LoggingContext): Resource[ReadOnlyLedger] = {
    implicit val ec: ExecutionContext = mat.executionContext
    val dbType = DbType.jdbcType(jdbcUrl)
    for {
      dbDispatcher <- DbDispatcher
        .owner(jdbcUrl, maxConnections, metrics)
        .acquire()
      ledgerReadDao = new MeteredLedgerReadDao(
        JdbcLedgerDao(dbDispatcher, dbType, mat.executionContext),
        metrics,
      )
      factory = new Factory(ledgerReadDao)
      ledger <- ResourceOwner
        .forFutureCloseable(() => factory.createReadOnlySqlLedger(ledgerId))
        .acquire()
    } yield ledger
  }

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
            logger.info(s"Found existing ledger with ID: ${foundLedgerId.unwrap}")
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
    headAtInitialization: Long,
    ledgerDao: LedgerReadDao,
)(implicit mat: Materializer)
    extends BaseLedger(ledgerId, headAtInitialization, ledgerDao) {

  private val ledgerEndUpdateKillSwitch = {
    val offsetUpdates = Source
      .tick(0.millis, 100.millis, ())
      .mapAsync(1)(_ => ledgerDao.lookupLedgerEnd())

    RestartSource
      .withBackoff(
        minBackoff = 1.second,
        maxBackoff = 10.seconds,
        randomFactor = 0.2
      )(() => offsetUpdates)
      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
      .to(Sink.foreach(dispatcher.signalNewHead))
      .run()
  }

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def close(): Unit = {
    ledgerEndUpdateKillSwitch.shutdown()
    super.close()
  }
}
