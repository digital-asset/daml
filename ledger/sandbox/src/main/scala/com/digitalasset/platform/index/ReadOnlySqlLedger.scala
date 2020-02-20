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
import com.digitalasset.platform.store.dao.{
  DbDispatcher,
  JdbcLedgerDao,
  LedgerReadDao,
  MeteredLedgerReadDao
}
import com.digitalasset.platform.store.{BaseLedger, DbType, ReadOnlyLedger}
import com.digitalasset.resources.{Resource, ResourceOwner}
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object ReadOnlySqlLedger {

  val maxConnections = 16

  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  def apply(
      jdbcUrl: String,
      ledgerId: Option[LedgerId],
      metrics: MetricRegistry,
      implicitPartyAllocation: Boolean = false,
  )(implicit mat: Materializer, logCtx: LoggingContext): Resource[ReadOnlyLedger] = {
    implicit val ec: ExecutionContext = mat.executionContext
    val dbType = DbType.jdbcType(jdbcUrl)
    for {
      dbDispatcher <- DbDispatcher
        .owner(jdbcUrl, maxConnections, metrics)
        .acquire()
      ledgerReadDao = new MeteredLedgerReadDao(
        JdbcLedgerDao(dbDispatcher, dbType, mat.executionContext, implicitPartyAllocation),
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
    def createReadOnlySqlLedger(initialLedgerId: Option[LedgerId])(
        implicit mat: Materializer
    ): Future[ReadOnlySqlLedger] = {

      implicit val ec: ExecutionContext = DEC

      for {
        ledgerId <- initialize(initialLedgerId)
        ledgerEnd <- ledgerDao.lookupLedgerEnd()
      } yield {

        new ReadOnlySqlLedger(ledgerId, ledgerEnd, ledgerDao)
      }
    }

    private def initialize(initialLedgerId: Option[LedgerId]): Future[LedgerId] = {
      // Note that here we only store the ledger entry and we do not update anything else, such as the
      // headRef. We also are not concerns with heartbeats / checkpoints. This is OK since this initialization
      // step happens before we start up the sql ledger at all, so it's running in isolation.

      initialLedgerId match {
        case Some(initialId) =>
          ledgerDao
            .lookupLedgerId()
            .flatMap {
              case Some(foundLedgerId) if foundLedgerId == initialId =>
                ledgerFound(foundLedgerId)
              case Some(foundLedgerId) =>
                val errorMsg =
                  s"Ledger id mismatch. Ledger id given ('$initialId') is not equal to the existing one ('$foundLedgerId')!"
                logger.error(errorMsg)
                Future.failed(new IllegalArgumentException(errorMsg))
              case None =>
                Future.successful(initialId)

            }(DEC)

        case None =>
          logger.info("No ledger id given. Looking for existing ledger in database.")
          ledgerDao
            .lookupLedgerId()
            .flatMap {
              case Some(foundLedgerId) => ledgerFound(foundLedgerId)
              case None =>
                Future.failed(new IllegalStateException("Underlying ledger not yet initialized"))
            }(DEC)
      }
    }

    private def ledgerFound(foundLedgerId: LedgerId) = {
      logger.info(s"Found existing ledger with id: ${foundLedgerId.unwrap}")
      Future.successful(foundLedgerId)
    }
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
