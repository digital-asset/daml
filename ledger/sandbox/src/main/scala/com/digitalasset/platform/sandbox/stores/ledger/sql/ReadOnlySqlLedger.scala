// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import com.digitalasset.ledger.api.domain.LedgerId
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.stores.ledger.ReadOnlyLedger
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{
  JdbcLedgerDao,
  LedgerDao,
  LedgerReadDao
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object ReadOnlySqlLedger {

  val noOfShortLivedConnections = 16
  val noOfStreamingConnections = 2

  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  def apply(jdbcUrl: String, ledgerId: Option[LedgerId])(
      implicit mat: Materializer,
      mm: MetricsManager): Future[ReadOnlyLedger] = {
    implicit val ec: ExecutionContext = DEC

    val dbType = JdbcLedgerDao.jdbcType(jdbcUrl)
    val dbDispatcher =
      DbDispatcher(jdbcUrl, dbType, noOfShortLivedConnections, noOfStreamingConnections)
    val ledgerReadDao = LedgerDao.meteredRead(
      JdbcLedgerDao(
        dbDispatcher,
        ContractSerializer,
        TransactionSerializer,
        ValueSerializer,
        KeyHasher,
        dbType))

    ReadOnlySqlLedgerFactory(ledgerReadDao).createReadOnlySqlLedger(ledgerId)
  }
}

private class ReadOnlySqlLedger(
    ledgerId: LedgerId,
    headAtInitialization: Long,
    ledgerDao: LedgerReadDao)(implicit mat: Materializer)
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

  override def close(): Unit = {
    ledgerEndUpdateKillSwitch.shutdown()
    super.close()
  }
}

private class ReadOnlySqlLedgerFactory(ledgerDao: LedgerReadDao) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** *
    * Creates a DB backed Ledger implementation.
    *
    * @param initialLedgerId a random ledger id is generated if none given, if set it's used to initialize the ledger.
    *                        In case the ledger had already been initialized, the given ledger id must not be set or must
    *                        be equal to the one in the database.
    * @return a compliant read-only Ledger implementation
    */
  def createReadOnlySqlLedger(initialLedgerId: Option[LedgerId])(
      implicit mat: Materializer): Future[ReadOnlySqlLedger] = {

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
            case Some(foundLedgerId) if (foundLedgerId == initialId) =>
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

private object ReadOnlySqlLedgerFactory {
  def apply(ledgerDao: LedgerReadDao): ReadOnlySqlLedgerFactory =
    new ReadOnlySqlLedgerFactory(ledgerDao)
}
