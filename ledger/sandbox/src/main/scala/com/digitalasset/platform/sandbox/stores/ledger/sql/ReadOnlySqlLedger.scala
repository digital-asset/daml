// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import com.digitalasset.ledger.api.domain.{LedgerId, ParticipantId}
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
  def apply(
      jdbcUrl: String)(implicit mat: Materializer, mm: MetricsManager): Future[ReadOnlyLedger] = {
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

    ReadOnlySqlLedgerFactory(ledgerReadDao).createReadOnlySqlLedger()
  }
}

private class ReadOnlySqlLedger(
    ledgerId: LedgerId,
    participantId: ParticipantId,
    headAtInitialization: Long,
    ledgerDao: LedgerReadDao)(implicit mat: Materializer)
    extends BaseLedger(ledgerId, participantId, headAtInitialization, ledgerDao) {

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
    * @return a compliant read-only Ledger implementation
    */
  def createReadOnlySqlLedger()(implicit mat: Materializer): Future[ReadOnlySqlLedger] = {

    implicit val ec: ExecutionContext = DEC

    for {
      ledgerId <- fetchLedgerId()
      participantId <- fetchParticipantId()
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
    } yield {

      new ReadOnlySqlLedger(ledgerId, participantId, ledgerEnd, ledgerDao)
    }
  }

  private def fetchLedgerId(): Future[LedgerId] = {
    ledgerDao
      .lookupLedgerId()
      .flatMap {
        case Some(foundLedgerId) =>
          logger.info(s"Found existing ledger with id: ${foundLedgerId.unwrap}")
          Future.successful(foundLedgerId)
        case None =>
          val errorMsg =
            s"Ledger id not found in database, most likely because it has not been initialized."
          logger.error(errorMsg)
          Future.failed(new IllegalStateException(errorMsg))
      }(DEC)
  }

  private def fetchParticipantId(): Future[ParticipantId] = {
    ledgerDao
      .lookupParticipantId()
      .flatMap {
        case Some(foundParticipantId) =>
          logger.info(s"Found existing participant with id: ${foundParticipantId.unwrap}")
          Future.successful(foundParticipantId)
        case None =>
          val errorMsg =
            s"Participant id not found in database, most likely because it has not been initialized."
          logger.error(errorMsg)
          Future.failed(new IllegalStateException(errorMsg))
      }(DEC)
  }
}

private object ReadOnlySqlLedgerFactory {
  def apply(ledgerDao: LedgerReadDao): ReadOnlySqlLedgerFactory =
    new ReadOnlySqlLedgerFactory(ledgerDao)
}
