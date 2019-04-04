// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.time.Instant

import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import akka.{Done, NotUsed}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
import com.digitalasset.ledger.backend.api.v1.TransactionSubmission
import com.digitalasset.platform.akkastreams.Dispatcher
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.sandbox.config.LedgerIdGenerator
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter
import com.digitalasset.platform.sandbox.stores.ActiveContracts
import com.digitalasset.platform.sandbox.stores.ActiveContracts.ActiveContract
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{
  Contract,
  LedgerDao,
  PostgresLedgerDao
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  TransactionSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import com.digitalasset.platform.sandbox.stores.ledger.{Ledger, LedgerEntry, LedgerSnapshot}
import org.slf4j.LoggerFactory

import scala.collection.{breakOut, immutable}
import scala.concurrent.{ExecutionContext, Future}

object SqlLedger {
  def apply(
      jdbcUrl: String,
      jdbcUser: String,
      ledgerId: Option[String],
      timeProvider: TimeProvider,
      acs: ActiveContracts,
      ledgerEntries: immutable.Seq[LedgerEntry])(implicit mat: Materializer): Future[Ledger] = {
    implicit val ec: ExecutionContext = DirectExecutionContext

    val noOfConnections = 10

    val dbDispatcher = DbDispatcher(jdbcUrl, jdbcUser, noOfConnections)
    val ledgerDao = PostgresLedgerDao(dbDispatcher, ContractSerializer, TransactionSerializer)
    val sqlLedgerFactory = SqlLedgerFactory(ledgerDao)

    for {
      sqlLedger <- sqlLedgerFactory.createSqlLedger(ledgerId, timeProvider)
      _ <- sqlLedger.loadStartingState(acs, ledgerEntries)
    } yield sqlLedger
  }
}

private class SqlLedger(
    val ledgerId: String,
    headAtInitialization: Long,
    ledgerDao: LedgerDao,
    timeProvider: TimeProvider)(implicit mat: Materializer)
    extends Ledger
    with AutoCloseable {

  private val logger = LoggerFactory.getLogger(getClass)

  private val dispatcher = Dispatcher[Long, LedgerEntry](
    readSuccessor = (o, _) => o + 1,
    readElement = ledgerDao.lookupLedgerEntryAssert,
    firstIndex = 0l,
    headAtInitialization = headAtInitialization
  )

  @volatile
  private var headRef: Long = headAtInitialization
  // the reason for modelling persistence as a reactive pipeline is to avoid having race-conditions between the
  // moving ledger-end, the async persistence operation and the dispatcher head notification
  private val persistenceQueue: SourceQueueWithComplete[Long => LedgerEntry] =
    createPersistenceQueue()

  private def createPersistenceQueue(): SourceQueueWithComplete[Long => LedgerEntry] = {
    val offsetGenerator: Source[Long, NotUsed] =
      Source.fromIterator(() => Iterator.iterate(headAtInitialization)(l => l + 1))
    val persistenceQueue = Source.queue[Long => LedgerEntry](128, OverflowStrategy.backpressure)
    implicit val ec: ExecutionContext = DirectExecutionContext
    persistenceQueue
      .zipWith(offsetGenerator)((f, offset) => offset -> f(offset))
      .mapAsync(1) {
        case (offset, ledgerEntry) => //strictly one after another!
          for {
            _ <- ledgerDao.storeLedgerEntry(offset, ledgerEntry)
            _ = dispatcher.signalNewHead(offset) //signalling downstream subscriptions
            _ = headRef = offset //updating the headRef
          } yield ()
      }
      .toMat(Sink.ignore)(Keep.left[SourceQueueWithComplete[Long => LedgerEntry], Future[Done]])
      .run()
  }

  override def close(): Unit = persistenceQueue.complete()

  private def loadStartingState(
      acs: ActiveContracts,
      ledgerEntries: immutable.Seq[LedgerEntry]): Future[Unit] =
    if (acs.contracts.nonEmpty || ledgerEntries.nonEmpty) {
      logger.info("initializing ledger with scenario output")
      implicit val ec: ExecutionContext = DirectExecutionContext
      //ledger entries must be persisted via the persistenceQueue!
      val fDone = Source(ledgerEntries)
        .mapAsync(1) { ledgerEntry =>
          persistenceQueue.offer(_ => ledgerEntry)
        }
        .runWith(Sink.ignore)

      val mappedContracts: immutable.Seq[Contract] = acs.contracts.map {
        case (cId, c) =>
          Contract(cId, c.let, c.transactionId, c.workflowId, c.witnesses, c.contract)
      }(breakOut)

      for {
        _ <- fDone
        _ <- ledgerDao.storeContracts(mappedContracts) //efficient batch insert
      } yield ()
    } else Future.successful(())

  override def ledgerEntries(offset: Option[Long]): Source[(Long, LedgerEntry), NotUsed] = {
    //TODO perf optimisation: the Dispatcher queries every ledger entry one by one, which is really not optimal having a relational db. We should change this behaviour and use a simple sql query fetching the ledger entry at once, at least until ledger end.
    dispatcher.startingAt(offset.getOrElse(0))
  }

  override def ledgerEnd: Long = headRef

  override def snapshot(): Future[LedgerSnapshot] = ??? //TODO implement it with a simple sql query

  override def lookupContract(
      contractId: Value.AbsoluteContractId): Future[Option[ActiveContract]] =
    ledgerDao
      .lookupActiveContract(contractId)
      .map(_.map {
        case Contract(_, let, transactionId, workflowId, witnesses, coinst) =>
          ActiveContract(let, transactionId, workflowId, coinst, witnesses, None)
      })(DirectExecutionContext)

  override def lookupKey(key: Node.GlobalKey): Future[Option[AbsoluteContractId]] =
    sys.error("contract keys not implemented yet in SQL backend")

  override def publishHeartbeat(time: Instant): Future[Unit] =
    persistenceQueue
      .offer(_ => LedgerEntry.Checkpoint(time))
      .map(_ => ())(DirectExecutionContext)

  override def publishTransaction(tx: TransactionSubmission): Future[Unit] =
    persistenceQueue
      .offer { offset =>
        val transactionId = offset.toString
        val toAbsCoid: ContractId => AbsoluteContractId =
          SandboxEventIdFormatter.makeAbsCoid(transactionId)
        val mappedTx = tx.transaction
          .mapContractIdAndValue(toAbsCoid, _.mapContractId(toAbsCoid))
          .mapNodeId(_.index.toString)

        val mappedDisclosure = tx.blindingInfo.explicitDisclosure
          .map {
            case (nodeId, party) =>
              nodeId.index.toString -> party.map(_.underlyingString)
          }

        LedgerEntry.Transaction(
          tx.commandId,
          transactionId,
          tx.applicationId,
          tx.submitter,
          tx.workflowId,
          tx.ledgerEffectiveTime,
          tx.maximumRecordTime,
          mappedTx,
          mappedDisclosure
        )
      }
      .map(_ => ())(DirectExecutionContext)
}

private class SqlLedgerFactory(ledgerDao: LedgerDao) {

  private val logger = LoggerFactory.getLogger(getClass)

  /** *
    * Creates a DB backed Ledger implementation.
    *
    * @param initialLedgerId a random ledger id is generated if none given, if set it's used to initialize the ledger.
    *                        In case the ledger had already been initialized, the given ledger id must not be set or must
    *                        be equal to the one in the database.
    * @param timeProvider    to get the current time when sequencing transactions
    * @return a compliant Ledger implementation
    */
  def createSqlLedger(initialLedgerId: Option[String], timeProvider: TimeProvider)(
      implicit mat: Materializer): Future[SqlLedger] = {
    @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
    implicit val ec = DirectExecutionContext
    for {
      ledgerId <- figureOutLedgerId(initialLedgerId)
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
    } yield new SqlLedger(ledgerId, ledgerEnd, ledgerDao, timeProvider)
  }

  private def figureOutLedgerId(initialLedgerId: Option[String]) = initialLedgerId match {
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
            sys.error(errorMsg)
          case None =>
            doInit(initialId)
        }(DirectExecutionContext)

    case None =>
      logger.info("No ledger id given. Looking for existing ledger in database.")
      ledgerDao
        .lookupLedgerId()
        .flatMap {
          case Some(foundLedgerId) =>
            ledgerFound(foundLedgerId)
          case None => doInit(LedgerIdGenerator.generateRandomId())
        }(DirectExecutionContext)
  }

  private def ledgerFound(foundLedgerId: String) = {
    logger.info(s"Found existing ledger with id: $foundLedgerId")
    Future.successful(foundLedgerId)
  }

  private def doInit(ledgerId: String): Future[String] = {
    logger.info(s"Initializing ledger with id: $ledgerId")
    implicit val ec: ExecutionContext = DirectExecutionContext
    for {
      _ <- ledgerDao.storeLedgerId(ledgerId)
      _ <- ledgerDao.storeInitialLedgerEnd(0)
    } yield (ledgerId)
  }

}

private object SqlLedgerFactory {
  def apply(ledgerDao: LedgerDao): SqlLedgerFactory = new SqlLedgerFactory(ledgerDao)
}
