// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.time.Instant

import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.{GraphDSL, Keep, MergePreferred, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, SourceShape}
import akka.{Done, NotUsed}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
import com.digitalasset.ledger.backend.api.v1.{SubmissionResult, TransactionSubmission}
import com.digitalasset.platform.akkastreams.Dispatcher
import com.digitalasset.platform.akkastreams.SteppingMode.RangeQuery
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.sandbox.config.LedgerIdGenerator
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter
import com.digitalasset.platform.sandbox.stores.ActiveContracts.ActiveContract
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlStartMode.{
  AlwaysReset,
  ContinueIfExists
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.PersistenceResponse.{Duplicate, Ok}
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao.{LedgerDao, PostgresLedgerDao}
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import com.digitalasset.platform.sandbox.stores.ledger.{Ledger, LedgerEntry, LedgerSnapshot}
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

sealed abstract class SqlStartMode extends Product with Serializable

object SqlStartMode {

  /** Will continue using an initialised ledger, otherwise initialize a new one */
  final case object ContinueIfExists extends SqlStartMode

  /** Will always reset and initialize the ledger, even if it has data.  */
  final case object AlwaysReset extends SqlStartMode

}

object SqlLedger {
  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  def apply(
      jdbcUrl: String,
      ledgerId: Option[String],
      timeProvider: TimeProvider,
      ledgerEntries: immutable.Seq[LedgerEntry],
      startMode: SqlStartMode = SqlStartMode.ContinueIfExists)(
      implicit mat: Materializer,
      mm: MetricsManager): Future[Ledger] = {
    implicit val ec: ExecutionContext = DirectExecutionContext

    val noOfShortLivedConnections = 8
    val noOfStreamingConnections = 4

    val dbDispatcher = DbDispatcher(jdbcUrl, noOfShortLivedConnections, noOfStreamingConnections)
    val ledgerDao = LedgerDao.metered(
      PostgresLedgerDao(
        dbDispatcher,
        ContractSerializer,
        TransactionSerializer,
        ValueSerializer,
        KeyHasher))

    val sqlLedgerFactory = SqlLedgerFactory(ledgerDao)

    for {
      sqlLedger <- sqlLedgerFactory.createSqlLedger(ledgerId, timeProvider, startMode)
      _ <- sqlLedger.loadStartingState(ledgerEntries)
    } yield sqlLedger
  }
}

private class SqlLedger(
    val ledgerId: String,
    headAtInitialization: Long,
    ledgerDao: LedgerDao,
    timeProvider: TimeProvider)(implicit mat: Materializer)
    extends Ledger {

  private val logger = LoggerFactory.getLogger(getClass)

  private def nextOffset(o: Long): Long = o + 1

  private val dispatcher = Dispatcher[Long, LedgerEntry](
    RangeQuery(ledgerDao.getLedgerEntries(_, _)),
    0l,
    headAtInitialization
  )

  @volatile
  private var headRef: Long = headAtInitialization
  // the reason for modelling persistence as a reactive pipeline is to avoid having race-conditions between the
  // moving ledger-end, the async persistence operation and the dispatcher head notification
  private val (checkpointQueue, persistenceQueue): (
      SourceQueueWithComplete[Long => LedgerEntry],
      SourceQueueWithComplete[Long => LedgerEntry]) = createQueues()

  private def createQueues(): (
      SourceQueueWithComplete[Long => LedgerEntry],
      SourceQueueWithComplete[Long => LedgerEntry]) = {

    val checkpointQueue = Source.queue[Long => LedgerEntry](1, OverflowStrategy.dropHead)
    val persistenceQueue = Source.queue[Long => LedgerEntry](128, OverflowStrategy.dropNew)

    implicit val ec: ExecutionContext = DirectExecutionContext

    val mergedSources = Source.fromGraph(GraphDSL.create(checkpointQueue, persistenceQueue) {
      case (q1Mat, q2Mat) =>
        q1Mat -> q2Mat
    } { implicit b => (s1, s2) =>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val merge = b.add(MergePreferred[Long => LedgerEntry](1))

      s1 ~> merge.preferred
      s2 ~> merge.in(0)

      SourceShape(merge.out)
    })

    mergedSources
      .mapAsync(1) { ledgerEntryGen => //strictly one after another!
        val offset = headRef // we can only do this because there is not parallelism here!
        val ledgerEntry = ledgerEntryGen(offset)
        val newLedgerEnd = nextOffset(offset)
        ledgerDao
          .storeLedgerEntry(offset, newLedgerEnd, ledgerEntry)
          .map {
            case Ok =>
              headRef = newLedgerEnd //updating the headRef
              dispatcher.signalNewHead(newLedgerEnd) //signalling downstream subscriptions
            case Duplicate =>
              () //we are staying with offset we had
          }(DirectExecutionContext)
      }
      .toMat(Sink.ignore)(
        Keep.left[
          (
              SourceQueueWithComplete[Long => LedgerEntry],
              SourceQueueWithComplete[Long => LedgerEntry]),
          Future[Done]])
      .run()
  }

  override def close(): Unit = {
    persistenceQueue.complete()
    ledgerDao.close()
  }

  private def loadStartingState(ledgerEntries: immutable.Seq[LedgerEntry]): Future[Unit] =
    if (ledgerEntries.nonEmpty) {
      logger.info("initializing ledger with scenario output")
      implicit val ec: ExecutionContext = DirectExecutionContext
      //ledger entries must be persisted via the transactionQueue!
      val fDone = Source(ledgerEntries)
        .mapAsync(1) { ledgerEntry =>
          enqueue(_ => ledgerEntry)
        }
        .runWith(Sink.ignore)

      // Note: the active contract set stored in the SQL database is updated through the insertion of ledger entries.
      // The given active contract set is ignored.
      for {
        _ <- fDone
      } yield ()
    } else Future.successful(())

  override def ledgerEntries(offset: Option[Long]): Source[(Long, LedgerEntry), NotUsed] = {
    //TODO perf optimisation: the Dispatcher queries every ledger entry one by one, which is really not optimal having a relational db. We should change this behaviour and use a simple sql query fetching the ledger entry at once, at least until ledger end.
    dispatcher.startingAt(offset.getOrElse(0))
  }

  override def ledgerEnd: Long = headRef

  override def snapshot(): Future[LedgerSnapshot] =
    //TODO (robert): SQL DAO does not know about ActiveContract, this method does a (trivial) mapping from DAO Contract to Ledger ActiveContract. Intended? The DAO layer was introduced its own Contract abstraction so it can also reason read archived ones if it's needed. In hindsight, this might be necessary at all  so we could probably collapse the two
    ledgerDao.getActiveContractSnapshot
      .map(s => LedgerSnapshot(s.offset, s.acs.map(c => (c.contractId, c.toActiveContract))))(
        DirectExecutionContext)

  override def lookupContract(
      contractId: Value.AbsoluteContractId): Future[Option[ActiveContract]] =
    ledgerDao
      .lookupActiveContract(contractId)
      .map(_.map(c => c.toActiveContract))(DirectExecutionContext)

  override def lookupKey(key: Node.GlobalKey): Future[Option[AbsoluteContractId]] =
    ledgerDao.lookupKey(key)

  override def publishHeartbeat(time: Instant): Future[Unit] =
    checkpointQueue
      .offer(_ => LedgerEntry.Checkpoint(time))
      .map(_ => ())(DirectExecutionContext) //this never pushes back, see createQueues above!

  override def publishTransaction(tx: TransactionSubmission): Future[SubmissionResult] =
    enqueue { offset =>
      val transactionId = offset.toString
      val toAbsCoid: ContractId => AbsoluteContractId =
        SandboxEventIdFormatter.makeAbsCoid(transactionId)

      val mappedTx = tx.transaction
        .mapContractIdAndValue(toAbsCoid, _.mapContractId(toAbsCoid))
        .mapNodeId(SandboxEventIdFormatter.fromTransactionId(transactionId, _))

      val mappedDisclosure = tx.blindingInfo.explicitDisclosure
        .map {
          case (nodeId, party) =>
            SandboxEventIdFormatter.fromTransactionId(transactionId, nodeId) -> party.map(
              _.underlyingString)
        }

      LedgerEntry.Transaction(
        tx.commandId,
        transactionId,
        tx.applicationId,
        tx.submitter,
        tx.workflowId,
        tx.ledgerEffectiveTime,
        timeProvider.getCurrentTime,
        mappedTx,
        mappedDisclosure
      )
    }

  private def enqueue(f: Long => LedgerEntry): Future[SubmissionResult] = {
    persistenceQueue
      .offer(f)
      .transform {
        case Success(Enqueued) =>
          Success(SubmissionResult.Acknowledged)
        case Success(Dropped) =>
          Success(SubmissionResult.Overloaded)
        case Success(QueueClosed) =>
          Failure(new IllegalStateException("queue closed"))
        case Success(QueueOfferResult.Failure(e)) => Failure(e)
        case Failure(f) => Failure(f)
      }(DirectExecutionContext)
  }
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
    * @param startMode       whether we should start with a clean state or continue where we left off
    * @return a compliant Ledger implementation
    */
  def createSqlLedger(
      initialLedgerId: Option[String],
      timeProvider: TimeProvider,
      startMode: SqlStartMode)(implicit mat: Materializer): Future[SqlLedger] = {
    @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
    implicit val ec = DirectExecutionContext

    def init() = startMode match {
      case AlwaysReset =>
        for {
          _ <- reset()
          ledgerId <- initialize(initialLedgerId)
        } yield ledgerId
      case ContinueIfExists => initialize(initialLedgerId)
    }

    for {
      ledgerId <- init()
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
    } yield new SqlLedger(ledgerId, ledgerEnd, ledgerDao, timeProvider)
  }

  private def reset(): Future[Unit] =
    ledgerDao.reset()

  private def initialize(initialLedgerId: Option[String]) = initialLedgerId match {
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
