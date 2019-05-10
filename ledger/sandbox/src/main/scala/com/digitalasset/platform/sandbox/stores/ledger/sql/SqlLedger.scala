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
import com.digitalasset.ledger.backend.api.v1.{
  RejectionReason,
  SubmissionResult,
  TransactionId,
  TransactionSubmission
}
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.sandbox.config.LedgerIdGenerator
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter
import com.digitalasset.platform.sandbox.stores.ActiveContracts.ActiveContract
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlStartMode.{
  AlwaysReset,
  ContinueIfExists
}
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
import scala.collection.immutable.Queue
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

  val noOfShortLivedConnections = 16
  val noOfStreamingConnections = 2

  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  def apply(
      jdbcUrl: String,
      ledgerId: Option[String],
      timeProvider: TimeProvider,
      ledgerEntries: immutable.Seq[LedgerEntry],
      queueDepth: Int,
      startMode: SqlStartMode = SqlStartMode.ContinueIfExists)(
      implicit mat: Materializer,
      mm: MetricsManager): Future[Ledger] = {
    implicit val ec: ExecutionContext = DEC

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
      sqlLedger <- sqlLedgerFactory.createSqlLedger(ledgerId, timeProvider, startMode, queueDepth)
      _ <- sqlLedger.loadStartingState(ledgerEntries)
    } yield sqlLedger
  }
}

private class SqlLedger(
    val ledgerId: String,
    headAtInitialization: Long,
    ledgerDao: LedgerDao,
    timeProvider: TimeProvider,
    queueDepth: Int)(implicit mat: Materializer)
    extends Ledger {

  import SqlLedger._

  private val logger = LoggerFactory.getLogger(getClass)

  private val dispatcher = Dispatcher[Long, LedgerEntry](
    RangeSource(ledgerDao.getLedgerEntries(_, _)),
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
    val persistenceQueue = Source.queue[Long => LedgerEntry](queueDepth, OverflowStrategy.dropNew)

    implicit val ec: ExecutionContext = DEC

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

    // We process the requests in batches when under pressure (see semantics of `batch`). Note
    // that this is safe on the read end because the readers rely on the dispatchers to know the
    // ledger end, and not the database itself. This means that they will not start reading from the new
    // ledger end until we tell them so, which we do when _all_ the entries have been committed.
    mergedSources
      .batch(noOfShortLivedConnections * 2L, e => Queue(e))((batch, e) => batch :+ e)
      .mapAsync(1) { queue =>
        val startOffset = headRef // we can only do this because there is no parallelism here!
        //shooting the SQL queries in parallel
        Future
          .sequence(queue.toIterator.zipWithIndex.map {
            case (ledgerEntryGen, i) =>
              val offset = startOffset + i
              ledgerDao
                .storeLedgerEntry(offset, offset + 1, ledgerEntryGen(offset))
                .map(_ => ())(DEC)
          })
          .map { _ =>
            //note that we can have holes in offsets in case of the storing of an entry failed for some reason
            headRef = startOffset + queue.length //updating the headRef
            dispatcher.signalNewHead(headRef) //signalling downstream subscriptions
          }(DEC)
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
      implicit val ec: ExecutionContext = DEC
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
    dispatcher.startingAt(offset.getOrElse(0))
  }

  override def ledgerEnd: Long = headRef

  override def snapshot(): Future[LedgerSnapshot] =
    //TODO (robert): SQL DAO does not know about ActiveContract, this method does a (trivial) mapping from DAO Contract to Ledger ActiveContract. Intended? The DAO layer was introduced its own Contract abstraction so it can also reason read archived ones if it's needed. In hindsight, this might be necessary at all  so we could probably collapse the two
    ledgerDao.getActiveContractSnapshot
      .map(s => LedgerSnapshot(s.offset, s.acs.map(c => (c.contractId, c.toActiveContract))))(DEC)

  override def lookupContract(
      contractId: Value.AbsoluteContractId): Future[Option[ActiveContract]] =
    ledgerDao
      .lookupActiveContract(contractId)
      .map(_.map(c => c.toActiveContract))(DEC)

  override def lookupKey(key: Node.GlobalKey): Future[Option[AbsoluteContractId]] =
    ledgerDao.lookupKey(key)

  override def publishHeartbeat(time: Instant): Future[Unit] =
    checkpointQueue
      .offer(_ => LedgerEntry.Checkpoint(time))
      .map(_ => ())(DEC) //this never pushes back, see createQueues above!

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
          case (nodeId, parties) =>
            SandboxEventIdFormatter.fromTransactionId(transactionId, nodeId) ->
              parties.toSet[String]
        }

      val recordTime = timeProvider.getCurrentTime
      if (recordTime.isAfter(tx.maximumRecordTime)) {
        // This can happen if the DAML-LF computation (i.e. exercise of a choice) takes longer
        // than the time window between LET and MRT allows for.
        // See https://github.com/digital-asset/daml/issues/987
        LedgerEntry.Rejection(
          recordTime,
          tx.commandId,
          tx.applicationId,
          tx.submitter,
          RejectionReason.TimedOut(
            s"RecordTime $recordTime is after MaximumRecordTime ${tx.maximumRecordTime}")
        )
      } else {
        LedgerEntry.Transaction(
          tx.commandId,
          transactionId,
          tx.applicationId,
          tx.submitter,
          tx.workflowId,
          tx.ledgerEffectiveTime,
          recordTime,
          mappedTx,
          mappedDisclosure
        )
      }
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
      }(DEC)
  }

  override def lookupTransaction(
      transactionId: TransactionId): Future[Option[(Long, LedgerEntry.Transaction)]] =
    ledgerDao
      .lookupLedgerEntry(transactionId.toLong)
      .map(_.collect[(Long, LedgerEntry.Transaction)] {
        case t: LedgerEntry.Transaction =>
          (transactionId.toLong, t) // the transaction is also the offset
      })(DEC)

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
    * @param queueDepth      the depth of the buffer for persisting entries. When gets full, the system will signal back-pressure
    *                        upstream
    * @return a compliant Ledger implementation
    */
  def createSqlLedger(
      initialLedgerId: Option[String],
      timeProvider: TimeProvider,
      startMode: SqlStartMode,
      queueDepth: Int)(implicit mat: Materializer): Future[SqlLedger] = {
    @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
    implicit val ec = DEC

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
    } yield new SqlLedger(ledgerId, ledgerEnd, ledgerDao, timeProvider, queueDepth)
  }

  private def reset(): Future[Unit] =
    ledgerDao.reset()

  private def initialize(initialLedgerId: Option[String]): Future[String] = initialLedgerId match {
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
            doInit(initialId).map(_ => initialId)(DEC)
        }(DEC)

    case None =>
      logger.info("No ledger id given. Looking for existing ledger in database.")
      ledgerDao
        .lookupLedgerId()
        .flatMap {
          case Some(foundLedgerId) => ledgerFound(foundLedgerId)
          case None =>
            val randomLedgerId = LedgerIdGenerator.generateRandomId()
            doInit(randomLedgerId).map(_ => randomLedgerId)(DEC)
        }(DEC)
  }

  private def ledgerFound(foundLedgerId: String) = {
    logger.info(s"Found existing ledger with id: $foundLedgerId")
    Future.successful(foundLedgerId)
  }

  private def doInit(ledgerId: String): Future[Unit] = {
    logger.info(s"Initializing ledger with id: $ledgerId")
    ledgerDao.initializeLedger(ledgerId, 0)
  }

}

private object SqlLedgerFactory {
  def apply(ledgerDao: LedgerDao): SqlLedgerFactory = new SqlLedgerFactory(ledgerDao)
}
