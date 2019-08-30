// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.time.Instant
import java.util.UUID

import akka.Done
import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.{GraphDSL, Keep, MergePreferred, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, SourceShape}
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails, RejectionReason}
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.sandbox.LedgerIdGenerator
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.services.transaction.SandboxEventIdFormatter
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger.sql.SqlStartMode.{
  AlwaysReset,
  ContinueIfExists
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.dao._
import com.digitalasset.platform.sandbox.stores.ledger.sql.serialisation.{
  ContractSerializer,
  KeyHasher,
  TransactionSerializer,
  ValueSerializer
}
import com.digitalasset.platform.sandbox.stores.ledger.sql.util.DbDispatcher
import com.digitalasset.platform.sandbox.stores.ledger.{Ledger, LedgerEntry}
import com.digitalasset.platform.sandbox.stores.{InMemoryActiveLedgerState, InMemoryPackageStore}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

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
      ledgerId: Option[LedgerId],
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      queueDepth: Int,
      startMode: SqlStartMode = SqlStartMode.ContinueIfExists)(
      implicit mat: Materializer,
      mm: MetricsManager): Future[Ledger] = {
    implicit val ec: ExecutionContext = DEC

    val dbType = JdbcLedgerDao.jdbcType(jdbcUrl)
    val dbDispatcher =
      DbDispatcher(jdbcUrl, dbType, noOfShortLivedConnections, noOfStreamingConnections)
    val ledgerDao = LedgerDao.metered(
      JdbcLedgerDao(
        dbDispatcher,
        ContractSerializer,
        TransactionSerializer,
        ValueSerializer,
        KeyHasher,
        dbType))

    val sqlLedgerFactory = SqlLedgerFactory(ledgerDao)

    sqlLedgerFactory.createSqlLedger(
      ledgerId,
      timeProvider,
      startMode,
      acs,
      packages,
      initialLedgerEntries,
      queueDepth,
      dbType.supportsParallelWrites)
  }
}

private class SqlLedger(
    ledgerId: LedgerId,
    headAtInitialization: Long,
    ledgerDao: LedgerDao,
    timeProvider: TimeProvider,
    packages: InMemoryPackageStore,
    queueDepth: Int,
    parallelLedgerAppend: Boolean)(implicit mat: Materializer)
    extends BaseLedger(ledgerId, headAtInitialization, ledgerDao)
    with Ledger {

  import SqlLedger._

  private val logger = LoggerFactory.getLogger(getClass)

  // the reason for modelling persistence as a reactive pipeline is to avoid having race-conditions between the
  // moving ledger-end, the async persistence operation and the dispatcher head notification
  private val (checkpointQueue, persistenceQueue): (
      SourceQueueWithComplete[Long => PersistenceEntry],
      SourceQueueWithComplete[Long => PersistenceEntry]) = createQueues()

  watchForFailures(checkpointQueue, "checkpoint")
  watchForFailures(persistenceQueue, "persistence")

  private def watchForFailures(queue: SourceQueueWithComplete[_], name: String) =
    queue
      .watchCompletion()
      .onComplete {
        case Failure(t) => logger.error(s"$name queue has been closed with a failure!", t)
        case _ => ()
      }(DEC)

  private def createQueues(): (
      SourceQueueWithComplete[Long => PersistenceEntry],
      SourceQueueWithComplete[Long => PersistenceEntry]) = {

    val checkpointQueue = Source.queue[Long => PersistenceEntry](1, OverflowStrategy.dropHead)
    val persistenceQueue =
      Source.queue[Long => PersistenceEntry](queueDepth, OverflowStrategy.dropNew)

    implicit val ec: ExecutionContext = DEC

    val mergedSources = Source.fromGraph(GraphDSL.create(checkpointQueue, persistenceQueue) {
      case (q1Mat, q2Mat) =>
        q1Mat -> q2Mat
    } { implicit b => (s1, s2) =>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val merge = b.add(MergePreferred[Long => PersistenceEntry](1))

      s1 ~> merge.preferred
      s2 ~> merge.in(0)

      SourceShape(merge.out)
    })

    // By default we process the requests in batches when under pressure (see semantics of `batch`). Note
    // that this is safe on the read end because the readers rely on the dispatchers to know the
    // ledger end, and not the database itself. This means that they will not start reading from the new
    // ledger end until we tell them so, which we do when _all_ the entries have been committed.
    val maxBatchSize = if (parallelLedgerAppend) noOfShortLivedConnections * 2L else 1L
    mergedSources
      .batch(maxBatchSize, e => Queue(e))((batch, e) => batch :+ e)
      .mapAsync(1) { queue =>
        val startOffset = dispatcher.getHead()
        // we can only do this because there is no parallelism here!
        //shooting the SQL queries in parallel
        Future
          .sequence(queue.toIterator.zipWithIndex.map {
            case (ledgerEntryGen, i) =>
              val offset = startOffset + i
              ledgerDao
                .storeLedgerEntry(offset, offset + 1, None, ledgerEntryGen(offset))
                .map(_ => ())(DEC)
                .recover {
                  case t =>
                    //recovering from the failure so the persistence stream doesn't die
                    logger.error(s"Failed to persist entry with offset: $offset", t)
                    ()
                }(DEC)
          })
          .map { _ =>
            //note that we can have holes in offsets in case of the storing of an entry failed for some reason
            dispatcher.signalNewHead(startOffset + queue.length) //signalling downstream subscriptions
          }(DEC)
      }
      .toMat(Sink.ignore)(Keep.left[
        (
            SourceQueueWithComplete[Long => PersistenceEntry],
            SourceQueueWithComplete[Long => PersistenceEntry]),
        Future[Done]])
      .run()
  }

  override def close(): Unit = {
    super.close()
    persistenceQueue.complete()
    checkpointQueue.complete()
  }

  override def publishHeartbeat(time: Instant): Future[Unit] =
    checkpointQueue
      .offer(_ => PersistenceEntry.Checkpoint(LedgerEntry.Checkpoint(time)))
      .map(_ => ())(DEC) //this never pushes back, see createQueues above!

  override def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Future[SubmissionResult] =
    enqueue { offset =>
      val transactionId = Ref.LedgerString.fromLong(offset)
      val toAbsCoid: ContractId => AbsoluteContractId =
        SandboxEventIdFormatter.makeAbsCoid(transactionId)

      val mappedTx = transaction
        .mapContractIdAndValue(toAbsCoid, _.mapContractId(toAbsCoid))
        .mapNodeId(SandboxEventIdFormatter.fromTransactionId(transactionId, _))

      val blindingInfo = Blinding.blind(transaction)

      val mappedDisclosure = blindingInfo.explicitDisclosure
        .map {
          case (nodeId, parties) =>
            SandboxEventIdFormatter.fromTransactionId(transactionId, nodeId) -> parties
        }

      val mappedLocalImplicitDisclosure = blindingInfo.localImplicitDisclosure.map {
        case (k, v) => SandboxEventIdFormatter.fromTransactionId(transactionId, k) -> v
      }

      val recordTime = timeProvider.getCurrentTime
      if (recordTime.isAfter(submitterInfo.maxRecordTime.toInstant)) {
        // This can happen if the DAML-LF computation (i.e. exercise of a choice) takes longer
        // than the time window between LET and MRT allows for.
        // See https://github.com/digital-asset/daml/issues/987
        PersistenceEntry.Rejection(
          LedgerEntry.Rejection(
            recordTime,
            submitterInfo.commandId,
            submitterInfo.applicationId,
            submitterInfo.submitter,
            RejectionReason.TimedOut(
              s"RecordTime $recordTime is after MaximumRecordTime ${submitterInfo.maxRecordTime}")
          )
        )
      } else {
        PersistenceEntry.Transaction(
          LedgerEntry.Transaction(
            Some(submitterInfo.commandId),
            transactionId,
            Some(submitterInfo.applicationId),
            Some(submitterInfo.submitter),
            transactionMeta.workflowId,
            transactionMeta.ledgerEffectiveTime.toInstant,
            recordTime,
            mappedTx,
            mappedDisclosure
          ),
          mappedLocalImplicitDisclosure,
          blindingInfo.globalImplicitDisclosure
        )
      }
    }

  private def enqueue(f: Long => PersistenceEntry): Future[SubmissionResult] =
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

  override def allocateParty(
      party: Party,
      displayName: Option[String]): Future[PartyAllocationResult] =
    ledgerDao
      .storeParty(party, displayName, None)
      .map {
        case PersistenceResponse.Ok =>
          PartyAllocationResult.Ok(PartyDetails(party, displayName, true))
        case PersistenceResponse.Duplicate =>
          PartyAllocationResult.AlreadyExists
      }(DEC)

  override def uploadPackages(
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]): Future[UploadPackagesResult] = {
    val submissionId = UUID.randomUUID().toString
    val packages = payload.map(archive =>
      (archive, PackageDetails(archive.getPayload.size().toLong, knownSince, sourceDescription)))
    ledgerDao
      .uploadLfPackages(submissionId, packages, None)
      .map { result =>
        result.get(PersistenceResponse.Ok).fold(logger.info(s"No package uploaded")) { uploaded =>
          logger.info(s"Successfully uploaded $uploaded packages")
        }
        for (duplicates <- result.get(PersistenceResponse.Duplicate)) {
          logger.info(s"$duplicates packages discarded as duplicates")
        }
        // Unlike the data access layer, the API has no concept of duplicates, so we
        // discard the information; package upload is idempotent, apart from the fact
        // that we only keep the knownSince and sourceDescription of the first upload.
        UploadPackagesResult.Ok
      }(DEC)
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
    * @param initialLedgerEntries The initial ledger entries -- usually provided by the scenario runner. Will only be
    *                             used if starting from a fresh database.
    * @param queueDepth      the depth of the buffer for persisting entries. When gets full, the system will signal back-pressure
    *                        upstream
    * @param parallelLedgerAppend whether to append to the ledger in parallelized batches
    * @return a compliant Ledger implementation
    */
  def createSqlLedger(
      initialLedgerId: Option[LedgerId],
      timeProvider: TimeProvider,
      startMode: SqlStartMode,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      queueDepth: Int,
      parallelLedgerAppend: Boolean
  )(implicit mat: Materializer): Future[SqlLedger] = {
    @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
    implicit val ec = DEC

    def init(): Future[LedgerId] = startMode match {
      case AlwaysReset =>
        for {
          _ <- reset()
          ledgerId <- initialize(initialLedgerId, timeProvider, acs, packages, initialLedgerEntries)
        } yield ledgerId
      case ContinueIfExists =>
        initialize(initialLedgerId, timeProvider, acs, packages, initialLedgerEntries)
    }

    for {
      ledgerId <- init()
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
    } yield
      new SqlLedger(
        ledgerId,
        ledgerEnd,
        ledgerDao,
        timeProvider,
        packages,
        queueDepth,
        parallelLedgerAppend)
  }

  private def reset(): Future[Unit] =
    ledgerDao.reset()

  private def initialize(
      initialLedgerId: Option[LedgerId],
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump]): Future[LedgerId] = {
    // Note that here we only store the ledger entry and we do not update anything else, such as the
    // headRef. We also are not concerns with heartbeats / checkpoints. This is OK since this initialization
    // step happens before we start up the sql ledger at all, so it's running in isolation.

    initialLedgerId match {
      case Some(initialId) =>
        ledgerDao
          .lookupLedgerId()
          .flatMap {
            case Some(foundLedgerId) if (foundLedgerId == initialId) =>
              if (initialLedgerEntries.nonEmpty) {
                logger.warn(
                  s"Initial ledger entries provided, presumably from scenario, but I'm picking up from an existing database, and thus they will not be used")
              }
              if (packages.listLfPackagesSync().nonEmpty) {
                logger.warn(
                  s"Initial packages provided, presumably as command line arguments, but I'm picking up from an existing database, and thus they will not be used")
              }
              ledgerFound(foundLedgerId)
            case Some(foundLedgerId) =>
              val errorMsg =
                s"Ledger id mismatch. Ledger id given ('$initialId') is not equal to the existing one ('$foundLedgerId')!"
              logger.error(errorMsg)
              sys.error(errorMsg)
            case None =>
              if (initialLedgerEntries.nonEmpty) {
                logger.info(
                  s"Initializing ledger with ${initialLedgerEntries.length} ledger entries")
              }

              val contracts = acs.contracts.values
                .map(f => Contract.fromActiveContract(f))
                .toList

              val initialLedgerEnd = 0L
              val entriesWithOffset = initialLedgerEntries.foldLeft(
                (initialLedgerEnd, immutable.Seq.empty[(Long, LedgerEntry)]))((acc, le) => {
                val offset = acc._1
                val seq = acc._2
                le match {
                  case LedgerEntryOrBump.Entry(entry) =>
                    (offset + 1, seq :+ offset -> entry)
                  case LedgerEntryOrBump.Bump(increment) =>
                    (offset + increment, seq)
                }
              })

              @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
              implicit val ec = DEC
              for {
                _ <- doInit(initialId)
                _ <- copyPackages(packages, timeProvider.getCurrentTime)
                _ <- ledgerDao.storeInitialState(
                  contracts,
                  entriesWithOffset._2,
                  entriesWithOffset._1)
              } yield { initialId }

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
  }

  private def ledgerFound(foundLedgerId: LedgerId) = {
    logger.info(s"Found existing ledger with id: ${foundLedgerId.unwrap}")
    Future.successful(foundLedgerId)
  }

  private def doInit(ledgerId: LedgerId): Future[Unit] = {
    logger.info(s"Initializing ledger with id: ${ledgerId.unwrap}")
    ledgerDao.initializeLedger(ledgerId, 0)
  }

  private def copyPackages(store: InMemoryPackageStore, knownSince: Instant): Future[Unit] = {

    val packageDetails = store.listLfPackagesSync()
    if (packageDetails.nonEmpty) {
      logger.info(s"Copying initial packages ${packageDetails.keys.mkString(",")}")
      val submissionId = UUID.randomUUID().toString
      val packages = packageDetails.toList.map(pkg => {
        val archive =
          store.getLfArchiveSync(pkg._1).getOrElse(sys.error(s"Package ${pkg._1} not found"))
        archive -> PackageDetails(archive.getPayload.size.toLong, knownSince, None)
      })
      ledgerDao
        .uploadLfPackages(submissionId, packages, None)
        .transform(_ => (), e => sys.error("Failed to copy initial packages: " + e.getMessage))(DEC)
    } else {
      Future.successful(())
    }
  }

}

private object SqlLedgerFactory {
  def apply(ledgerDao: LedgerDao): SqlLedgerFactory = new SqlLedgerFactory(ledgerDao)
}
