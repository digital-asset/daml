// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.time.Instant

import akka.Done
import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.{GraphDSL, Keep, MergePreferred, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, SourceShape}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Blinding
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractId}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.dec.{DirectExecutionContext => DEC}
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails, RejectionReason}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.events.EventIdFormatter
import com.digitalasset.platform.packages.InMemoryPackageStore
import com.digitalasset.platform.sandbox.LedgerIdGenerator
import com.digitalasset.platform.sandbox.stores.InMemoryActiveLedgerState
import com.digitalasset.platform.sandbox.stores.ledger.Ledger
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.store.dao.JdbcLedgerDao.defaultNumberOfShortLivedConnections
import com.digitalasset.platform.store.dao.{
  DbDispatcher,
  JdbcLedgerDao,
  LedgerDao,
  MeteredLedgerDao
}
import com.digitalasset.platform.store.entries.{LedgerEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.digitalasset.platform.store.{BaseLedger, DbType, FlywayMigrations, PersistenceEntry}
import com.digitalasset.resources.ResourceOwner
import scalaz.syntax.tag._

import scala.collection.immutable
import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SqlLedger {

  private case class Offsets(offset: Long, nextOffset: Long)

  private type Queues = (
      SourceQueueWithComplete[Offsets => Future[Unit]],
      SourceQueueWithComplete[Offsets => Future[Unit]],
  )

  //jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
  def owner(
      jdbcUrl: String,
      ledgerId: Option[LedgerId],
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      queueDepth: Int,
      startMode: SqlStartMode = SqlStartMode.ContinueIfExists,
      metrics: MetricRegistry,
  )(implicit mat: Materializer, logCtx: LoggingContext): ResourceOwner[Ledger] = {
    implicit val ec: ExecutionContext = DEC

    new FlywayMigrations(jdbcUrl).migrate()

    val dbType = DbType.jdbcType(jdbcUrl)
    val maxConnections =
      if (dbType.supportsParallelWrites) defaultNumberOfShortLivedConnections else 1
    for {
      dbDispatcher <- DbDispatcher.owner(jdbcUrl, maxConnections, metrics)
      ledgerDao = new MeteredLedgerDao(
        JdbcLedgerDao(dbDispatcher, dbType, mat.executionContext),
        metrics,
      )
      ledger <- ResourceOwner
        .forFutureCloseable(
          () =>
            new SqlLedgerFactory(ledgerDao).createSqlLedger(
              ledgerId,
              participantId,
              timeProvider,
              startMode,
              acs,
              packages,
              initialLedgerEntries,
              queueDepth,
              // we use `maxConnections` for the maximum batch size, since it doesn't make sense to try to
              // persist more ledger entries concurrently than we have SQL executor threads and SQL
              // connections available.
              maxConnections,
          ))
    } yield ledger
  }
}

private final class SqlLedger(
    ledgerId: LedgerId,
    participantId: ParticipantId,
    headAtInitialization: Long,
    ledgerDao: LedgerDao,
    timeProvider: TimeProvider,
    packages: InMemoryPackageStore,
    queueDepth: Int,
    maxBatchSize: Int,
)(implicit mat: Materializer, logCtx: LoggingContext)
    extends BaseLedger(ledgerId, headAtInitialization, ledgerDao)
    with Ledger {

  import SqlLedger._

  private val logger = ContextualizedLogger.get(this.getClass)

  // the reason for modelling persistence as a reactive pipeline is to avoid having race-conditions between the
  // moving ledger-end, the async persistence operation and the dispatcher head notification
  private val (checkpointQueue, persistenceQueue): Queues = createQueues()

  watchForFailures(checkpointQueue, "checkpoint")
  watchForFailures(persistenceQueue, "persistence")

  private def watchForFailures(queue: SourceQueueWithComplete[_], name: String): Unit =
    queue
      .watchCompletion()
      .failed
      .foreach { throwable =>
        logger.error(s"$name queue has been closed with a failure!", throwable)
      }(DEC)

  private def createQueues(): Queues = {
    implicit val ec: ExecutionContext = DEC

    val checkpointQueue = Source.queue[Offsets => Future[Unit]](1, OverflowStrategy.dropHead)
    val persistenceQueue =
      Source.queue[Offsets => Future[Unit]](queueDepth, OverflowStrategy.dropNew)

    val mergedSources =
      Source.fromGraph(GraphDSL.create(checkpointQueue, persistenceQueue)(_ -> _) {
        implicit b => (checkpointSource, persistenceSource) =>
          import akka.stream.scaladsl.GraphDSL.Implicits._
          val merge = b.add(MergePreferred[Offsets => Future[Unit]](1))

          checkpointSource ~> merge.preferred
          persistenceSource ~> merge.in(0)

          SourceShape(merge.out)
      })

    // By default we process the requests in batches when under pressure (see semantics of `batch`). Note
    // that this is safe on the read end because the readers rely on the dispatchers to know the
    // ledger end, and not the database itself. This means that they will not start reading from the new
    // ledger end until we tell them so, which we do when _all_ the entries have been committed.
    mergedSources
      .batch(maxBatchSize.toLong, Queue(_))(_.enqueue(_))
      .mapAsync(1) { queue =>
        val startOffset = dispatcher.getHead()
        // we can only do this because there is no parallelism here!
        //shooting the SQL queries in parallel
        Future
          .sequence(queue.toIterator.zipWithIndex.map {
            case (persist, i) =>
              val offset = startOffset + i
              persist(Offsets(offset, offset + 1L))
          })
          .map { _ =>
            //note that we can have holes in offsets in case of the storing of an entry failed for some reason
            dispatcher.signalNewHead(startOffset + queue.length) //signalling downstream subscriptions
          }
      }
      .toMat(Sink.ignore)(Keep.left[Queues, Future[Done]])
      .run()
  }

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def close(): Unit = {
    super.close()
    persistenceQueue.complete()
    checkpointQueue.complete()
  }

  private def storeLedgerEntry(offsets: Offsets, entry: PersistenceEntry): Future[Unit] =
    ledgerDao
      .storeLedgerEntry(offsets.offset, offsets.nextOffset, None, entry)
      .map(_ => ())(DEC)
      .recover {
        case t =>
          //recovering from the failure so the persistence stream doesn't die
          logger.error(s"Failed to persist entry with offsets: $offsets", t)
          ()
      }(DEC)

  override def publishHeartbeat(time: Instant): Future[Unit] =
    checkpointQueue
      .offer(offsets =>
        storeLedgerEntry(offsets, PersistenceEntry.Checkpoint(LedgerEntry.Checkpoint(time))))
      .map(_ => ())(DEC) //this never pushes back, see createQueues above!

  override def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Future[SubmissionResult] =
    enqueue { offsets =>
      val transactionId = Ref.LedgerString.fromLong(offsets.offset)
      val toAbsCoid: ContractId => AbsoluteContractId =
        EventIdFormatter.makeAbsCoid(transactionId)

      val mappedTx = transaction
        .resolveRelCid(EventIdFormatter.makeAbs(transactionId))
        .mapNodeId(EventIdFormatter.fromTransactionId(transactionId, _))

      val blindingInfo = Blinding.blind(transaction)

      val mappedDisclosure = blindingInfo.disclosure
        .map {
          case (nodeId, parties) =>
            EventIdFormatter.fromTransactionId(transactionId, nodeId) -> parties
        }

      val mappedLocalDivulgence = blindingInfo.localDivulgence.map {
        case (k, v) => EventIdFormatter.fromTransactionId(transactionId, k) -> v
      }

      val recordTime = timeProvider.getCurrentTime
      val entry = if (recordTime.isAfter(submitterInfo.maxRecordTime.toInstant)) {
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
          mappedLocalDivulgence,
          blindingInfo.globalDivulgence,
          List.empty
        )
      }

      storeLedgerEntry(offsets, entry)

    }

  private def enqueue(persist: Offsets => Future[Unit]): Future[SubmissionResult] =
    persistenceQueue
      .offer(persist)
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

  override def publishPartyAllocation(
      submissionId: SubmissionId,
      party: Party,
      displayName: Option[String]): Future[SubmissionResult] = {
    enqueue { offsets =>
      ledgerDao
        .storePartyEntry(
          offsets.offset,
          offsets.nextOffset,
          None,
          PartyLedgerEntry.AllocationAccepted(
            Some(submissionId),
            participantId,
            timeProvider.getCurrentTime,
            PartyDetails(party, displayName, isLocal = true))
        )
        .map(_ => ())(DEC)
        .recover {
          case t =>
            //recovering from the failure so the persistence stream doesn't die
            logger.error(s"Failed to persist party $party with offsets: $offsets", t)
            ()
        }(DEC)
    }
  }

  override def uploadPackages(
      submissionId: SubmissionId,
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]): Future[SubmissionResult] = {
    val packages = payload.map(archive =>
      (archive, PackageDetails(archive.getPayload.size().toLong, knownSince, sourceDescription)))
    enqueue { offsets =>
      ledgerDao
        .storePackageEntry(
          offsets.offset,
          offsets.nextOffset,
          None,
          packages,
          Some(PackageLedgerEntry.PackageUploadAccepted(submissionId, timeProvider.getCurrentTime)),
        )
        .map(_ => ())(DEC)
        .recover {
          case t =>
            //recovering from the failure so the persistence stream doesn't die
            logger.error(s"Failed to persist packages with offsets: $offsets", t)
            ()
        }(DEC)
    }
  }

  override def publishConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: String,
      config: Configuration): Future[SubmissionResult] =
    enqueue { offsets =>
      val recordTime = timeProvider.getCurrentTime
      val mrt = maxRecordTime.toInstant

      val storeF =
        if (recordTime.isAfter(mrt)) {
          ledgerDao
            .storeConfigurationEntry(
              offsets.offset,
              offsets.nextOffset,
              None,
              recordTime,
              submissionId,
              participantId,
              config,
              Some(s"Configuration change timed out: $mrt > $recordTime"),
            )
        } else {
          // NOTE(JM): If the generation in the new configuration is invalid
          // we persist a rejection. This is done inside storeConfigurationEntry
          // as we need to check against the current configuration within the same
          // database transaction.
          ledgerDao
            .storeConfigurationEntry(
              offsets.offset,
              offsets.nextOffset,
              None,
              recordTime,
              submissionId,
              participantId,
              config,
              None
            )
        }

      storeF
        .map(_ => ())(DEC)
        .recover {
          case t =>
            //recovering from the failure so the persistence stream doesn't die
            logger.error(s"Failed to persist configuration with offsets: $offsets", t)
            ()
        }(DEC)
    }
}

private final class SqlLedgerFactory(ledgerDao: LedgerDao)(implicit logCtx: LoggingContext) {

  private val logger = ContextualizedLogger.get(this.getClass)

  /** *
    * Creates a DB backed Ledger implementation.
    *
    * @param initialLedgerId a random ledger id is generated if none given, if set it's used to initialize the ledger.
    *                        In case the ledger had already been initialized, the given ledger id must not be set or must
    *                        be equal to the one in the database.
    * @param participantId   the participant identifier
    * @param timeProvider    to get the current time when sequencing transactions
    * @param startMode       whether we should start with a clean state or continue where we left off
    * @param initialLedgerEntries The initial ledger entries -- usually provided by the scenario runner. Will only be
    *                             used if starting from a fresh database.
    * @param queueDepth      the depth of the buffer for persisting entries. When gets full, the system will signal back-pressure
    *                        upstream
    * @param maxBatchSize maximum size of ledger entry batches to be persisted
    * @return a compliant Ledger implementation
    */
  def createSqlLedger(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      startMode: SqlStartMode,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      queueDepth: Int,
      maxBatchSize: Int,
  )(implicit mat: Materializer): Future[SqlLedger] = {
    implicit val ec: ExecutionContext = DEC

    def init(): Future[LedgerId] = startMode match {
      case SqlStartMode.AlwaysReset =>
        for {
          _ <- reset()
          ledgerId <- initialize(initialLedgerId, timeProvider, acs, packages, initialLedgerEntries)
        } yield ledgerId
      case SqlStartMode.ContinueIfExists =>
        initialize(initialLedgerId, timeProvider, acs, packages, initialLedgerEntries)
    }

    for {
      ledgerId <- init()
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
    } yield
      new SqlLedger(
        ledgerId,
        participantId,
        ledgerEnd,
        ledgerDao,
        timeProvider,
        packages,
        queueDepth,
        maxBatchSize,
      )
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
            case Some(foundLedgerId) if foundLedgerId == initialId =>
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

              val contracts = acs.activeContracts.values.toList

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

              implicit val ec: ExecutionContext = DEC
              for {
                _ <- doInit(initialId)
                _ <- copyPackages(packages, timeProvider.getCurrentTime, entriesWithOffset._1)
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

  private def copyPackages(
      store: InMemoryPackageStore,
      knownSince: Instant,
      newLedgerEnd: Long): Future[Unit] = {

    val packageDetails = store.listLfPackagesSync()
    if (packageDetails.nonEmpty) {
      logger.info(s"Copying initial packages ${packageDetails.keys.mkString(",")}")
      val packages = packageDetails.toList.map(pkg => {
        val archive =
          store.getLfArchiveSync(pkg._1).getOrElse(sys.error(s"Package ${pkg._1} not found"))
        archive -> PackageDetails(archive.getPayload.size.toLong, knownSince, None)
      })

      ledgerDao
        .storePackageEntry(
          newLedgerEnd, // FIXME(JM): Find a more reasonable way to do this.
          newLedgerEnd,
          None,
          packages,
          None)
        .transform(_ => (), e => sys.error("Failed to copy initial packages: " + e.getMessage))(DEC)
    } else {
      Future.successful(())
    }
  }

}
