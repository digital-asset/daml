// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import akka.Done
import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.GraphDSL.Implicits._
import akka.stream.scaladsl.{GraphDSL, Keep, MergePreferred, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult, SourceShape}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.dec.{DirectExecutionContext => DEC}
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails, RejectionReason}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.ApiOffset.ApiOffsetConverter
import com.digitalasset.platform.common.{LedgerIdMismatchException, LedgerIdMode}
import com.digitalasset.platform.configuration.ServerRole
import com.digitalasset.platform.packages.InMemoryPackageStore
import com.digitalasset.platform.sandbox.LedgerIdGenerator
import com.digitalasset.platform.sandbox.stores.InMemoryActiveLedgerState
import com.digitalasset.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.digitalasset.platform.sandbox.stores.ledger.{Ledger, SandboxOffset}
import com.digitalasset.platform.store.dao.{JdbcLedgerDao, LedgerDao}
import com.digitalasset.platform.store.entries.{LedgerEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.digitalasset.platform.store.{BaseLedger, FlywayMigrations, PersistenceEntry}
import com.digitalasset.resources.ProgramResource.StartupException
import com.digitalasset.resources.ResourceOwner

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object SqlLedger {

  private type Queues = (
      SourceQueueWithComplete[Offset => Future[Unit]],
      SourceQueueWithComplete[Offset => Future[Unit]],
  )

  def owner(
      serverRole: ServerRole,
      // jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
      jdbcUrl: String,
      ledgerId: LedgerIdMode,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      initialConfig: Configuration,
      queueDepth: Int,
      startMode: SqlStartMode = SqlStartMode.ContinueIfExists,
      metrics: MetricRegistry,
  )(implicit mat: Materializer, logCtx: LoggingContext): ResourceOwner[Ledger] =
    for {
      _ <- ResourceOwner.forFuture(() => new FlywayMigrations(jdbcUrl).migrate()(DEC))
      ledgerDao <- JdbcLedgerDao.writeOwner(serverRole, jdbcUrl, metrics)
      ledger <- ResourceOwner.forFutureCloseable(
        () =>
          new SqlLedgerFactory(ledgerDao).createSqlLedger(
            ledgerId,
            participantId,
            timeProvider,
            startMode,
            acs,
            packages,
            initialLedgerEntries,
            initialConfig,
            queueDepth,
            // we use `maxConcurrentConnections` for the maximum batch size, since it doesn't make
            // sense to try to persist more ledger entries concurrently than we have SQL executor
            // threads and SQL connections available.
            ledgerDao.maxConcurrentConnections,
        ))
    } yield ledger
}

private final class SqlLedger(
    ledgerId: LedgerId,
    participantId: ParticipantId,
    headAtInitialization: Offset,
    configAtInitialization: Option[Configuration],
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

    val checkpointQueue = Source.queue[Offset => Future[Unit]](1, OverflowStrategy.dropHead)
    val persistenceQueue =
      Source.queue[Offset => Future[Unit]](queueDepth, OverflowStrategy.dropNew)

    val mergedSources =
      Source.fromGraph(GraphDSL.create(checkpointQueue, persistenceQueue)(_ -> _) {
        implicit b => (checkpointSource, persistenceSource) =>
          val merge = b.add(MergePreferred[Offset => Future[Unit]](1))

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
        val startOffset = SandboxOffset.fromOffset(dispatcher.getHead())
        // we can only do this because there is no parallelism here!
        //shooting the SQL queries in parallel
        Future
          .sequence(queue.toIterator.zipWithIndex.map {
            case (persist, i) =>
              val offset = startOffset + i + 1
              persist(SandboxOffset.toOffset(offset))
          })
          .map { _ =>
            //note that we can have holes in offsets in case of the storing of an entry failed for some reason
            dispatcher.signalNewHead(SandboxOffset.toOffset(startOffset + queue.length)) //signalling downstream subscriptions
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

  // Note: ledger entries are written in batches, and this variable is updated while writing a ledger configuration
  // changed entry. Transactions written around the same time as a configuration change entry might not use the correct
  // time model.
  private[this] val currentConfiguration =
    new AtomicReference[Option[Configuration]](configAtInitialization)

  // Validates the given ledger time according to the ledger time model
  private def checkTimeModel(ledgerTime: Instant): Either[String, Unit] = {
    val recordTime = timeProvider.getCurrentTime

    currentConfiguration
      .get()
      .fold[Either[String, Unit]](
        Left("No ledger configuration available, can not validate ledger time")
      )(
        config => config.timeModel.checkTime(ledgerTime, recordTime)
      )
  }

  private def storeLedgerEntry(offset: Offset, entry: PersistenceEntry): Future[Unit] =
    ledgerDao
      .storeLedgerEntry(offset, entry)
      .map(_ => ())(DEC)
      .recover {
        case t =>
          //recovering from the failure so the persistence stream doesn't die
          logger.error(s"Failed to persist entry with offset: ${offset.toApiString}", t)
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
    enqueue { offset =>
      val transactionId = offset.toApiString

      val (transactionForIndex, disclosureForIndex, globalDivulgence) =
        Ledger.convertToCommittedTransaction(transactionId, transaction)

      val ledgerTime = transactionMeta.ledgerEffectiveTime.toInstant
      val recordTime = timeProvider.getCurrentTime
      val entry = checkTimeModel(ledgerTime)
        .fold(
          reason =>
            PersistenceEntry.Rejection(
              LedgerEntry.Rejection(
                recordTime,
                submitterInfo.commandId,
                submitterInfo.applicationId,
                submitterInfo.submitter,
                RejectionReason.InvalidLedgerTime(reason)
              )
          ),
          _ =>
            PersistenceEntry.Transaction(
              LedgerEntry.Transaction(
                Some(submitterInfo.commandId),
                transactionId,
                Some(submitterInfo.applicationId),
                Some(submitterInfo.submitter),
                transactionMeta.workflowId,
                transactionMeta.ledgerEffectiveTime.toInstant,
                recordTime,
                transactionForIndex,
                disclosureForIndex
              ),
              globalDivulgence,
              List.empty
          )
        )

      storeLedgerEntry(offset, entry)

    }

  private def enqueue(persist: Offset => Future[Unit]): Future[SubmissionResult] =
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
    enqueue { offset =>
      ledgerDao
        .storePartyEntry(
          offset,
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
            logger.error(s"Failed to persist party $party with offset: ${offset.toApiString}", t)
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
    enqueue { offset =>
      ledgerDao
        .storePackageEntry(
          offset,
          packages,
          Some(PackageLedgerEntry.PackageUploadAccepted(submissionId, timeProvider.getCurrentTime)),
        )
        .map(_ => ())(DEC)
        .recover {
          case t =>
            //recovering from the failure so the persistence stream doesn't die
            logger.error(s"Failed to persist packages with offset: ${offset.toApiString}", t)
            ()
        }(DEC)
    }
  }

  override def publishConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: String,
      config: Configuration): Future[SubmissionResult] =
    enqueue { offset =>
      val recordTime = timeProvider.getCurrentTime
      val mrt = maxRecordTime.toInstant

      val storeF =
        if (recordTime.isAfter(mrt)) {
          ledgerDao
            .storeConfigurationEntry(
              offset,
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
          // NOTE(RA): Since the new configuration can be rejected inside storeConfigurationEntry,
          // we look up the current configuration again to see if it was stored successfully.
          implicit val ec: ExecutionContext = DEC
          for {
            response <- ledgerDao
              .storeConfigurationEntry(
                offset,
                recordTime,
                submissionId,
                participantId,
                config,
                None
              )
            newConfig <- ledgerDao.lookupLedgerConfiguration()
          } yield {
            currentConfiguration.set(newConfig.map(_._2))
            response
          }
        }

      storeF
        .map(_ => ())(DEC)
        .recover {
          case t =>
            //recovering from the failure so the persistence stream doesn't die
            logger.error(s"Failed to persist configuration with offset: $offset", t)
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
      initialLedgerId: LedgerIdMode,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      startMode: SqlStartMode,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      initialConfig: Configuration,
      queueDepth: Int,
      maxBatchSize: Int,
  )(implicit mat: Materializer): Future[SqlLedger] = {
    implicit val ec: ExecutionContext = DEC

    def init(): Future[LedgerId] = startMode match {
      case SqlStartMode.AlwaysReset =>
        for {
          _ <- reset()
          ledgerId <- initialize(
            initialLedgerId,
            participantId,
            timeProvider,
            acs,
            packages,
            initialLedgerEntries,
            initialConfig)
        } yield ledgerId
      case SqlStartMode.ContinueIfExists =>
        initialize(
          initialLedgerId,
          participantId,
          timeProvider,
          acs,
          packages,
          initialLedgerEntries,
          initialConfig)
    }

    for {
      ledgerId <- init()
      ledgerEnd <- ledgerDao.lookupLedgerEnd()
      ledgerConfig <- ledgerDao.lookupLedgerConfiguration()
    } yield
      new SqlLedger(
        ledgerId,
        participantId,
        ledgerEnd,
        ledgerConfig.map(_._2),
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
      initialLedgerId: LedgerIdMode,
      participantId: ParticipantId,
      timeProvider: TimeProvider,
      acs: InMemoryActiveLedgerState,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      initialConfig: Configuration,
  ): Future[LedgerId] = {
    // Note that here we only store the ledger entry and we do not update anything else, such as the
    // headRef. We also are not concerns with heartbeats / checkpoints. This is OK since this initialization
    // step happens before we start up the sql ledger at all, so it's running in isolation.

    implicit val ec: ExecutionContext = DEC
    for {
      currentLedgerId <- ledgerDao.lookupLedgerId()
      initializationRequired = currentLedgerId.isEmpty
      ledgerId <- (currentLedgerId, initialLedgerId) match {
        case (Some(foundLedgerId), LedgerIdMode.Static(initialId)) if foundLedgerId == initialId =>
          ledgerFound(foundLedgerId, initialLedgerEntries, packages)

        case (Some(foundLedgerId), LedgerIdMode.Static(initialId)) =>
          Future.failed(
            new LedgerIdMismatchException(foundLedgerId, initialId) with StartupException)

        case (Some(foundLedgerId), LedgerIdMode.Dynamic) =>
          ledgerFound(foundLedgerId, initialLedgerEntries, packages)

        case (None, LedgerIdMode.Static(initialId)) =>
          Future.successful(initialId)

        case (None, LedgerIdMode.Dynamic) =>
          val randomLedgerId = LedgerIdGenerator.generateRandomId()
          Future.successful(randomLedgerId)
      }
      _ <- if (initializationRequired) {
        logger.info(s"Initializing ledger with ID: $ledgerId")
        for {
          _ <- ledgerDao.initializeLedger(ledgerId, Offset.begin)
          _ <- initializeLedgerEntries(
            initialLedgerEntries,
            initialConfig,
            timeProvider,
            packages,
            acs,
            participantId)
        } yield ()
      } else {
        Future.unit
      }
    } yield ledgerId
  }

  private def ledgerFound(
      foundLedgerId: LedgerId,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      packages: InMemoryPackageStore,
  ): Future[LedgerId] = {
    logger.info(s"Found existing ledger with ID: $foundLedgerId")
    if (initialLedgerEntries.nonEmpty) {
      logger.warn(
        s"Initial ledger entries provided, presumably from scenario, but there is an existing database, and thus they will not be used.")
    }
    if (packages.listLfPackagesSync().nonEmpty) {
      logger.warn(
        s"Initial packages provided, presumably as command line arguments, but there is an existing database, and thus they will not be used.")
    }
    Future.successful(foundLedgerId)
  }

  private def initializeLedgerEntries(
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      initialConfig: Configuration,
      timeProvider: TimeProvider,
      packages: InMemoryPackageStore,
      acs: InMemoryActiveLedgerState,
      participantId: ParticipantId,
  )(implicit executionContext: ExecutionContext): Future[Unit] = {
    if (initialLedgerEntries.nonEmpty) {
      logger.info(s"Initializing ledger with ${initialLedgerEntries.length} ledger entries.")
    }

    val (ledgerEnd, ledgerEntries) =
      initialLedgerEntries.foldLeft((1L, Vector.empty[(Offset, LedgerEntry)])) {
        case ((offset, entries), entryOrBump) =>
          entryOrBump match {
            case LedgerEntryOrBump.Entry(entry) =>
              (offset + 1, entries :+ SandboxOffset.toOffset(offset + 1) -> entry)
            case LedgerEntryOrBump.Bump(increment) =>
              (offset + increment, entries)
          }
      }

    val contracts = acs.activeContracts.values.toList
    for {
      _ <- copyPackages(packages, timeProvider.getCurrentTime, SandboxOffset.toOffset(ledgerEnd))
      _ <- ledgerDao.storeConfigurationEntry(
        offset = SandboxOffset.toOffset(0),
        recordedAt = timeProvider.getCurrentTime,
        submissionId = UUID.randomUUID.toString,
        participantId = participantId,
        configuration = initialConfig,
        rejectionReason = None,
      )
      _ <- ledgerDao.storeInitialState(contracts, ledgerEntries, SandboxOffset.toOffset(ledgerEnd))
    } yield ()
  }

  private def copyPackages(
      store: InMemoryPackageStore,
      knownSince: Instant,
      newLedgerEnd: Offset): Future[Unit] = {

    val packageDetails = store.listLfPackagesSync()
    if (packageDetails.nonEmpty) {
      logger.info(s"Copying initial packages ${packageDetails.keys.mkString(",")}")
      val packages = packageDetails.toList.map(pkg => {
        val archive =
          store.getLfArchiveSync(pkg._1).getOrElse(sys.error(s"Package ${pkg._1} not found"))
        archive -> PackageDetails(archive.getPayload.size.toLong, knownSince, None)
      })

      ledgerDao
        .storePackageEntry(newLedgerEnd, packages, None)
        .transform(_ => (), e => sys.error("Failed to copy initial packages: " + e.getMessage))(DEC)
    } else {
      Future.successful(())
    }
  }

}
