// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import akka.Done
import akka.stream.QueueOfferResult.{Dropped, Enqueued, QueueClosed}
import akka.stream.scaladsl.{Keep, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import com.daml.api.util.TimeProvider
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.dec.{DirectExecutionContext => DEC}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{LedgerId, ParticipantId, PartyDetails}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.transaction.TransactionCommitter
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.common.{LedgerIdMode, MismatchException}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.LedgerIdGenerator
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.sandbox.stores.ledger.sql.SqlLedger._
import com.daml.platform.sandbox.stores.ledger.{Ledger, SandboxOffset}
import com.daml.platform.store.dao.events.LfValueTranslation
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerDao}
import com.daml.platform.store.entries.{LedgerEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.{BaseLedger, FlywayMigrations}
import com.daml.resources.ProgramResource.StartupException
import scalaz.Tag

import scala.collection.immutable.Queue
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

private[sandbox] object SqlLedger {

  private type PersistenceQueue = SourceQueueWithComplete[Offset => Future[Unit]]

  object Owner {

    private val nonEmptyLedgerEntriesWarningMessage =
      "Initial ledger entries provided, presumably from scenario, but there is an existing database, and thus they will not be used."

    private val nonEmptyPackagesWarningMessage =
      "Initial packages provided, presumably as command line arguments, but there is an existing database, and thus they will not be used."

  }

  final class Owner(
      name: LedgerName,
      serverRole: ServerRole,
      // jdbcUrl must have the user/password encoded in form of: "jdbc:postgresql://localhost/test?user=fred&password=secret"
      jdbcUrl: String,
      providedLedgerId: LedgerIdMode,
      participantId: domain.ParticipantId,
      timeProvider: TimeProvider,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      queueDepth: Int,
      transactionCommitter: TransactionCommitter,
      startMode: SqlStartMode = SqlStartMode.ContinueIfExists,
      eventsPageSize: Int,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslation.Cache,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[Ledger] {

    private val logger = ContextualizedLogger.get(this.getClass)

    override def acquire()(implicit context: ResourceContext): Resource[Ledger] =
      for {
        _ <- Resource.fromFuture(new FlywayMigrations(jdbcUrl).migrate())
        dao <- ledgerDaoOwner().acquire()
        _ <- startMode match {
          case SqlStartMode.AlwaysReset =>
            Resource.fromFuture(dao.reset())
          case SqlStartMode.ContinueIfExists =>
            Resource.unit
        }
        retrievedLedgerId <- Resource.fromFuture(dao.lookupLedgerId())
        ledgerId <- Resource.fromFuture(retrievedLedgerId.fold(initialize(dao))(resume))
        _ <- Resource.fromFuture(initOrCheckParticipantId(dao, Tag.unwrap(participantId)))
        ledgerEnd <- Resource.fromFuture(dao.lookupLedgerEnd())
        ledgerConfig <- Resource.fromFuture(dao.lookupLedgerConfiguration())
        dispatcher <- dispatcherOwner(ledgerEnd).acquire()
        persistenceQueue <- new PersistenceQueueOwner(dispatcher).acquire()
        // Close the dispatcher before the persistence queue.
        _ <- Resource(Future.unit)(_ => Future.successful(dispatcher.close()))
        ledger <- sqlLedgerOwner(
          ledgerId,
          ledgerConfig.map(_._2),
          dao,
          dispatcher,
          persistenceQueue,
        ).acquire()
      } yield ledger

    // Store only the ledger entries (no headref, etc.). This is OK since this initialization
    // step happens before we start up the sql ledger at all, so it's running in isolation.
    private def initialize(
        dao: LedgerDao,
    )(implicit executionContext: ExecutionContext): Future[LedgerId] = {
      val ledgerId = providedLedgerId.or(LedgerIdGenerator.generateRandomId(name))
      logger.info(s"Initializing node with ledger id '$ledgerId'")
      for {
        _ <- dao.initializeLedger(ledgerId)
        _ <- initializeLedgerEntries(
          initialLedgerEntries,
          timeProvider,
          packages,
          dao,
        )
      } yield ledgerId
    }

    private def resume(retrievedLedgerId: LedgerId): Future[LedgerId] =
      providedLedgerId match {
        case LedgerIdMode.Static(`retrievedLedgerId`) | LedgerIdMode.Dynamic =>
          logger.info(s"Found existing ledger id '$retrievedLedgerId'")
          if (initialLedgerEntries.nonEmpty) {
            logger.warn(Owner.nonEmptyLedgerEntriesWarningMessage)
          }
          if (packages.listLfPackagesSync().nonEmpty) {
            logger.warn(Owner.nonEmptyPackagesWarningMessage)
          }
          Future.successful(retrievedLedgerId)
        case LedgerIdMode.Static(mismatchingProvidedLedgerId) =>
          Future.failed(
            new MismatchException.LedgerId(retrievedLedgerId, mismatchingProvidedLedgerId)
            with StartupException
          )
      }

    private def initOrCheckParticipantId(dao: LedgerDao, participantId: String)(
        implicit executionContext: ExecutionContext,
    ): Future[Unit] = {
      val providedLedgerId = ParticipantId(Ref.ParticipantId.assertFromString(participantId))
      dao
        .lookupParticipantId()
        .flatMap(
          _.fold(dao.initializeParticipantId(providedLedgerId)) {
            case `providedLedgerId` =>
              Future.successful(logger.info(s"Found existing participant id '$providedLedgerId'"))
            case retrievedLedgerId =>
              Future.failed(
                new MismatchException.ParticipantId(retrievedLedgerId, providedLedgerId))
          }
        )
    }

    private def initializeLedgerEntries(
        initialLedgerEntries: ImmArray[LedgerEntryOrBump],
        timeProvider: TimeProvider,
        packages: InMemoryPackageStore,
        ledgerDao: LedgerDao,
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

      for {
        _ <- copyPackages(
          packages,
          ledgerDao,
          timeProvider.getCurrentTime,
          SandboxOffset.toOffset(ledgerEnd),
        )
        _ <- ledgerDao.storeInitialState(ledgerEntries, SandboxOffset.toOffset(ledgerEnd))
      } yield ()
    }

    private def copyPackages(
        store: InMemoryPackageStore,
        ledgerDao: LedgerDao,
        knownSince: Instant,
        newLedgerEnd: Offset,
    ): Future[Unit] = {
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
          .transform(_ => (), e => sys.error("Failed to copy initial packages: " + e.getMessage))(
            DEC)
      } else {
        Future.unit
      }
    }

    private def ledgerDaoOwner(): ResourceOwner[LedgerDao] =
      JdbcLedgerDao.validatingWriteOwner(
        serverRole,
        jdbcUrl,
        eventsPageSize,
        metrics,
        lfValueTranslationCache,
      )

    private def dispatcherOwner(ledgerEnd: Offset): ResourceOwner[Dispatcher[Offset]] =
      Dispatcher.owner(
        name = "sql-ledger",
        zeroIndex = Offset.beforeBegin,
        headAtInitialization = ledgerEnd,
      )

    private def sqlLedgerOwner(
        ledgerId: LedgerId,
        ledgerConfig: Option[Configuration],
        ledgerDao: LedgerDao,
        dispatcher: Dispatcher[Offset],
        persistenceQueue: PersistenceQueue,
    ): ResourceOwner[SqlLedger] =
      ResourceOwner.forCloseable(
        () =>
          new SqlLedger(
            ledgerId,
            ledgerConfig,
            ledgerDao,
            dispatcher,
            timeProvider,
            persistenceQueue,
            transactionCommitter,
        ))

    private final class PersistenceQueueOwner(dispatcher: Dispatcher[Offset])
        extends ResourceOwner[PersistenceQueue] {
      override def acquire()(implicit context: ResourceContext): Resource[PersistenceQueue] =
        Resource(Future.successful {
          val queue =
            Source.queue[Offset => Future[Unit]](queueDepth, OverflowStrategy.dropNew)

          // By default we process the requests in batches when under pressure (see semantics of `batch`). Note
          // that this is safe on the read end because the readers rely on the dispatchers to know the
          // ledger end, and not the database itself. This means that they will not start reading from the new
          // ledger end until we tell them so, which we do when _all_ the entries have been committed.
          val persistenceQueue = queue
            .batch(1, Queue(_))(_.enqueue(_))
            .mapAsync(1)(persistAll)
            .toMat(Sink.ignore)(Keep.left[PersistenceQueue, Future[Done]])
            .run()
          watchForFailures(persistenceQueue)
          persistenceQueue
        })(queue => Future.successful(queue.complete()))

      private def persistAll(queue: Queue[Offset => Future[Unit]]): Future[Unit] = {
        implicit val executionContext: ExecutionContext = DEC
        val startOffset = SandboxOffset.fromOffset(dispatcher.getHead())
        // This will attempt to run the SQL queries concurrently, but there is no parallelism here,
        // so they will still run sequentially.
        Future
          .sequence(queue.toIterator.zipWithIndex.map {
            case (persist, i) =>
              val offset = startOffset + i + 1
              persist(SandboxOffset.toOffset(offset))
          })
          .map { _ =>
            // note that we can have holes in offsets in case of the storing of an entry failed for some reason
            dispatcher.signalNewHead(
              SandboxOffset
                .toOffset(startOffset + queue.length)) // signalling downstream subscriptions
          }
      }
    }

    private def watchForFailures(queue: SourceQueueWithComplete[_]): Unit =
      queue
        .watchCompletion()
        .failed
        .foreach { throwable =>
          logger.error("Persistence queue has been closed with a failure.", throwable)
        }(DEC)

  }
}

private final class SqlLedger(
    ledgerId: LedgerId,
    configAtInitialization: Option[Configuration],
    ledgerDao: LedgerDao,
    dispatcher: Dispatcher[Offset],
    timeProvider: TimeProvider,
    persistenceQueue: PersistenceQueue,
    transactionCommitter: TransactionCommitter,
) extends BaseLedger(ledgerId, ledgerDao, dispatcher)
    with Ledger {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def close(): Unit =
    super.close()

  // Note: ledger entries are written in batches, and this variable is updated while writing a ledger configuration
  // changed entry. Transactions written around the same time as a configuration change entry might not use the correct
  // time model.
  private[this] val currentConfiguration =
    new AtomicReference[Option[Configuration]](configAtInitialization)

  // Validates the given ledger time according to the ledger time model
  private def checkTimeModel(
      ledgerTime: Instant,
      recordTime: Instant,
  ): Either[RejectionReason, Unit] = {
    currentConfiguration
      .get()
      .fold[Either[RejectionReason, Unit]](
        Left(
          RejectionReason.InvalidLedgerTime(
            "No ledger configuration available, cannot validate ledger time"))
      )(
        _.timeModel.checkTime(ledgerTime, recordTime).left.map(RejectionReason.InvalidLedgerTime)
      )
  }

  override def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction,
  )(implicit loggingContext: LoggingContext): Future[SubmissionResult] =
    enqueue { offset =>
      val transactionId = offset.toApiString

      val ledgerTime = transactionMeta.ledgerEffectiveTime.toInstant
      val recordTime = timeProvider.getCurrentTime

      checkTimeModel(ledgerTime, recordTime)
        .fold(
          reason =>
            ledgerDao.storeRejection(
              Some(submitterInfo),
              recordTime,
              offset,
              reason,
          ),
          _ =>
            ledgerDao.storeTransaction(
              Some(submitterInfo),
              transactionMeta.workflowId,
              transactionId,
              recordTime,
              transactionMeta.ledgerEffectiveTime.toInstant,
              offset,
              transactionCommitter.commitTransaction(transactionId, transaction),
              Nil,
          )
        )
        .transform(
          _.map(_ => ()).recover {
            case NonFatal(t) =>
              logger.error(s"Failed to persist entry with offset: ${offset.toApiString}", t)
          }
        )(DEC)

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
      displayName: Option[String],
  )(implicit loggingContext: LoggingContext): Future[SubmissionResult] = {
    enqueue { offset =>
      ledgerDao
        .storePartyEntry(
          offset,
          PartyLedgerEntry.AllocationAccepted(
            Some(submissionId),
            timeProvider.getCurrentTime,
            PartyDetails(party, displayName, isLocal = true),
          ),
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
      payload: List[Archive],
  )(implicit loggingContext: LoggingContext): Future[SubmissionResult] = {
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
      config: Configuration,
  )(implicit loggingContext: LoggingContext): Future[SubmissionResult] =
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
            response <- ledgerDao.storeConfigurationEntry(
              offset,
              recordTime,
              submissionId,
              config,
              None,
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
