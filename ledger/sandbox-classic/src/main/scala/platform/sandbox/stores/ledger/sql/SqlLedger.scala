// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger.sql

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
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.{ContractStore, PackageDetails}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.data.{ImmArray, Ref, Time}
import com.daml.lf.engine.{Engine, ValueEnricher}
import com.daml.lf.transaction.{SubmittedTransaction, TransactionCommitter}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.PruneBuffersNoOp
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.common.{LedgerIdMode, MismatchException}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.packages.InMemoryPackageStore
import com.daml.platform.sandbox.LedgerIdGenerator
import com.daml.platform.sandbox.config.LedgerName
import com.daml.platform.sandbox.stores.ledger.ScenarioLoader.LedgerEntryOrBump
import com.daml.platform.sandbox.stores.ledger.sql.SqlLedger._
import com.daml.platform.sandbox.stores.ledger.{Ledger, Rejection, SandboxOffset}
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.appendonlydao.events.CompressionStrategy
import com.daml.platform.store.cache.{MutableLedgerEndCache, TranslationCacheBackedContractStore}
import com.daml.platform.store.appendonlydao.{LedgerDao, LedgerWriteDao}
import com.daml.platform.store.cache.TranslationCacheBackedContractStore
import com.daml.platform.store.entries.{LedgerEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.{BaseLedger, FlywayMigrations, LfValueTranslationCache}
import com.google.rpc.status.{Status => RpcStatus}
import io.grpc.Status

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration
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
      databaseConnectionPoolSize: Int,
      databaseConnectionTimeout: FiniteDuration,
      providedLedgerId: LedgerIdMode,
      participantId: domain.ParticipantId,
      timeProvider: TimeProvider,
      packages: InMemoryPackageStore,
      initialLedgerEntries: ImmArray[LedgerEntryOrBump],
      queueDepth: Int,
      transactionCommitter: TransactionCommitter,
      startMode: SqlStartMode,
      eventsPageSize: Int,
      eventsProcessingParallelism: Int,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
      engine: Engine,
      validatePartyAllocation: Boolean,
      enableCompression: Boolean,
      errorFactories: ErrorFactories,
  )(implicit mat: Materializer, loggingContext: LoggingContext)
      extends ResourceOwner[Ledger] {

    private val logger = ContextualizedLogger.get(this.getClass)

    override def acquire()(implicit context: ResourceContext): Resource[Ledger] =
      for {
        _ <- Resource.fromFuture(
          new FlywayMigrations(jdbcUrl).migrate()
        )
        ledgerEndCache = MutableLedgerEndCache()
        dao <- ledgerDaoOwner(ledgerEndCache, servicesExecutionContext, errorFactories).acquire()
        _ <- startMode match {
          case SqlStartMode.ResetAndStart =>
            Resource.fromFuture(dao.reset())
          case SqlStartMode.MigrateAndStart =>
            Resource.unit
        }
        existingLedgerId <- Resource.fromFuture(dao.lookupLedgerId())
        existingParticipantId <- Resource.fromFuture(dao.lookupParticipantId())
        ledgerId <- Resource.fromFuture(initialize(dao, existingLedgerId, existingParticipantId))
        ledgerEnd <- Resource.fromFuture(dao.lookupLedgerEndOffsetAndSequentialId())
        _ = ledgerEndCache.set(ledgerEnd)
        ledgerConfig <- Resource.fromFuture(dao.lookupLedgerConfiguration())
        dispatcher <- dispatcherOwner(ledgerEnd._1).acquire()
        persistenceQueue <- new PersistenceQueueOwner(dispatcher).acquire()
        // Close the dispatcher before the persistence queue.
        _ <- Resource(Future.unit)(_ => Future.successful(dispatcher.close()))
        contractStore <- TranslationCacheBackedContractStore.owner(
          lfValueTranslationCache,
          dao.contractsReader,
        )
        ledger <- sqlLedgerOwner(
          ledgerId,
          ledgerConfig.map(_._2),
          dao,
          contractStore,
          dispatcher,
          persistenceQueue,
        ).acquire()
      } yield ledger

    // Store only the ledger entries (no headref, etc.). This is OK since this initialization
    // step happens before we start up the sql ledger at all, so it's running in isolation.
    private def initialize(
        dao: LedgerWriteDao,
        existingLedgerIdO: Option[LedgerId],
        existingParticipantIdO: Option[ParticipantId],
    )(implicit executionContext: ExecutionContext): Future[LedgerId] = {
      val ledgerId = providedLedgerId.or(LedgerIdGenerator.generateRandomId(name))
      (providedLedgerId, existingLedgerIdO, existingParticipantIdO) match {
        case (_, None, _) =>
          initializeNewLedger(dao, ledgerId, participantId)
        case (LedgerIdMode.Dynamic, Some(existingLedgerId), Some(`participantId`)) =>
          // In dynamic ledgerId mode, reuse the existing ledgerId if the participantId matches
          logger.info(
            s"Dynamic ledger id mode: Found matching participant id '$participantId', using existing ledger id '$existingLedgerId'"
          )
          initializeExistingLedger(dao, existingLedgerId, participantId)
        case (LedgerIdMode.Dynamic, _, Some(existingParticipantId)) =>
          logger.error(
            s"Dynamic ledger id mode: Found existing participant id '$existingParticipantId' not matching provided participant id '$participantId'"
          )
          throw MismatchException.ParticipantId(existingParticipantId, participantId)
        case _ =>
          initializeExistingLedger(dao, ledgerId, participantId)
      }
    }

    private def initializeExistingLedger(
        dao: LedgerWriteDao,
        ledgerId: LedgerId,
        participantId: ParticipantId,
    )(implicit executionContext: ExecutionContext): Future[LedgerId] = {
      logger.info(s"Resuming node with ledger id '$ledgerId and participant id '$participantId'")
      if (initialLedgerEntries.nonEmpty) {
        logger.warn(Owner.nonEmptyLedgerEntriesWarningMessage)
      }
      if (packages.listLfPackagesSync().nonEmpty) {
        logger.warn(Owner.nonEmptyPackagesWarningMessage)
      }
      for {
        _ <- dao.initialize(ledgerId, participantId)
      } yield ledgerId
    }

    private def initializeNewLedger(
        dao: LedgerWriteDao,
        ledgerId: LedgerId,
        participantId: ParticipantId,
    )(implicit executionContext: ExecutionContext): Future[LedgerId] = {
      logger.info(
        s"Initializing node with ledger id '$ledgerId' and participant id '$participantId'"
      )
      for {
        _ <- dao.initialize(ledgerId, participantId)
        _ <- initializeLedgerEntries(
          initialLedgerEntries,
          timeProvider,
          packages,
          dao,
        )
      } yield ledgerId
    }

    private def initializeLedgerEntries(
        initialLedgerEntries: ImmArray[LedgerEntryOrBump],
        timeProvider: TimeProvider,
        packages: InMemoryPackageStore,
        ledgerDao: LedgerWriteDao,
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
          timeProvider.getCurrentTimestamp,
          SandboxOffset.toOffset(ledgerEnd),
        )
        _ <- ledgerDao.storeInitialState(ledgerEntries, SandboxOffset.toOffset(ledgerEnd))
      } yield ()
    }

    private def copyPackages(
        store: InMemoryPackageStore,
        ledgerDao: LedgerWriteDao,
        knownSince: Timestamp,
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
            DEC
          )
      } else {
        Future.unit
      }
    }

    private def ledgerDaoOwner(
        ledgerEndCache: MutableLedgerEndCache,
        servicesExecutionContext: ExecutionContext,
        errorFactories: ErrorFactories,
    ): ResourceOwner[LedgerDao] = {
    val compressionStrategy =
      if (enableCompression) CompressionStrategy.allGZIP(metrics)
      else CompressionStrategy.none(metrics)
      com.daml.platform.store.appendonlydao.JdbcLedgerDao.validatingWriteOwner(
        serverRole = serverRole,
        jdbcUrl = jdbcUrl,
        connectionPoolSize = databaseConnectionPoolSize,
        connectionTimeout = databaseConnectionTimeout,
        eventsPageSize = eventsPageSize,
        eventsProcessingParallelism = eventsProcessingParallelism,
        servicesExecutionContext = servicesExecutionContext,
        metrics = metrics,
        lfValueTranslationCache = lfValueTranslationCache,
        validatePartyAllocation = validatePartyAllocation,
        enricher = Some(new ValueEnricher(engine)),
        participantId = Ref.ParticipantId.assertFromString(participantId.toString),
        compressionStrategy = compressionStrategy,
        ledgerEndCache = ledgerEndCache,
        errorFactories = errorFactories,
      )
    }

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
        contractStore: ContractStore,
        dispatcher: Dispatcher[Offset],
        persistenceQueue: PersistenceQueue,
    ): ResourceOwner[SqlLedger] =
      ResourceOwner.forCloseable(() =>
        new SqlLedger(
          ledgerId,
          ledgerConfig,
          ledgerDao,
          contractStore,
          dispatcher,
          timeProvider,
          persistenceQueue,
          transactionCommitter,
        )
      )

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
          .sequence(queue.iterator.zipWithIndex.map { case (persist, i) =>
            val offset = startOffset + i + 1
            persist(SandboxOffset.toOffset(offset))
          })
          .map { _ =>
            // note that we can have holes in offsets in case of the storing of an entry failed for some reason
            dispatcher.signalNewHead(
              SandboxOffset
                .toOffset(startOffset + queue.length)
            ) // signalling downstream subscriptions
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
    contractStore: ContractStore,
    dispatcher: Dispatcher[Offset],
    timeProvider: TimeProvider,
    persistenceQueue: PersistenceQueue,
    transactionCommitter: TransactionCommitter,
) extends BaseLedger(
      ledgerId,
      ledgerDao,
      ledgerDao.transactionsReader,
      contractStore,
      PruneBuffersNoOp,
      dispatcher,
    )
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
      ledgerTime: Timestamp,
      recordTime: Timestamp,
  ): Either[Rejection, Unit] =
    currentConfiguration
      .get()
      .toRight(Rejection.NoLedgerConfiguration)
      .flatMap(
        _.timeModel.checkTime(ledgerTime, recordTime).left.map(Rejection.InvalidLedgerTime)
      )

  override def publishTransaction(
      submitterInfo: state.SubmitterInfo,
      transactionMeta: state.TransactionMeta,
      transaction: SubmittedTransaction,
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] =
    enqueue { offset =>
      val transactionId = offset.toApiString

      val ledgerTime = transactionMeta.ledgerEffectiveTime
      val recordTime = timeProvider.getCurrentTimestamp

      checkTimeModel(ledgerTime, recordTime)
        .fold(
          reason =>
            ledgerDao.storeRejection(
              completionInfo = Some(submitterInfo.toCompletionInfo),
              recordTime = recordTime,
              offset = offset,
              reason = reason.toStateRejectionReason,
            ),
          _ => {
            val divulgedContracts = Nil
            // This indexer-ledger does not trim fetch and lookupByKey nodes in the transaction,
            // so it doesn't need to pre-compute blinding information.
            val blindingInfo = None

            ledgerDao.storeTransaction(
              completionInfo = Some(submitterInfo.toCompletionInfo),
              workflowId = transactionMeta.workflowId,
              transactionId = transactionId,
              ledgerEffectiveTime = transactionMeta.ledgerEffectiveTime,
              offset = offset,
              transaction = transactionCommitter.commitTransaction(transactionId, transaction),
              divulgedContracts = divulgedContracts,
              blindingInfo = blindingInfo,
              recordTime = recordTime,
            )
          },
        )
        .transform(
          _.map(_ => ()).recover { case NonFatal(t) =>
            logger.error(s"Failed to persist entry with offset: ${offset.toApiString}", t)
          }
        )(DEC)

    }

  private def enqueue(persist: Offset => Future[Unit]): Future[state.SubmissionResult] =
    persistenceQueue
      .offer(persist)
      .transform {
        case Success(Enqueued) =>
          Success(state.SubmissionResult.Acknowledged)
        case Success(Dropped) =>
          Success(
            state.SubmissionResult.SynchronousError(
              RpcStatus.of(
                Status.Code.RESOURCE_EXHAUSTED.value(),
                "System is overloaded, please try again later",
                Seq.empty,
              )
            )
          )
        case Success(QueueClosed) =>
          Failure(new IllegalStateException("queue closed"))
        case Success(QueueOfferResult.Failure(e)) => Failure(e)
        case Failure(f) => Failure(f)
      }(DEC)

  override def publishPartyAllocation(
      submissionId: Ref.SubmissionId,
      party: Ref.Party,
      displayName: Option[String],
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] = {
    enqueue { offset =>
      ledgerDao
        .getParties(Seq(party))
        .flatMap {
          case Nil =>
            ledgerDao
              .storePartyEntry(
                offset,
                PartyLedgerEntry.AllocationAccepted(
                  Some(submissionId),
                  timeProvider.getCurrentTimestamp,
                  PartyDetails(party, displayName, isLocal = true),
                ),
              )
              .map(_ => ())(DEC)
              .recover { case t =>
                //recovering from the failure so the persistence stream doesn't die
                logger.error(
                  s"Failed to persist party $party with offset: ${offset.toApiString}",
                  t,
                )
                ()
              }(DEC)

          case _ =>
            logger.warn(
              s"Ignoring duplicate party submission with ID $party for submissionId ${Some(submissionId)}"
            )
            Future.unit
        }(DEC)
    }
  }

  override def uploadPackages(
      submissionId: Ref.SubmissionId,
      knownSince: Timestamp,
      sourceDescription: Option[String],
      payload: List[Archive],
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] = {
    val packages = payload.map(archive =>
      (archive, PackageDetails(archive.getPayload.size().toLong, knownSince, sourceDescription))
    )
    enqueue { offset =>
      ledgerDao
        .storePackageEntry(
          offset,
          packages,
          Some(
            PackageLedgerEntry.PackageUploadAccepted(submissionId, timeProvider.getCurrentTimestamp)
          ),
        )
        .map(_ => ())(DEC)
        .recover { case t =>
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
  )(implicit loggingContext: LoggingContext): Future[state.SubmissionResult] =
    enqueue { offset =>
      val recordTime = timeProvider.getCurrentTimestamp

      val storeF =
        if (recordTime > maxRecordTime) {
          ledgerDao
            .storeConfigurationEntry(
              offset,
              recordTime,
              submissionId,
              config,
              Some(s"Configuration change timed out: $maxRecordTime > $recordTime"),
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
        .recover { case t =>
          //recovering from the failure so the persistence stream doesn't die
          logger.error(s"Failed to persist configuration with offset: $offset", t)
          ()
        }(DEC)
    }
}
