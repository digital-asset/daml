// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.ParticipantId
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.common
import com.daml.platform.common.MismatchException
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.parallel.ParallelIndexerFactory
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.backend.StorageBackend
import com.daml.platform.store.dao.LedgerDao
import com.daml.platform.store.{DbType, FlywayMigrations, LfValueTranslationCache}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object JdbcIndexer {
  // TODO append-only: currently StandaloneIndexerServer (among others) has a hard dependency on this concrete class.
  // Clean this up, for example by extracting the public interface of this class into a trait so that it's easier
  // to write applications that do not depend on a particular indexer implementation.
  private[daml] final class Factory private[indexer] (
      config: IndexerConfig,
      readService: ReadService,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      updateFlowOwnerBuilder: ExecuteUpdate.FlowOwnerBuilder,
      serverRole: ServerRole,
      flywayMigrations: FlywayMigrations,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
  )(implicit materializer: Materializer, loggingContext: LoggingContext) {

    private[daml] def this(
        serverRole: ServerRole,
        config: IndexerConfig,
        readService: ReadService,
        servicesExecutionContext: ExecutionContext,
        metrics: Metrics,
        lfValueTranslationCache: LfValueTranslationCache.Cache,
    )(implicit materializer: Materializer, loggingContext: LoggingContext) =
      this(
        config,
        readService,
        servicesExecutionContext,
        metrics,
        ExecuteUpdate.owner,
        serverRole,
        new FlywayMigrations(config.jdbcUrl),
        lfValueTranslationCache,
      )

    private val logger = ContextualizedLogger.get(this.getClass)

    def validateSchema()(implicit
        resourceContext: ResourceContext
    ): Future[ResourceOwner[Indexer]] =
      flywayMigrations
        .validate(config.enableAppendOnlySchema)
        .flatMap(_ => initialized(resetSchema = false))(resourceContext.executionContext)

    def migrateSchema(
        allowExistingSchema: Boolean
    )(implicit resourceContext: ResourceContext): Future[ResourceOwner[Indexer]] =
      flywayMigrations
        .migrate(allowExistingSchema, config.enableAppendOnlySchema)
        .flatMap(_ => initialized(resetSchema = false))(resourceContext.executionContext)

    def resetSchema()(implicit
        resourceContext: ResourceContext
    ): Future[ResourceOwner[Indexer]] = initialized(resetSchema = true)

    private[this] def initializedMutatingSchema(
        resetSchema: Boolean
    ): Future[ResourceOwner[Indexer]] =
      Future.successful(for {
        ledgerDao <- com.daml.platform.store.dao.JdbcLedgerDao.writeOwner(
          serverRole,
          config.jdbcUrl,
          config.databaseConnectionPoolSize,
          config.databaseConnectionTimeout,
          config.eventsPageSize,
          servicesExecutionContext,
          metrics,
          lfValueTranslationCache,
          jdbcAsyncCommitMode = config.asyncCommitMode,
          enricher = None,
        )
        _ <-
          if (resetSchema) {
            ResourceOwner.forFuture(() => ledgerDao.reset())
          } else {
            ResourceOwner.unit
          }
        initialLedgerEnd <- initializeLedger(ledgerDao)()
        dbType = DbType.jdbcType(config.jdbcUrl)
        updateFlow <- updateFlowOwnerBuilder(
          dbType,
          ledgerDao,
          metrics,
          config.participantId,
          config.updatePreparationParallelism,
          materializer.executionContext,
          loggingContext,
        )
      } yield new JdbcIndexer(initialLedgerEnd, metrics, updateFlow))

    private[this] def initializedAppendOnlySchema(resetSchema: Boolean)(implicit
        resourceContext: ResourceContext
    ): Future[ResourceOwner[Indexer]] = {
      implicit val executionContext: ExecutionContext = resourceContext.executionContext
      // TODO append-only: clean up the mixed use of Future, Resource, and ResourceOwner
      Future.successful(for {
        // Note: the LedgerDao interface is only used for initialization here, it can be released immediately
        // after initialization is finished. Hence the use of ResourceOwner.use().
        _ <- ResourceOwner.forFuture(() =>
          com.daml.platform.store.appendonlydao.JdbcLedgerDao
            .writeOwner(
              serverRole,
              config.jdbcUrl,
              config.databaseConnectionPoolSize,
              config.databaseConnectionTimeout,
              config.eventsPageSize,
              servicesExecutionContext,
              metrics,
              lfValueTranslationCache,
              jdbcAsyncCommitMode = config.asyncCommitMode,
              enricher = None,
              participantId = config.participantId,
            )
            .use(ledgerDao =>
              for {
                _ <-
                  if (resetSchema) {
                    ledgerDao.reset()
                  } else {
                    Future.successful(())
                  }
                _ <- initializeLedger(ledgerDao)().acquire().asFuture
              } yield ()
            )
        )
        indexer <- ParallelIndexerFactory(
          jdbcUrl = config.jdbcUrl,
          storageBackend = StorageBackend.of(DbType.jdbcType(config.jdbcUrl)),
          participantId = config.participantId,
          translation = new LfValueTranslation(
            cache = lfValueTranslationCache,
            metrics = metrics,
            enricherO = None,
            loadPackage = (_, _) => Future.successful(None),
          ),
          compressionStrategy =
            if (config.enableCompression) CompressionStrategy.allGZIP(metrics)
            else CompressionStrategy.none(metrics),
          mat = materializer,
          maxInputBufferSize = config.maxInputBufferSize,
          inputMappingParallelism = config.inputMappingParallelism,
          batchingParallelism = config.batchingParallelism,
          ingestionParallelism = config.ingestionParallelism,
          submissionBatchSize = config.submissionBatchSize,
          tailingRateLimitPerSecond = config.tailingRateLimitPerSecond,
          batchWithinMillis = config.batchWithinMillis,
          metrics = metrics,
        )
      } yield indexer)
    }

    private def initialized(resetSchema: Boolean)(implicit
        resourceContext: ResourceContext
    ): Future[ResourceOwner[Indexer]] =
      if (config.enableAppendOnlySchema)
        initializedAppendOnlySchema(resetSchema)
      else
        initializedMutatingSchema(resetSchema)

    // TODO append-only: This is just a series of database operations, it should return a Future and not a ResourceOwner
    private def initializeLedger(dao: LedgerDao)(): ResourceOwner[Option[Offset]] =
      new ResourceOwner[Option[Offset]] {
        override def acquire()(implicit context: ResourceContext): Resource[Option[Offset]] =
          Resource.fromFuture(for {
            initialConditions <- readService.getLedgerInitialConditions().runWith(Sink.head)
            existingLedgerId <- dao.lookupLedgerId()
            providedLedgerId = domain.LedgerId(initialConditions.ledgerId)
            _ <- existingLedgerId.fold(initializeLedgerData(providedLedgerId, dao))(
              checkLedgerIds(_, providedLedgerId)
            )
            _ <- initOrCheckParticipantId(dao)
            initialLedgerEnd <- dao.lookupInitialLedgerEnd()
          } yield initialLedgerEnd)
      }

    private def checkLedgerIds(
        existingLedgerId: domain.LedgerId,
        providedLedgerId: domain.LedgerId,
    ): Future[Unit] =
      if (existingLedgerId == providedLedgerId) {
        logger.info(s"Found existing ledger with ID: $existingLedgerId")
        Future.unit
      } else {
        Future.failed(new MismatchException.LedgerId(existingLedgerId, providedLedgerId))
      }

    private def initializeLedgerData(
        providedLedgerId: domain.LedgerId,
        ledgerDao: LedgerDao,
    ): Future[Unit] = {
      logger.info(s"Initializing ledger with ID: $providedLedgerId")
      ledgerDao.initializeLedger(providedLedgerId)
    }

    private def initOrCheckParticipantId(
        dao: LedgerDao
    )(implicit resourceContext: ResourceContext): Future[Unit] = {
      val id = ParticipantId(Ref.ParticipantId.assertFromString(config.participantId))
      dao
        .lookupParticipantId()
        .flatMap(
          _.fold(dao.initializeParticipantId(id)) {
            case `id` =>
              Future.successful(logger.info(s"Found existing participant id '$id'"))
            case retrievedLedgerId =>
              Future.failed(new common.MismatchException.ParticipantId(retrievedLedgerId, id))
          }
        )(resourceContext.executionContext)
    }

  }

  private val logger = ContextualizedLogger.get(classOf[JdbcIndexer])
}

/** @param startExclusive The last offset received from the read service.
  */
private[daml] class JdbcIndexer private[indexer] (
    startExclusive: Option[Offset],
    metrics: Metrics,
    executeUpdate: ExecuteUpdate,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends Indexer {

  import JdbcIndexer.logger

  override def subscription(readService: ReadService): ResourceOwner[IndexFeedHandle] =
    new SubscriptionResourceOwner(readService)

  private def handleStateUpdate(implicit
      loggingContext: LoggingContext
  ): Flow[OffsetUpdate, Unit, NotUsed] =
    Flow[OffsetUpdate]
      .wireTap(Sink.foreach[OffsetUpdate] { case OffsetUpdate(offsetStep, update) =>
        val lastReceivedRecordTime = update.recordTime.toInstant.toEpochMilli

        logger.trace(update.description)

        metrics.daml.indexer.lastReceivedRecordTime.updateValue(lastReceivedRecordTime)
        metrics.daml.indexer.lastReceivedOffset.updateValue(offsetStep.offset.toApiString)
      })
      .via(executeUpdate.flow)
      .map(_ => ())

  private def zipWithPreviousOffset(
      initialOffset: Option[Offset]
  ): Flow[(Offset, Update), OffsetUpdate, NotUsed] =
    Flow[(Offset, Update)]
      .statefulMapConcat { () =>
        val previousOffsetRef = new AtomicReference(initialOffset)

        { offsetUpdateTuple: (Offset, Update) =>
          val (nextOffset, update) = offsetUpdateTuple
          val offsetStep =
            previousOffsetRef
              .getAndSet(Some(nextOffset))
              .map(IncrementalOffsetStep(_, nextOffset))
              .getOrElse(CurrentOffset(nextOffset))

          OffsetUpdate(offsetStep, update) :: Nil
        }
      }

  private class SubscriptionResourceOwner(
      readService: ReadService
  )(implicit loggingContext: LoggingContext)
      extends ResourceOwner[IndexFeedHandle] {
    override def acquire()(implicit context: ResourceContext): Resource[IndexFeedHandle] =
      Resource(Future {
        val (killSwitch, completionFuture) = readService
          .stateUpdates(startExclusive)
          .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
          .via(zipWithPreviousOffset(startExclusive))
          .via(handleStateUpdate)
          .toMat(Sink.ignore)(Keep.both)
          .run()

        new SubscriptionIndexFeedHandle(killSwitch, completionFuture.map(_ => ()))
      })(handle =>
        for {
          _ <- Future(handle.killSwitch.shutdown())
          _ <- handle.completed.recover { case NonFatal(_) => () }
        } yield ()
      )
  }

  private class SubscriptionIndexFeedHandle(
      val killSwitch: KillSwitch,
      override val completed: Future[Unit],
  ) extends IndexFeedHandle

}
