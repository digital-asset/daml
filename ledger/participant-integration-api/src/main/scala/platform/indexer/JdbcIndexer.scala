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
import com.daml.platform.indexer.poc.PoCIndexerFactory
import com.daml.platform.store.dao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.dao.{JdbcLedgerDao, LedgerDao}
import com.daml.platform.store.{DbType, FlywayMigrations}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object JdbcIndexer {

  private[daml] final class Factory private[indexer] (
      config: IndexerConfig,
      readService: ReadService,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      updateFlowOwnerBuilder: ExecuteUpdate.FlowOwnerBuilder,
      ledgerDaoOwner: ResourceOwner[LedgerDao],
      flywayMigrations: FlywayMigrations,
      lfValueTranslationCache: LfValueTranslation.Cache,
  )(implicit materializer: Materializer, loggingContext: LoggingContext) {

    private[daml] def this(
        serverRole: ServerRole,
        config: IndexerConfig,
        readService: ReadService,
        servicesExecutionContext: ExecutionContext,
        metrics: Metrics,
        lfValueTranslationCache: LfValueTranslation.Cache,
    )(implicit materializer: Materializer, loggingContext: LoggingContext) =
      this(
        config,
        readService,
        servicesExecutionContext,
        metrics,
        ExecuteUpdate.owner,
        JdbcLedgerDao.writeOwner(
          serverRole,
          config.jdbcUrl,
          config.databaseConnectionPoolSize,
          config.eventsPageSize,
          servicesExecutionContext,
          metrics,
          lfValueTranslationCache,
          jdbcAsyncCommits = true,
          enricher = None,
        ),
        new FlywayMigrations(config.jdbcUrl),
        lfValueTranslationCache,
      )

    private val logger = ContextualizedLogger.get(this.getClass)

    def validateSchema()(implicit
        resourceContext: ResourceContext
    ): Future[ResourceOwner[Indexer]] =
      flywayMigrations
        .validate()
        .flatMap(_ => initialized(resetSchema = false))(resourceContext.executionContext)

    def migrateSchema(
        allowExistingSchema: Boolean
    )(implicit resourceContext: ResourceContext): Future[ResourceOwner[Indexer]] =
      flywayMigrations
        .migrate(allowExistingSchema)
        .flatMap(_ => initialized(resetSchema = false))(resourceContext.executionContext)

    def resetSchema(): Future[ResourceOwner[Indexer]] = initialized(resetSchema = true)

    private def initialized(resetSchema: Boolean): Future[ResourceOwner[Indexer]] =
      if (config.usePoCIndexer)
        Future.successful(
          for {
            ledgerDao <- ledgerDaoOwner
            _ <-
              if (resetSchema) {
                ResourceOwner.forFuture(() => ledgerDao.reset())
              } else {
                ResourceOwner.unit
              }
            _ <- initializeLedger(ledgerDao)()
            // TODO we defer initialisation to original code for now, from here PoC ledger factoring starts
            // TODO until here mutable initialisation is concluded, from here read only initialisation ATM
            indexer <- PoCIndexerFactory(
              jdbcUrl = config.jdbcUrl,
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
              inputMappingParallelism = config.inputMappingParallelism,
              ingestionParallelism = config.ingestionParallelism,
              submissionBatchSize = config.submissionBatchSize,
              tailingRateLimitPerSecond = config.tailingRateLimitPerSecond,
              batchWithinMillis = config.batchWithinMillis,
              runStageUntil = config.runStageUntil,
            )
          } yield indexer
        )
      else
        Future.successful(for {
          ledgerDao <- ledgerDaoOwner
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

}

class SubscriptionIndexFeedHandle(val killSwitch: KillSwitch, override val completed: Future[Unit])
    extends IndexFeedHandle
