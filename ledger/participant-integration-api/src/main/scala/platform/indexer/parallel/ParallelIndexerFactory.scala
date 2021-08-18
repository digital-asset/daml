// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.sql.Connection
import java.util.concurrent.{Executors, TimeUnit}

import akka.NotUsed
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{KillSwitch, KillSwitches, Materializer, UniqueKillSwitch}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext.withEnrichedLoggingContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{InstrumentedSource, Metrics}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.ha.{HaConfig, HaCoordinator, Handle, NoopHaCoordinator}
import com.daml.platform.indexer.parallel.AsyncSupport._
import com.daml.platform.indexer.{IndexFeedHandle, Indexer}
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.backend
import com.daml.platform.store.backend.DataSourceStorageBackend.DataSourceConfig
import com.daml.platform.store.backend.{DbDto, StorageBackend}
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

object ParallelIndexerFactory {

  private val keepAliveMaxIdleDuration = FiniteDuration(200, "millis")

  private val logger = ContextualizedLogger.get(this.getClass)

  def apply[DB_BATCH](
      jdbcUrl: String,
      storageBackend: StorageBackend[DB_BATCH],
      participantId: Ref.ParticipantId,
      translation: LfValueTranslation,
      compressionStrategy: CompressionStrategy,
      mat: Materializer,
      maxInputBufferSize: Int,
      inputMappingParallelism: Int,
      batchingParallelism: Int,
      ingestionParallelism: Int,
      submissionBatchSize: Long,
      tailingRateLimitPerSecond: Int,
      batchWithinMillis: Long,
      metrics: Metrics,
      dataSourceConfig: DataSourceConfig,
      haConfig: HaConfig,
  )(implicit loggingContext: LoggingContext): ResourceOwner[Indexer] = {
    for {
      inputMapperExecutor <- asyncPool(
        inputMappingParallelism,
        "input-mapping-pool",
        Some(metrics.daml.parallelIndexer.inputMapping.executor -> metrics.registry),
      )
      batcherExecutor <- asyncPool(
        batchingParallelism,
        "batching-pool",
        Some(metrics.daml.parallelIndexer.batching.executor -> metrics.registry),
      )
      toDbDto = backend.UpdateToDbDto(
        participantId = participantId,
        translation = translation,
        compressionStrategy = compressionStrategy,
      )
      haCoordinator <-
        if (storageBackend.dbLockSupported && haConfig.enable)
          ResourceOwner
            .forExecutorService(() =>
              ExecutionContext.fromExecutorService(
                Executors.newFixedThreadPool(
                  1,
                  new ThreadFactoryBuilder().setNameFormat(s"ha-coordinator-%d").build,
                )
              )
            )
            .map(
              HaCoordinator.databaseLockBasedHaCoordinator(
                // this DataSource will be used to spawn the main connection where we keep the Indexer Main Lock
                // The life-cycle of such connections matches the life-cycle of a protectedExecution
                dataSource = storageBackend.createDataSource(jdbcUrl),
                storageBackend = storageBackend,
                _,
                scheduler = mat.system.scheduler,
                haConfig = haConfig,
              )
            )
        else
          ResourceOwner.successful(NoopHaCoordinator)
    } yield {
      val ingest: (
          Long,
          DbDispatcher,
      ) => Source[(Offset, state.Update), NotUsed] => Source[Unit, NotUsed] =
        (initialSeqId, dbDispatcher) =>
          source =>
            BatchingParallelIngestionPipe(
              submissionBatchSize = submissionBatchSize,
              batchWithinMillis = batchWithinMillis,
              inputMappingParallelism = inputMappingParallelism,
              inputMapper = inputMapperExecutor.execute(inputMapper(metrics, toDbDto)),
              seqMapperZero = seqMapperZero(initialSeqId),
              seqMapper = seqMapper(metrics),
              batchingParallelism = batchingParallelism,
              batcher = batcherExecutor.execute(batcher(storageBackend.batch, metrics)),
              ingestingParallelism = ingestionParallelism,
              ingester = ingester(storageBackend.insertBatch, dbDispatcher, metrics),
              tailer = tailer(storageBackend.batch(Vector.empty)),
              tailingRateLimitPerSecond = tailingRateLimitPerSecond,
              ingestTail =
                ingestTail[DB_BATCH](storageBackend.updateLedgerEnd, dbDispatcher, metrics),
            )(
              InstrumentedSource
                .bufferedSource(
                  original = source,
                  counter = metrics.daml.parallelIndexer.inputBufferLength,
                  size = maxInputBufferSize,
                )
                .map(_ -> System.nanoTime())
            )
              .map(_ => ())
              .keepAlive( // TODO ha: remove as stable. This keepAlive approach was introduced for safety with async commit. This is still needed until HA is mandatory for Postgres to ensure safety with async commit. This will not needed anymore if HA is enabled by default, since the Ha mutual exclusion implementation with advisory locks makes impossible to let a db-shutdown go undetected.
                keepAliveMaxIdleDuration,
                () =>
                  if (dbDispatcher.currentHealth() == HealthStatus.healthy) {
                    logger.debug("Indexer keep-alive: database connectivity OK")
                  } else {
                    logger
                      .warn("Indexer keep-alive: database connectivity lost. Stopping indexing.")
                    throw new Exception(
                      "Connectivity issue to the index-database detected. Stopping indexing."
                    )
                  },
              )

      def subscribe(resourceContext: ResourceContext)(readService: state.ReadService): Handle = {
        implicit val rc: ResourceContext = resourceContext
        implicit val ec: ExecutionContext = resourceContext.executionContext
        implicit val matImplicit: Materializer = mat
        haCoordinator.protectedExecution { connectionInitializer =>
          val killSwitchPromise = Promise[KillSwitch]()

          val completionFuture = DbDispatcher
            .owner(
              // this is tha DataSource which will be wrapped by HikariCP, and which will drive the ingestion
              // therefore this needs to be configured with the connection-init-hook, what we get from HaCoordinator
              dataSource = storageBackend.createDataSource(
                jdbcUrl = jdbcUrl,
                dataSourceConfig = dataSourceConfig,
                connectionInitHook = Some(connectionInitializer.initialize),
              ),
              serverRole = ServerRole.Indexer,
              connectionPoolSize =
                ingestionParallelism + 1, // + 1 for the tailing ledger_end updates
              connectionTimeout = FiniteDuration(
                250,
                "millis",
              ), // 250 millis is the lowest possible value for this Hikari configuration (see HikariConfig JavaDoc)
              metrics = metrics,
            )
            .use { dbDispatcher =>
              dbDispatcher
                .executeSql(metrics.daml.parallelIndexer.initialization)(
                  storageBackend.initializeIngestion
                )
                .flatMap { initialized =>
                  val (killSwitch, completionFuture) =
                    ingest(initialized.lastEventSeqId.getOrElse(0L), dbDispatcher)(
                      readService.stateUpdates(beginAfter = initialized.lastOffset)
                    )
                      .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
                      .toMat(Sink.ignore)(Keep.both)
                      .run()
                  // the tricky bit:
                  // the future in the completion handler will be this one
                  // but the future for signaling for the HaCoordinator, that the protected execution is initialized needs to complete precisely here
                  killSwitchPromise.success(killSwitch)
                  completionFuture
                }
            }

          killSwitchPromise.future
            .map(Handle(completionFuture.map(_ => ()), _))
        }
      }

      toIndexer(subscribe)
    }
  }

  /** Batch wraps around a T-typed batch, enriching it with processing relevant information.
    *
    * @param lastOffset The latest offset available in the batch. Needed for tail ingestion.
    * @param lastSeqEventId The latest sequential-event-id in the batch, or if none present there, then the latest from before. Needed for tail ingestion.
    * @param lastRecordTime The latest record time in the batch, in milliseconds since Epoch. Needed for metrics population.
    * @param batch The batch of variable type.
    * @param batchSize Size of the batch measured in number of updates. Needed for metrics population.
    * @param averageStartTime The nanosecond timestamp of the start of the previous processing stage. Needed for metrics population: how much time is spend by a particular update in a certain stage.
    */
  case class Batch[+T](
      lastOffset: Offset,
      lastSeqEventId: Long,
      lastRecordTime: Long,
      batch: T,
      batchSize: Int,
      averageStartTime: Long,
      offsets: Vector[Offset],
  )

  def inputMapper(
      metrics: Metrics,
      toDbDto: Offset => state.Update => Iterator[DbDto],
  )(implicit
      loggingContext: LoggingContext
  ): Iterable[((Offset, state.Update), Long)] => Batch[Vector[DbDto]] = { input =>
    metrics.daml.parallelIndexer.inputMapping.batchSize.update(input.size)
    input.foreach { case ((offset, update), _) =>
      withEnrichedLoggingContext("offset" -> offset, "update" -> update) {
        implicit loggingContext =>
          logger.info(s"Storing ${update.description}")
      }
    }
    val batch = input.iterator.flatMap { case ((offset, update), _) =>
      toDbDto(offset)(update)
    }.toVector
    Batch(
      lastOffset = input.last._1._1,
      lastSeqEventId = 0, // will be filled later in the sequential step
      lastRecordTime = input.last._1._2.recordTime.toInstant.toEpochMilli,
      batch = batch,
      batchSize = input.size,
      averageStartTime = input.view.map(_._2 / input.size).sum,
      offsets = input.view.map(_._1._1).toVector,
    )
  }

  def seqMapperZero(initialSeqId: Long): Batch[Vector[DbDto]] =
    Batch(
      lastOffset = null,
      lastSeqEventId = initialSeqId, // this is the only property of interest in the zero element
      lastRecordTime = 0,
      batch = Vector.empty,
      batchSize = 0,
      averageStartTime = 0,
      offsets = Vector.empty,
    )

  def seqMapper(metrics: Metrics)(
      previous: Batch[Vector[DbDto]],
      current: Batch[Vector[DbDto]],
  ): Batch[Vector[DbDto]] = {
    var eventSeqId = previous.lastSeqEventId
    val batchWithSeqIds = current.batch.map {
      case dbDto: DbDto.EventCreate =>
        eventSeqId += 1
        dbDto.copy(event_sequential_id = eventSeqId)

      case dbDto: DbDto.EventExercise =>
        eventSeqId += 1
        dbDto.copy(event_sequential_id = eventSeqId)

      case dbDto: DbDto.EventDivulgence =>
        eventSeqId += 1
        dbDto.copy(event_sequential_id = eventSeqId)

      case notEvent => notEvent
    }
    val nowNanos = System.nanoTime()
    metrics.daml.parallelIndexer.inputMapping.duration.update(
      (nowNanos - current.averageStartTime) / current.batchSize,
      TimeUnit.NANOSECONDS,
    )
    current.copy(
      lastSeqEventId = eventSeqId,
      batch = batchWithSeqIds,
      averageStartTime = nowNanos, // setting start time to the start of the next stage
    )
  }

  def batcher[DB_BATCH](
      batchF: Vector[DbDto] => DB_BATCH,
      metrics: Metrics,
  ): Batch[Vector[DbDto]] => Batch[DB_BATCH] = { inBatch =>
    val dbBatch = batchF(inBatch.batch)
    val nowNanos = System.nanoTime()
    metrics.daml.parallelIndexer.batching.duration.update(
      (nowNanos - inBatch.averageStartTime) / inBatch.batchSize,
      TimeUnit.NANOSECONDS,
    )
    inBatch.copy(
      batch = dbBatch,
      averageStartTime = nowNanos,
    )
  }

  def ingester[DB_BATCH](
      ingestFunction: (Connection, DB_BATCH) => Unit,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] =
    batch =>
      withEnrichedLoggingContext("updateOffsets" -> batch.offsets) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.parallelIndexer.ingestion) { connection =>
          metrics.daml.parallelIndexer.updates.inc(batch.batchSize.toLong)
          ingestFunction(connection, batch.batch)
          val nowNanos = System.nanoTime()
          metrics.daml.parallelIndexer.ingestion.duration.update(
            (nowNanos - batch.averageStartTime) / batch.batchSize,
            TimeUnit.NANOSECONDS,
          )
          batch
        }
      }

  def tailer[DB_BATCH](
      zeroDbBatch: DB_BATCH
  ): (Batch[DB_BATCH], Batch[DB_BATCH]) => Batch[DB_BATCH] =
    (_, curr) =>
      Batch[DB_BATCH](
        lastOffset = curr.lastOffset,
        lastSeqEventId = curr.lastSeqEventId,
        lastRecordTime = curr.lastRecordTime,
        batch = zeroDbBatch, // not used anymore
        batchSize = 0, // not used anymore
        averageStartTime = 0, // not used anymore
        offsets = Vector.empty, // not used anymore
      )

  def ledgerEndFrom(batch: Batch[_]): StorageBackend.LedgerEnd =
    StorageBackend.LedgerEnd(
      lastOffset = batch.lastOffset,
      lastEventSeqId = batch.lastSeqEventId,
    )

  def ingestTail[DB_BATCH](
      ingestTailFunction: StorageBackend.LedgerEnd => Connection => Unit,
      dbDispatcher: DbDispatcher,
      metrics: Metrics,
  )(implicit loggingContext: LoggingContext): Batch[DB_BATCH] => Future[Batch[DB_BATCH]] =
    batch =>
      withEnrichedLoggingContext("updateOffset" -> batch.lastOffset) { implicit loggingContext =>
        dbDispatcher.executeSql(metrics.daml.parallelIndexer.tailIngestion) { connection =>
          ingestTailFunction(ledgerEndFrom(batch))(connection)
          metrics.daml.indexer.ledgerEndSequentialId.updateValue(batch.lastSeqEventId)
          metrics.daml.indexer.lastReceivedRecordTime.updateValue(batch.lastRecordTime)
          metrics.daml.indexer.lastReceivedOffset.updateValue(batch.lastOffset.toHexString)
          logger.info("Ledger end updated")
          batch
        }
      }

  def toIndexer(ingestionPipeOn: ResourceContext => state.ReadService => Handle): Indexer =
    readService =>
      new ResourceOwner[IndexFeedHandle] {
        override def acquire()(implicit context: ResourceContext): Resource[IndexFeedHandle] = {
          Resource {
            Future {
              val handle = ingestionPipeOn(context)(readService)
              new SubscriptionIndexFeedHandle(handle.killSwitch, handle.completed)
            }
          } { handle =>
            handle.killSwitch.shutdown()
            handle.completed.recover { case NonFatal(_) =>
              ()
            }
          }
        }
      }

  class SubscriptionIndexFeedHandle(
      val killSwitch: KillSwitch,
      override val completed: Future[Unit],
  ) extends IndexFeedHandle
}
