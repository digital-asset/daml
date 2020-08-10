// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.util.UUID
import java.util.concurrent.Executors

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.ledger.api.domain
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.on.sql.SqlLedgerReaderWriter._
import com.daml.ledger.on.sql.queries.Queries
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlLogEntryId, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.api.{
  CommitMetadata,
  LedgerReader,
  LedgerRecord,
  LedgerWriter
}
import com.daml.ledger.participant.state.kvutils.{Bytes, OffsetBuilder}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator._
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.common.LedgerIdMismatchException
import com.daml.resources.{Resource, ResourceOwner}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class SqlLedgerReaderWriter(
    override val ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    val participantId: ParticipantId,
    metrics: Metrics,
    database: Database,
    dispatcher: Dispatcher[Index],
    committer: ValidatingCommitter[Index],
    committerExecutionContext: ExecutionContext,
) extends LedgerWriter
    with LedgerReader {

  override def currentHealth(): HealthStatus = Healthy

  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        OffsetBuilder.highestIndex(startExclusive.getOrElse(StartOffset)),
        RangeSource(
          (startExclusive, endInclusive) =>
            Source
              .future(
                Timed.value(metrics.daml.ledger.log.read, database.inReadTransaction("read_log") {
                  queries =>
                    Future.fromTry(queries.selectFromLog(startExclusive, endInclusive))
                }))
              .mapConcat(identity)
              .mapMaterializedValue(_ => NotUsed)),
      )
      .map { case (_, entry) => entry }

  override def commit(
      correlationId: String,
      envelope: Bytes,
      metadata: CommitMetadata,
  ): Future[SubmissionResult] =
    committer.commit(correlationId, envelope, participantId)(committerExecutionContext)
}

object SqlLedgerReaderWriter {
  private val StartOffset: Offset = OffsetBuilder.fromLong(StartIndex)

  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  final class Owner(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      metrics: Metrics,
      engine: Engine,
      jdbcUrl: String,
      resetOnStartup: Boolean,
      stateValueCache: Cache[Bytes, DamlStateValue] = Cache.none,
      timeProvider: TimeProvider = DefaultTimeProvider,
      seedService: SeedService,
  )(implicit loggingContext: LoggingContext)
      extends ResourceOwner[SqlLedgerReaderWriter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[SqlLedgerReaderWriter] =
      for {
        uninitializedDatabase <- Database.owner(jdbcUrl, metrics).acquire()
        database <- Resource.fromFuture(
          if (resetOnStartup) uninitializedDatabase.migrateAndReset()
          else Future.successful(uninitializedDatabase.migrate()))
        ledgerId <- Resource.fromFuture(updateOrRetrieveLedgerId(ledgerId, database))
        dispatcher <- new DispatcherOwner(database).acquire()
        validator = SubmissionValidator.createForTimeMode(
          new SqlLedgerStateAccess(database, metrics),
          allocateNextLogEntryId = new LogEntryIdAllocator(seedService).allocate _,
          stateValueCache = stateValueCache,
          engine = engine,
          metrics = metrics,
          inStaticTimeMode = timeProvider != TimeProvider.UTC,
        )
        committer = new ValidatingCommitter[Index](
          () => timeProvider.getCurrentTime,
          validator = validator,
          postCommit = dispatcher.signalNewHead,
        )
        committerExecutionContext <- ResourceOwner
          .forExecutorService(() =>
            ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor()))
          .acquire()
      } yield
        new SqlLedgerReaderWriter(
          ledgerId,
          participantId,
          metrics,
          database,
          dispatcher,
          committer,
          committerExecutionContext,
        )
  }

  private def updateOrRetrieveLedgerId(
      providedLedgerId: LedgerId,
      database: Database,
  ): Future[LedgerId] =
    database.inWriteTransaction("retrieve_ledger_id") { queries =>
      Future.fromTry(
        queries
          .updateOrRetrieveLedgerId(providedLedgerId)
          .flatMap { ledgerId =>
            if (providedLedgerId != ledgerId) {
              Failure(
                new LedgerIdMismatchException(
                  domain.LedgerId(ledgerId),
                  domain.LedgerId(providedLedgerId),
                ))
            } else {
              Success(ledgerId)
            }
          })
    }

  private final class DispatcherOwner(database: Database) extends ResourceOwner[Dispatcher[Index]] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[Dispatcher[Index]] =
      for {
        head <- Resource.fromFuture(
          database
            .inReadTransaction("read_head") { queries =>
              Future.fromTry(
                queries.selectLatestLogEntryId().map(_.map(_ + 1).getOrElse(StartIndex)))
            })
        dispatcher <- Dispatcher
          .owner(
            name = "sql-participant-state",
            zeroIndex = StartIndex,
            headAtInitialization = head,
          )
          .acquire()
      } yield dispatcher
  }

  private final class LogEntryIdAllocator(seedService: SeedService) {
    def allocate(): DamlLogEntryId = {
      val seed = seedService.nextSeed().bytes.toByteArray
      DamlLogEntryId.newBuilder
        .setEntryId(ByteString.copyFromUtf8(UUID.nameUUIDFromBytes(seed).toString))
        .build()
    }
  }

  private final class SqlLedgerStateAccess(database: Database, metrics: Metrics)
      extends LedgerStateAccess[Index] {
    override def inTransaction[T](body: LedgerStateOperations[Index] => Future[T])(
        implicit executionContext: ExecutionContext
    ): Future[T] =
      database.inWriteTransaction("commit") { queries =>
        body(new TimedLedgerStateOperations(new SqlLedgerStateOperations(queries), metrics))
      }
  }

  private final class SqlLedgerStateOperations(queries: Queries)
      extends BatchingLedgerStateOperations[Index] {
    override def readState(keys: Seq[Key])(
        implicit executionContext: ExecutionContext
    ): Future[Seq[Option[Value]]] =
      Future.fromTry(queries.selectStateValuesByKeys(keys))

    override def writeState(keyValuePairs: Seq[(Key, Value)])(
        implicit executionContext: ExecutionContext
    ): Future[Unit] =
      Future.fromTry(queries.updateState(keyValuePairs))

    override def appendToLog(key: Key, value: Value)(
        implicit executionContext: ExecutionContext
    ): Future[Index] =
      Future.fromTry(queries.insertRecordIntoLog(key, value))
  }

}
