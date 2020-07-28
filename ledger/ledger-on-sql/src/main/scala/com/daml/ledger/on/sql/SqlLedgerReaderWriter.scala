// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.ledger.api.domain
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.on.sql.SqlLedgerReaderWriter._
import com.daml.ledger.on.sql.queries.Queries
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlLogEntryId, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.{Bytes, KVOffset}
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
    engine: Engine,
    metrics: Metrics,
    timeProvider: TimeProvider,
    stateValueCache: Cache[Bytes, DamlStateValue],
    database: Database,
    dispatcher: Dispatcher[Index],
    seedService: SeedService
)(
    implicit executionContext: ExecutionContext,
    materializer: Materializer,
    logCtx: LoggingContext,
) extends LedgerWriter
    with LedgerReader {

  private def allocateSeededLogEntryId(): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(
        ByteString.copyFromUtf8(
          UUID.nameUUIDFromBytes(seedService.nextSeed().bytes.toByteArray).toString))
      .build()

  private val committer = new ValidatingCommitter[Index](
    () => timeProvider.getCurrentTime,
    SubmissionValidator
      .createForTimeMode(
        SqlLedgerStateAccess,
        allocateNextLogEntryId = () => allocateSeededLogEntryId(),
        stateValueCache = stateValueCache,
        engine = engine,
        metrics = metrics,
        inStaticTimeMode = timeProvider != TimeProvider.UTC,
      ),
    latestSequenceNo => dispatcher.signalNewHead(latestSequenceNo),
  )

  override def currentHealth(): HealthStatus = Healthy

  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        KVOffset.highestIndex(startExclusive.getOrElse(StartOffset)),
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

  override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] =
    committer.commit(correlationId, envelope, participantId)

  private object SqlLedgerStateAccess extends LedgerStateAccess[Index] {
    override def inTransaction[T](body: LedgerStateOperations[Index] => Future[T]): Future[T] =
      database.inWriteTransaction("commit") { queries =>
        body(new TimedLedgerStateOperations(new SqlLedgerStateOperations(queries), metrics))
      }
  }

  private final class SqlLedgerStateOperations(queries: Queries)
      extends BatchingLedgerStateOperations[Index] {
    override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
      Future.fromTry(queries.selectStateValuesByKeys(keys))

    override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
      Future.fromTry(queries.updateState(keyValuePairs))

    override def appendToLog(key: Key, value: Value): Future[Index] =
      Future.fromTry(queries.insertRecordIntoLog(key, value))
  }
}

object SqlLedgerReaderWriter {
  private val StartOffset: Offset = KVOffset.fromLong(StartIndex)

  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  final class Owner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      metrics: Metrics,
      engine: Engine,
      jdbcUrl: String,
      resetOnStartup: Boolean,
      stateValueCache: Cache[Bytes, DamlStateValue] = Cache.none,
      timeProvider: TimeProvider = DefaultTimeProvider,
      seedService: SeedService,
  )(implicit materializer: Materializer, logCtx: LoggingContext)
      extends ResourceOwner[SqlLedgerReaderWriter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[SqlLedgerReaderWriter] =
      for {
        uninitializedDatabase <- Database.owner(jdbcUrl, metrics).acquire()
        database <- Resource.fromFuture(
          if (resetOnStartup) uninitializedDatabase.migrateAndReset()
          else Future.successful(uninitializedDatabase.migrate()))
        ledgerId <- Resource.fromFuture(updateOrRetrieveLedgerId(initialLedgerId, database))
        dispatcher <- ResourceOwner.forFutureCloseable(() => newDispatcher(database)).acquire()
      } yield
        new SqlLedgerReaderWriter(
          ledgerId,
          participantId,
          engine,
          metrics,
          timeProvider,
          stateValueCache,
          database,
          dispatcher,
          seedService,
        )
  }

  private def updateOrRetrieveLedgerId(initialLedgerId: Option[LedgerId], database: Database)(
      implicit executionContext: ExecutionContext,
      logCtx: LoggingContext,
  ): Future[LedgerId] =
    database.inWriteTransaction("retrieve_ledger_id") { queries =>
      val providedLedgerId =
        initialLedgerId.getOrElse(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))
      Future.fromTry(
        queries
          .updateOrRetrieveLedgerId(providedLedgerId)
          .flatMap { ledgerId =>
            if (initialLedgerId.exists(_ != ledgerId)) {
              Failure(
                new LedgerIdMismatchException(
                  domain.LedgerId(ledgerId),
                  domain.LedgerId(initialLedgerId.get),
                ))
            } else {
              Success(ledgerId)
            }
          })
    }

  private def newDispatcher(database: Database)(
      implicit executionContext: ExecutionContext,
      logCtx: LoggingContext,
  ): Future[Dispatcher[Index]] =
    database
      .inReadTransaction("read_head") { queries =>
        Future.fromTry(queries.selectLatestLogEntryId().map(_.map(_ + 1).getOrElse(StartIndex)))
      }
      .map(head => Dispatcher("sql-participant-state", StartIndex, head))
}
