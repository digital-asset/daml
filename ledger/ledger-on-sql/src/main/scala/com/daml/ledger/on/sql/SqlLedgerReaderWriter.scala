// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.time.Instant
import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.on.sql.SqlLedgerReaderWriter._
import com.daml.ledger.on.sql.queries.Queries
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.api.{LedgerEntry, LedgerReader, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.{Bytes, KVOffset}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.digitalasset.platform.common.LedgerIdMismatchException
import com.digitalasset.resources.{Resource, ResourceOwner}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class SqlLedgerReaderWriter(
    override val ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    val participantId: ParticipantId,
    metricRegistry: MetricRegistry,
    timeProvider: TimeProvider,
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
      .create(
        SqlLedgerStateAccess,
        allocateNextLogEntryId = () => allocateSeededLogEntryId(),
        metricRegistry = metricRegistry,
      ),
    latestSequenceNo => dispatcher.signalNewHead(latestSequenceNo),
  )

  override def currentHealth(): HealthStatus = Healthy

  override def events(startExclusive: Option[Offset]): Source[LedgerEntry, NotUsed] =
    dispatcher
      .startingAt(
        KVOffset.highestIndex(startExclusive.getOrElse(StartOffset)),
        RangeSource((start, end) => {
          Source
            .future(database
              .inReadTransaction(s"Querying events ]$start, $end] from log") { queries =>
                Future.fromTry(queries.selectFromLog(start, end))
              })
            .mapConcat(identity)
            .mapMaterializedValue(_ => NotUsed)
        }),
      )
      .map { case (_, entry) => entry }

  override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] =
    committer.commit(correlationId, envelope, participantId)

  object SqlLedgerStateAccess extends LedgerStateAccess[Index] {
    override def inTransaction[T](body: LedgerStateOperations[Index] => Future[T]): Future[T] =
      database.inWriteTransaction("Committing a submission") { queries =>
        body(new SqlLedgerStateOperations(queries))
      }
  }

  class SqlLedgerStateOperations(queries: Queries) extends BatchingLedgerStateOperations[Index] {
    override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
      Future.fromTry(queries.selectStateValuesByKeys(keys))

    override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
      Future.fromTry(queries.updateState(keyValuePairs))

    override def appendToLog(key: Key, value: Value): Future[Index] =
      Future.fromTry(queries.insertRecordIntoLog(key, value))
  }
}

object SqlLedgerReaderWriter {
  private val logger = ContextualizedLogger.get(classOf[SqlLedgerReaderWriter])

  private val StartOffset: Offset = KVOffset.fromLong(StartIndex)

  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  class Owner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      metricRegistry: MetricRegistry,
      jdbcUrl: String,
      timeProvider: TimeProvider = DefaultTimeProvider,
      heartbeats: Source[Instant, NotUsed] = Source.empty,
      seedService: SeedService,
  )(implicit materializer: Materializer, logCtx: LoggingContext)
      extends ResourceOwner[SqlLedgerReaderWriter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[SqlLedgerReaderWriter] =
      for {
        uninitializedDatabase <- Database.owner(jdbcUrl).acquire()
        database = uninitializedDatabase.migrate()
        ledgerId <- Resource.fromFuture(updateOrRetrieveLedgerId(initialLedgerId, database))
        dispatcher <- ResourceOwner.forFutureCloseable(() => newDispatcher(database)).acquire()
        _ = publishHeartbeats(database, dispatcher, heartbeats)
      } yield
        new SqlLedgerReaderWriter(
          ledgerId,
          participantId,
          metricRegistry,
          timeProvider,
          database,
          dispatcher,
          seedService,
        )
  }

  private def updateOrRetrieveLedgerId(initialLedgerId: Option[LedgerId], database: Database)(
      implicit executionContext: ExecutionContext,
      logCtx: LoggingContext,
  ): Future[LedgerId] =
    database.inWriteTransaction("Checking ledger ID at startup") { queries =>
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
      .inReadTransaction("Reading head at startup") { queries =>
        Future.fromTry(queries.selectLatestLogEntryId().map(_.map(_ + 1).getOrElse(StartIndex)))
      }
      .map(head => Dispatcher("sql-participant-state", StartIndex, head))

  private def publishHeartbeats(
      database: Database,
      dispatcher: Dispatcher[Index],
      heartbeats: Source[Instant, NotUsed],
  )(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
      logCtx: LoggingContext,
  ): Future[Unit] =
    heartbeats
      .runWith(
        Sink.foreach(timestamp =>
          database
            .inWriteTransaction("Publishing heartbeat") { queries =>
              Future.fromTry(queries.insertHeartbeatIntoLog(timestamp))
            }
            .onComplete {
              case Success(latestSequenceNo) => dispatcher.signalNewHead(latestSequenceNo)
              case Failure(exception) => logger.error("Publishing heartbeat failed.", exception)
          }))
      .map(_ => ())
}
