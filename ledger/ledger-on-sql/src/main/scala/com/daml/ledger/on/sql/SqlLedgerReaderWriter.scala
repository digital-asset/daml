// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection
import java.time.Clock
import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.on.sql.SqlLedgerReaderWriter._
import com.daml.ledger.on.sql.queries.Queries.Index
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.logging.LoggingContext.withEnrichedLoggingContext
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.digitalasset.resources.ResourceOwner
import com.google.protobuf.ByteString
import javax.sql.DataSource

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeSet
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class SqlLedgerReaderWriter(
    ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    val participantId: ParticipantId,
    database: Database,
    dispatcher: Dispatcher[Index],
)(
    implicit executionContext: ExecutionContext,
    materializer: Materializer,
    logCtx: LoggingContext,
) extends LedgerWriter
    with LedgerReader {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val engine = Engine()

  private val queries = database.queries

  // TODO: implement
  override def currentHealth(): HealthStatus = Healthy

  override def retrieveLedgerId(): LedgerId = ledgerId

  override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        offset.getOrElse(StartOffset).components.head,
        RangeSource((start, end) => {
          val result = inDatabaseReadTransaction(s"Querying events [$start, $end[ from log") {
            implicit connection =>
              queries.selectFromLog(start, end)
          }
          if (result.length < end - start) {
            val missing = TreeSet(start until end: _*) -- result.map(_._1)
            Source.failed(new IllegalStateException(s"Missing entries: ${missing.mkString(", ")}"))
          } else {
            Source(result)
          }
        }),
      )
      .map { case (_, record) => record }

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] =
    withEnrichedLoggingContext("correlationId" -> correlationId) { implicit logCtx =>
      Future {
        val submission = Envelope
          .openSubmission(envelope)
          .getOrElse(throw new IllegalArgumentException("Not a valid submission in envelope"))
        val stateInputKeys = submission.getInputDamlStateList.asScala.toSet
        val entryId = allocateEntryId()
        val newHead = inDatabaseWriteTransaction("Committing a submission") { implicit connection =>
          val stateInputs = readState(stateInputKeys)
          val (logEntry, stateUpdates) =
            KeyValueCommitting.processSubmission(
              engine,
              entryId,
              currentRecordTime(),
              LedgerReader.DefaultConfiguration,
              submission,
              participantId,
              stateInputs,
            )
          queries.updateState(stateUpdates)
          val latestSequenceNo =
            queries.insertIntoLog(entryId, Envelope.enclose(logEntry))
          latestSequenceNo + 1
        }
        dispatcher.signalNewHead(newHead)
        SubmissionResult.Acknowledged
      }
    }

  private def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  private def allocateEntryId(): DamlLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFromUtf8(UUID.randomUUID().toString))
      .build()

  private def readState(
      stateInputKeys: Set[DamlStateKey],
  )(implicit connection: Connection): Map[DamlStateKey, Option[DamlStateValue]] = {
    val builder = Map.newBuilder[DamlStateKey, Option[DamlStateValue]]
    builder ++= stateInputKeys.map(_ -> None)
    queries
      .selectStateByKeys(stateInputKeys)
      .foldLeft(builder)(_ += _)
      .result()
  }

  private def inDatabaseReadTransaction[T](message: String)(
      body: Connection => T,
  )(implicit logCtx: LoggingContext): T = {
    inDatabaseTransaction(message, database.readerConnectionPool)(body)
  }

  private def inDatabaseWriteTransaction[T](message: String)(
      body: Connection => T,
  )(implicit logCtx: LoggingContext): T = {
    inDatabaseTransaction(message, database.writerConnectionPool)(body)
  }

  private def inDatabaseTransaction[T](
      message: String,
      connectionPool: DataSource,
  )(
      body: Connection => T,
  )(implicit logCtx: LoggingContext): T = {
    val connection =
      time(s"$message: acquiring connection")(connectionPool.getConnection())
    time(message) {
      try {
        val result = body(connection)
        connection.commit()
        result
      } catch {
        case NonFatal(exception) =>
          connection.rollback()
          throw exception
      } finally {
        connection.close()
      }
    }
  }

  private def time[T](message: String)(body: => T)(implicit logCtx: LoggingContext): T = {
    val startTime = System.nanoTime()
    logger.trace(s"$message: starting")
    val result = body
    val endTime = System.nanoTime()
    logger.trace(s"$message: finished in ${Duration.fromNanos(endTime - startTime).toMillis}ms")
    result
  }
}

object SqlLedgerReaderWriter {
  val StartIndex: Index = 1

  private val StartOffset: Offset = Offset(Array(StartIndex))

  def owner(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      jdbcUrl: String,
  )(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
      logCtx: LoggingContext,
  ): ResourceOwner[SqlLedgerReaderWriter] =
    for {
      dispatcher <- ResourceOwner.forCloseable(() =>
        Dispatcher(
          "sql-participant-state",
          zeroIndex = StartIndex,
          headAtInitialization = StartIndex,
        ),
      )
      uninitializedDatabase <- Database.owner(jdbcUrl)
    } yield {
      val database = uninitializedDatabase.migrate()
      new SqlLedgerReaderWriter(ledgerId, participantId, database, dispatcher)
    }
}
