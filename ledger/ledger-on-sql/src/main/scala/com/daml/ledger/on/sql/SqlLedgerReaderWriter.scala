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
import com.daml.ledger.on.sql.queries.Queries
import com.daml.ledger.on.sql.queries.Queries.Index
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
  DamlSubmission
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
import com.google.protobuf.ByteString
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import javax.sql.DataSource

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

class SqlLedgerReaderWriter(
    ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    val participantId: ParticipantId,
    queries: Queries,
    connectionSource: DataSource with AutoCloseable,
)(
    implicit executionContext: ExecutionContext,
    materializer: Materializer,
    loggingContext: LoggingContext,
) extends LedgerWriter
    with LedgerReader
    with AutoCloseable {

  private val logger = ContextualizedLogger.get(this.getClass)

  private val engine = Engine()

  private val dispatcher: Dispatcher[Index] =
    Dispatcher(
      "sql-participant-state",
      zeroIndex = FirstIndex,
      headAtInitialization = FirstIndex,
    )

  // TODO: implement
  override def currentHealth(): HealthStatus = Healthy

  override def close(): Unit = {
    dispatcher.close()
    connectionSource.close()
  }

  override def retrieveLedgerId(): LedgerId = ledgerId

  override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        offset.getOrElse(FirstOffset).components.head,
        RangeSource((start, end) =>
          withEnrichedLoggingContext("start" -> start, "end" -> end) { implicit loggingContext =>
            withDatabaseStream("Querying events from log") { implicit connection =>
              Source(queries.selectFromLog(start, end))
            }
        })
      )
      .map { case (_, record) => record }

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] =
    withEnrichedLoggingContext("correlationId" -> correlationId) { implicit loggingContext =>
      inDatabaseTransaction("Committing a submission") { implicit connection =>
        Future {
          val submission = Envelope
            .openSubmission(envelope)
            .getOrElse(throw new IllegalArgumentException("Not a valid submission in envelope"))
          val stateInputKeys: Set[DamlStateKey] = submission.getInputDamlStateList.asScala.toSet
          val stateInputs = readState(stateInputKeys)
          val entryId = queries.nextEntryId()
          val logEntryId =
            DamlLogEntryId
              .newBuilder()
              .setEntryId(ByteString.copyFromUtf8(entryId.toHexString))
              .build()
          val (logEntry, stateUpdates) = KeyValueCommitting.processSubmission(
            engine,
            logEntryId,
            currentRecordTime(),
            LedgerReader.DefaultConfiguration,
            submission,
            participantId,
            stateInputs,
          )
          verifyStateUpdatesAgainstPreDeclaredOutputs(stateUpdates, logEntryId, submission)
          queries.insertIntoLog(entryId, Envelope.enclose(logEntry))
          queries.updateState(stateUpdates)
          dispatcher.signalNewHead(entryId + 1)
          SubmissionResult.Acknowledged
        }
      }
    }

  private def verifyStateUpdatesAgainstPreDeclaredOutputs(
      actualStateUpdates: Map[DamlStateKey, DamlStateValue],
      entryId: DamlLogEntryId,
      submission: DamlSubmission,
  ): Unit = {
    val expectedStateUpdates = KeyValueCommitting.submissionOutputs(entryId, submission)
    if (!(actualStateUpdates.keySet subsetOf expectedStateUpdates)) {
      val unaccountedKeys = actualStateUpdates.keySet diff expectedStateUpdates
      sys.error(
        s"CommitActor: State updates not a subset of expected updates! Keys [$unaccountedKeys] are unaccounted for!")
    }
  }

  private def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

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

  private def migrate(): Future[Unit] =
    inDatabaseTransaction("Migrating the database") { implicit connection =>
      Future
        .sequence(
          Seq(
            Future(queries.createLogTable()),
            Future(queries.createStateTable()),
          ))
        .map(_ => ())
    }

  private def inDatabaseTransaction[T](message: String)(
      body: Connection => Future[T],
  ): Future[T] = {
    val connection =
      time(s"$message: acquiring connection", connectionSource.getConnection())(loggingContext)
    timeFuture(
      message,
      body(connection).transform(
        value => {
          connection.commit()
          connection.close()
          value
        },
        exception => {
          connection.rollback()
          connection.close()
          exception
        }
      ))(loggingContext)
  }

  private def withDatabaseStream[Out, Mat](message: String)(
      body: Connection => Source[Out, Mat],
  ): Source[Out, NotUsed] = {
    val connection =
      time(s"$message: acquiring connection", connectionSource.getConnection())(loggingContext)
    timeStream(
      message,
      body(connection)
        .mapMaterializedValue { _ =>
          connection.close()
          NotUsed
        })(loggingContext)
  }

  private def time[T](message: String, body: => T)(implicit loggingContext: LoggingContext): T = {
    val startTime = System.currentTimeMillis()
    logger.trace(s"$message: starting")
    val result = body
    logTimeTaken(message, startTime)(result)
  }

  private def timeFuture[T](
      message: String,
      body: => Future[T],
  )(implicit loggingContext: LoggingContext): Future[T] = {
    val startTime = System.currentTimeMillis()
    logger.trace(s"$message: starting")
    body.transform(logTimeTaken(message, startTime), logTimeTaken(message, startTime))
  }

  private def timeStream[Out, Mat](
      message: String,
      body: => Source[Out, Mat],
  )(implicit loggingContext: LoggingContext): Source[Out, Mat] = {
    val startTime = System.currentTimeMillis()
    logger.trace(s"$message: starting")
    body.mapMaterializedValue(logTimeTaken(message, startTime))
  }

  private def logTimeTaken[T](
      message: String,
      startTime: Index,
  )(result: T)(implicit loggingContext: LoggingContext): T = {
    val endTime = System.currentTimeMillis()
    withEnrichedLoggingContext("duration" -> (endTime - startTime)) { loggingContextWithTime =>
      logger.trace(s"$message: finished")(loggingContextWithTime)
    }(loggingContext)
    result
  }
}

object SqlLedgerReaderWriter {
  val FirstIndex: Index = 1

  private val FirstOffset: Offset = Offset(Array(FirstIndex))

  def apply(
      ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
      participantId: ParticipantId,
      jdbcUrl: String,
  )(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
      loggingContext: LoggingContext,
  ): Future[SqlLedgerReaderWriter] =
    Future {
      val database = Database(jdbcUrl)
      val connectionPool = newConnectionPool(jdbcUrl, database)
      new SqlLedgerReaderWriter(ledgerId, participantId, database.queries, connectionPool)
    }.flatMap { ledger =>
      ledger.migrate().map(_ => ledger)
    }

  private def newConnectionPool(
      jdbcUrl: String,
      database: Database,
  ): DataSource with AutoCloseable = {
    val connectionPoolConfig = new HikariConfig
    connectionPoolConfig.setJdbcUrl(jdbcUrl)
    connectionPoolConfig.setAutoCommit(false)
    database.maximumPoolSize.foreach { maximumPoolSize =>
      connectionPoolConfig.setMaximumPoolSize(maximumPoolSize)
    }
    new HikariDataSource(connectionPoolConfig)
  }
}
