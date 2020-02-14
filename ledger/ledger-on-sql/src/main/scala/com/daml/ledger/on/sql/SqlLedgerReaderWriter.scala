// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection
import java.time.Clock
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.on.sql.SqlLedgerReaderWriter._
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.ValidationResult.{
  MissingInputState,
  SubmissionValidated,
  ValidationError
}
import com.daml.ledger.validator.{
  BatchingLedgerStateOperations,
  LedgerStateAccess,
  LedgerStateOperations,
  SubmissionValidator
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.logging.LoggingContext.withEnrichedLoggingContext
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.digitalasset.resources.ResourceOwner

import scala.collection.immutable.TreeSet
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

class SqlLedgerReaderWriter(
    override val ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    val participantId: ParticipantId,
    database: Database,
    dispatcher: Dispatcher[Index],
)(
    implicit executionContext: ExecutionContext,
    materializer: Materializer,
    logCtx: LoggingContext,
) extends LedgerWriter
    with LedgerReader {

  private val queries = database.queries

  private val validator = SubmissionValidator.create(SqlLedgerStateAccess)

  private val head = new AtomicLong(dispatcher.getHead())

  // TODO: implement
  override def currentHealth(): HealthStatus = Healthy

  override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        offset.getOrElse(StartOffset).components.head,
        RangeSource((start, end) => {
          Source
            .futureSource(database
              .inReadTransaction(s"Querying events [$start, $end[ from log") {
                implicit connection =>
                  Future.successful(queries.selectFromLog(start, end))
              }
              .map { result =>
                if (result.length < end - start) {
                  val missing = TreeSet(start until end: _*) -- result.map(_._1)
                  Source.failed(
                    new IllegalStateException(s"Missing entries: ${missing.mkString(", ")}"))
                } else {
                  Source(result)
                }
              })
            .mapMaterializedValue(_ => NotUsed)
        }),
      )
      .map { case (_, record) => record }

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] =
    withEnrichedLoggingContext("correlationId" -> correlationId) { implicit logCtx =>
      validator
        .validateAndCommit(envelope, correlationId, currentRecordTime(), participantId)
        .map {
          case SubmissionValidated =>
            SubmissionResult.Acknowledged
          case MissingInputState(keys) =>
            SubmissionResult.InternalError(
              s"Missing input state: ${keys.map(_.map("%02x".format(_)).mkString).mkString(", ")}")
          case ValidationError(reason) =>
            SubmissionResult.InternalError(reason)
        }
    }

  private def currentRecordTime(): Timestamp =
    Timestamp.assertFromInstant(Clock.systemUTC().instant())

  object SqlLedgerStateAccess extends LedgerStateAccess {
    override def inTransaction[T](body: LedgerStateOperations => Future[T]): Future[T] =
      database
        .inWriteTransaction("Committing a submission") { implicit connection =>
          body(new SqlLedgerStateOperations)
        }
        .andThen {
          case Success(_) =>
            dispatcher.signalNewHead(head.get())
        }
  }

  class SqlLedgerStateOperations(implicit connection: Connection)
      extends BatchingLedgerStateOperations {
    override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
      Future.successful(queries.selectStateValuesByKeys(keys))

    override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
      Future.successful(queries.updateState(keyValuePairs))

    override def appendToLog(key: Key, value: Value): Future[Unit] = {
      val latestSequenceNo = queries.insertIntoLog(key, value)
      head.set(latestSequenceNo + 1)
      Future.unit
    }
  }
}

object SqlLedgerReaderWriter {
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
      uninitializedDatabase <- Database.owner(jdbcUrl)
      database = uninitializedDatabase.migrate()
      head <- ResourceOwner.forFuture(() =>
        database.inReadTransaction("Reading head at startup") { implicit connection =>
          Future(database.queries.selectLatestLogEntryId().map(_ + 1).getOrElse(StartIndex))
      })
      dispatcher <- ResourceOwner.forCloseable(
        () =>
          Dispatcher(
            "sql-participant-state",
            zeroIndex = StartIndex,
            headAtInitialization = head,
        ))
    } yield new SqlLedgerReaderWriter(ledgerId, participantId, database, dispatcher)
}
