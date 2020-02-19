// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection
import java.time.{Clock, Instant}
import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.on.sql.SqlLedgerReaderWriter._
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.{
  BatchingLedgerStateOperations,
  LedgerStateAccess,
  LedgerStateOperations,
  SubmissionValidator,
  ValidatingCommitter
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.digitalasset.resources.ResourceOwner

import scala.collection.immutable.TreeSet
import scala.concurrent.{ExecutionContext, Future}

class SqlLedgerReaderWriter(
    override val ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    val participantId: ParticipantId,
    now: () => Instant,
    database: Database,
    dispatcher: Dispatcher[Index],
)(
    implicit executionContext: ExecutionContext,
    materializer: Materializer,
    logCtx: LoggingContext,
) extends LedgerWriter
    with LedgerReader {

  private val queries = database.queries

  private val committer = new ValidatingCommitter[Index](
    participantId,
    now,
    SubmissionValidator.create(SqlLedgerStateAccess),
    latestSequenceNo => dispatcher.signalNewHead(latestSequenceNo + 1),
  )

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
    committer.commit(correlationId, envelope)

  object SqlLedgerStateAccess extends LedgerStateAccess[Index] {
    override def inTransaction[T](body: LedgerStateOperations[Index] => Future[T]): Future[T] =
      database.inWriteTransaction("Committing a submission") { implicit connection =>
        body(new SqlLedgerStateOperations)
      }
  }

  class SqlLedgerStateOperations(implicit connection: Connection)
      extends BatchingLedgerStateOperations[Index] {
    override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
      Future.successful(queries.selectStateValuesByKeys(keys))

    override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
      Future.successful(queries.updateState(keyValuePairs))

    override def appendToLog(key: Key, value: Value): Future[Index] =
      Future.successful(queries.insertIntoLog(key, value))
  }
}

object SqlLedgerReaderWriter {
  private val StartOffset: Offset = Offset(Array(StartIndex))

  private val DefaultClock: Clock = Clock.systemUTC()

  def owner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      jdbcUrl: String,
      now: () => Instant = () => DefaultClock.instant(),
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
    } yield {
      val ledgerId =
        initialLedgerId.getOrElse(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))
      new SqlLedgerReaderWriter(ledgerId, participantId, now, database, dispatcher)
    }
}
