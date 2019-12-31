// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.sql.Connection
import java.time.Clock
import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import anorm.SqlParser._
import anorm._
import com.daml.ledger.on.sql.SqlLedgerReaderWriter._
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
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.google.protobuf.ByteString
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import javax.sql.DataSource

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

class SqlLedgerReaderWriter(
    ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    val participantId: ParticipantId,
    connectionSource: DataSource with AutoCloseable,
)(implicit executionContext: ExecutionContext, materializer: Materializer)
    extends LedgerWriter
    with LedgerReader
    with AutoCloseable {

  private val engine = Engine()

  private val dispatcher: Dispatcher[Index] =
    Dispatcher(
      "posix-filesystem-participant-state",
      zeroIndex = StartIndex,
      headAtInitialization = StartIndex,
    )

  private val randomNumberGenerator = new Random()

  // TODO: implement
  override def currentHealth(): HealthStatus = Healthy

  override def close(): Unit = {
    dispatcher.close()
    connectionSource.close()
  }

  override def retrieveLedgerId(): LedgerId = ledgerId

  override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] = withConnection {
    implicit connection =>
      val startIndex = offset.getOrElse(StartOffset).components.head
      AkkaStream
        .source(
          SQL"SELECT sequence_no, entry_id, envelope FROM log WHERE sequence_no >= $startIndex",
          (int("sequence_no") ~ byteArray("entry_id") ~ byteArray("envelope")).map {
            case index ~ entryId ~ envelope =>
              LedgerRecord(
                Offset(Array(index.toLong)),
                DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFrom(entryId)).build(),
                envelope)
          }
        )
  }

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] =
    withConnection { implicit connection =>
      Future {
        val submission = Envelope
          .openSubmission(envelope)
          .getOrElse(throw new IllegalArgumentException("Not a valid submission in envelope"))
        val stateInputKeys: Set[DamlStateKey] = submission.getInputDamlStateList.asScala.toSet
        val stateInputs = readState(stateInputKeys)
        val entryId = allocateEntryId()
        val (logEntry, stateUpdates) = KeyValueCommitting.processSubmission(
          engine,
          entryId,
          currentRecordTime(),
          LedgerReader.DefaultConfiguration,
          submission,
          participantId,
          stateInputs,
        )
        verifyStateUpdatesAgainstPreDeclaredOutputs(stateUpdates, entryId, submission)
        val newHead = appendLog(entryId, Envelope.enclose(logEntry))
        updateState(stateUpdates)
        dispatcher.signalNewHead(newHead)
        SubmissionResult.Acknowledged
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

  private def allocateEntryId(): DamlLogEntryId = {
    val nonce: Array[Byte] = Array.ofDim(8)
    randomNumberGenerator.nextBytes(nonce)
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFrom(nonce))
      .build
  }

  private def appendLog(
      entry: DamlLogEntryId,
      envelope: ByteString,
  )(implicit connection: Connection): Index = {
    val maxIndex =
      SQL"SELECT MAX(sequence_no) max_sequence_no FROM log"
        .as(get[Option[Int]]("max_sequence_no").single)
    val currentHead = maxIndex.map(_ + 1).getOrElse(StartIndex)
    SQL"INSERT INTO log VALUES ($currentHead, ${entry.getEntryId.toByteArray}, ${envelope.toByteArray})"
      .executeInsert()
    currentHead + 1
  }

  private def readState(
      stateInputKeys: Set[DamlStateKey],
  )(implicit connection: Connection): Map[DamlStateKey, Option[DamlStateValue]] = {
    val builder = Map.newBuilder[DamlStateKey, Option[DamlStateValue]]
    builder ++= stateInputKeys.map(_ -> None)
    SQL"SELECT key, value FROM state WHERE key IN (${stateInputKeys.map(_.toByteArray)})"
      .as((byteArray("key") ~ byteArray("value")).map {
        case key ~ value =>
          DamlStateKey.parseFrom(key) -> Some(DamlStateValue.parseFrom(value))
      }.*)
      .foldLeft(builder)(_ += _)
      .result()
  }

  private def updateState(
      stateChanges: Map[DamlStateKey, DamlStateValue],
  )(implicit connection: Connection): Unit = {
    val existingKeys =
      SQL"SELECT key FROM state WHERE key IN (${stateChanges.keys.map(_.toByteArray).toSeq})"
        .as(byteArray("key").map(DamlStateKey.parseFrom).*)
        .toSet
    val (stateUpdates, stateInserts) = stateChanges.partition {
      case (key, _) => existingKeys.contains(key)
    }
    executeBatchSql("INSERT INTO state VALUES ({key}, {value})", asNamedParameters(stateInserts))
    executeBatchSql(
      "UPDATE state SET value = {value} WHERE key = {key}",
      asNamedParameters(stateUpdates))
  }

  private def asNamedParameters(
      stateUpdates: Map[DamlStateKey, DamlStateValue],
  ): Iterable[Seq[NamedParameter]] = {
    stateUpdates.map {
      case (key, value) =>
        Seq[NamedParameter]("key" -> key.toByteArray, "value" -> value.toByteArray)
    }
  }

  private def migrate(): Future[Unit] = withConnection { implicit connection =>
    Future
      .sequence(
        Seq(
          Future(
            SQL"CREATE TABLE IF NOT EXISTS log (sequence_no INT PRIMARY KEY, entry_id VARBINARY(16384), envelope BLOB)"
              .execute()),
          Future(
            SQL"CREATE TABLE IF NOT EXISTS state (key VARBINARY(16384) PRIMARY KEY, value BLOB)"
              .execute()),
        ))
      .map(_ => ())
  }

  private def withConnection[T](body: Connection => Future[T]): Future[T] = {
    val connection = connectionSource.getConnection()
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
    )
  }

  private def withConnection[Out, Mat](
      body: Connection => Source[Out, Future[Mat]],
  ): Source[Out, NotUsed] = {
    val connection = connectionSource.getConnection()
    body(connection).mapMaterializedValue(mat => {
      mat.onComplete { _ =>
        connection.close()
      }
      NotUsed
    })
  }

  private def executeBatchSql(
      query: String,
      params: Iterable[Seq[NamedParameter]],
  )(implicit connection: Connection): Unit = {
    if (params.nonEmpty)
      BatchSql(query, params.head, params.drop(1).toArray: _*).execute()
    ()
  }
}

object SqlLedgerReaderWriter {
  type Index = Int

  private val StartIndex: Index = 0

  private val StartOffset: Offset = Offset(Array(StartIndex.toLong))

  def apply(
      ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
      participantId: ParticipantId,
      jdbcUrl: String,
  )(
      implicit executionContext: ExecutionContext,
      materializer: Materializer,
  ): Future[SqlLedgerReaderWriter] = {
    val connectionPoolConfig = new HikariConfig
    connectionPoolConfig.setJdbcUrl(jdbcUrl)
    connectionPoolConfig.setAutoCommit(false)
    val connectionPool = new HikariDataSource(connectionPoolConfig)
    val ledger = new SqlLedgerReaderWriter(ledgerId, participantId, connectionPool)
    ledger.migrate().map(_ => ledger)
  }

  class SqlException private (message: String) extends RuntimeException(message)

  object SqlException {
    def apply[T](message: String, query: SimpleSql[T]) =
      new SqlException(s"$message\nQuery:\n$query")
  }
}
