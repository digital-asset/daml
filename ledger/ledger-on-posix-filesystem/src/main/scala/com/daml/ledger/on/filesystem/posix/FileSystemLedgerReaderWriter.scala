// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{Files, NoSuchFileException, Path}
import java.time.Clock
import java.util.UUID
import java.util.concurrent.Semaphore

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.on.filesystem.posix.FileSystemLedgerReaderWriter._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntryId,
  DamlStateKey,
  DamlStateValue,
  DamlSubmission
}
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.{Envelope, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.{LedgerId, Offset, ParticipantId, SubmissionResult}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

class FileSystemLedgerReaderWriter private (
    ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
    override val participantId: ParticipantId,
    logDirectory: Path,
    logIndexDirectory: Path,
    logEntriesDirectory: Path,
    stateDirectory: Path,
)(implicit executionContext: ExecutionContext)
    extends LedgerReader
    with LedgerWriter
    with AutoCloseable {

  private val lock = new Semaphore(1)

  private val logHeadPath: Path = logDirectory.resolve("head")

  private val engine = Engine()

  private val dispatcher: Dispatcher[Index] =
    Dispatcher(
      "posix-filesystem-participant-state",
      zeroIndex = StartOffset,
      headAtInitialization = StartOffset,
    )

  private val randomNumberGenerator = new Random()

  override def currentHealth(): HealthStatus = Healthy

  override def close(): Unit = {
    dispatcher.close()
  }

  override def retrieveLedgerId(): LedgerId = ledgerId

  override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        offset
          .map(_.components.head.toInt)
          .getOrElse(StartOffset),
        OneAfterAnother[Index, immutable.Seq[LedgerRecord]](
          (index: Index, _) => index + 1,
          (index: Index) => retrieveLogEntry(index).map(immutable.Seq(_))
        )
      )
      .mapConcat {
        case (_, updates) => updates
      }

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] =
    locked {
      val submission = Envelope
        .openSubmission(envelope)
        .getOrElse(throw new IllegalArgumentException("Not a valid submission in envelope"))
      for {
        stateInputStream <- Future.sequence(
          submission.getInputDamlStateList.asScala.toVector
            .map(key => readState(key).map(key -> _)))
        stateInputs: Map[DamlStateKey, Option[DamlStateValue]] = stateInputStream.toMap
        entryId = allocateEntryId()
        (logEntry, stateUpdates) = KeyValueCommitting.processSubmission(
          engine,
          entryId,
          currentRecordTime(),
          LedgerReader.DefaultConfiguration,
          submission,
          participantId,
          stateInputs,
        )
        _ = verifyStateUpdatesAgainstPreDeclaredOutputs(stateUpdates, entryId, submission)
        newHead <- appendLog(entryId, Envelope.enclose(logEntry))
        _ <- updateState(stateUpdates)
      } yield {
        dispatcher.signalNewHead(newHead)
        SubmissionResult.Acknowledged
      }
    }

  private def verifyStateUpdatesAgainstPreDeclaredOutputs(
      actualStateUpdates: Map[DamlStateKey, DamlStateValue],
      entryId: DamlLogEntryId,
      submission: DamlSubmission
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

  private def retrieveLogEntry(index: Index): Future[LedgerRecord] =
    for {
      entryId <- Future(Files.readAllBytes(logIndexDirectory.resolve(index.toString))).transform {
        case Success(value) => Success(value)
        case Failure(exception) if exception.isInstanceOf[NoSuchFileException] =>
          Failure(new NoSuchElementException(s"No log entry at $index."))
        case Failure(exception) => Failure(exception)
      }
      envelope <- Future(Files.readAllBytes(logEntriesDirectory.resolve(Bytes.toString(entryId))))
    } yield
      LedgerRecord(
        Offset(Array(index.toLong)),
        DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFrom(entryId)).build(),
        envelope)

  private def appendLog(entry: DamlLogEntryId, envelope: ByteString): Future[Index] =
    for {
      currentHead <- Future(Files.readAllLines(logHeadPath).get(0)).transform {
        case Success(contents) =>
          Success(contents.toInt)
        case Failure(exception) if exception.isInstanceOf[NoSuchFileException] =>
          Success(StartOffset)
        case Failure(exception) =>
          Failure(exception)
      }
      newHead = currentHead + 1
      id = Bytes.toString(entry.getEntryId)
      _ <- Future(Files.write(logEntriesDirectory.resolve(id), envelope.toByteArray))
      _ <- Future(
        Files.write(logIndexDirectory.resolve(currentHead.toString), entry.getEntryId.toByteArray))
      _ <- Future(Files.write(logHeadPath, Seq(newHead.toString).asJava))
    } yield newHead

  private def readState(key: DamlStateKey): Future[Option[DamlStateValue]] = Future {
    val path = StateKeys.resolveStateKey(stateDirectory, key)
    try {
      val contents = Files.readAllBytes(path)
      Some(DamlStateValue.parseFrom(contents))
    } catch {
      case _: NoSuchFileException =>
        None
    }
  }

  private def updateState(stateUpdates: Map[DamlStateKey, DamlStateValue]): Future[Unit] = Future {
    for ((key, value) <- stateUpdates) {
      val path = StateKeys.resolveStateKey(stateDirectory, key)
      Files.createDirectories(path.getParent)
      Files.write(path, value.toByteArray)
    }
  }

  // TODO: implement on the file system, not in memory
  private def locked[T](body: => Future[T]): Future[T] =
    Future
      .successful(lock.acquire())
      .flatMap(_ => body)
      .map(result => {
        lock.release()
        result
      })
}

object FileSystemLedgerReaderWriter {
  type Index = Int

  private val StartOffset: Index = 0

  def apply(
      ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
      participantId: ParticipantId,
      root: Path,
  )(implicit executionContext: ExecutionContext): Future[FileSystemLedgerReaderWriter] = Future {
    Files.createDirectories(root)
    val logDirectory = root.resolve("log")
    Files.createDirectories(logDirectory)
    val logIndexDirectory = logDirectory.resolve("index")
    Files.createDirectories(logIndexDirectory)
    val logEntriesDirectory = logDirectory.resolve("entries")
    Files.createDirectories(logEntriesDirectory)
    val stateDirectory = root.resolve("state")
    Files.createDirectories(stateDirectory)
    new FileSystemLedgerReaderWriter(
      ledgerId,
      participantId,
      logDirectory,
      logIndexDirectory,
      logEntriesDirectory,
      stateDirectory)
  }
}
