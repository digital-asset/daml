// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.filesystem.posix

import java.nio.file.{Files, NoSuchFileException, Path}
import java.time.Clock
import java.util.UUID

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

class FileSystemLedgerReaderWriter private (
    ledgerId: LedgerId,
    override val participantId: ParticipantId,
    root: Path,
)(implicit executionContext: ExecutionContext)
    extends LedgerReader
    with LedgerWriter
    with AutoCloseable {

  // used as the ledger lock; when committing, only one commit owns the lock at a time
  private val lockPath = root.resolve("lock")
  // the root of the ledger log
  private val logDirectory = root.resolve("log")
  // stores each ledger entry
  private val logEntriesDirectory = logDirectory.resolve("entries")
  // a counter which is incremented with each commit;
  // always one more than the latest commit in the index
  private val logHeadPath = logDirectory.resolve("head")
  // a directory of sequential commits, each pointing to an entry in the "entries" directory
  private val logIndexDirectory = logDirectory.resolve("index")
  // a key-value store of the current state
  private val stateDirectory = root.resolve("state")

  private val lock = new FileSystemLock(lockPath)

  private val engine = Engine()

  private val dispatcher: Dispatcher[Index] =
    Dispatcher(
      "posix-filesystem-participant-state",
      zeroIndex = StartOffset,
      headAtInitialization = StartOffset,
    )

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
          (index: Index) => Future.successful(immutable.Seq(retrieveLogEntry(index))),
        )
      )
      .mapConcat {
        case (_, updates) => updates
      }

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] = {
    val submission = Envelope
      .openSubmission(envelope)
      .getOrElse(throw new IllegalArgumentException("Not a valid submission in envelope"))
    lock.run {
      val stateInputStream =
        submission.getInputDamlStateList.asScala.toVector
          .map(key => key -> readState(key))
      val stateInputs: Map[DamlStateKey, Option[DamlStateValue]] = stateInputStream.toMap
      val currentHead = currentLogHead()
      val entryId = DamlLogEntryId
        .newBuilder()
        .setEntryId(ByteString.copyFromUtf8(currentHead.toHexString))
        .build()
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
      val newHead = appendLog(currentHead, Envelope.enclose(logEntry))
      updateState(stateUpdates)
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

  private def retrieveLogEntry(entryId: Index): LedgerRecord = {
    val envelope = Files.readAllBytes(logEntriesDirectory.resolve(entryId.toString))
    LedgerRecord(
      Offset(Array(entryId.toLong)),
      DamlLogEntryId
        .newBuilder()
        .setEntryId(ByteString.copyFromUtf8(entryId.toHexString))
        .build(),
      envelope,
    )
  }

  private def currentLogHead(): Index = {
    try {
      Files.readAllLines(logHeadPath).get(0).toInt
    } catch {
      case _: NoSuchFileException =>
        StartOffset
    }
  }

  private def appendLog(currentHead: Index, envelope: ByteString): Index = {
    Files.write(logEntriesDirectory.resolve(currentHead.toString), envelope.toByteArray)
    val newHead = currentHead + 1
    Files.write(logHeadPath, Seq(newHead.toString).asJava)
    newHead
  }

  private def readState(key: DamlStateKey): Option[DamlStateValue] = {
    val path = StateKeys.resolveStateKey(stateDirectory, key)
    try {
      val contents = Files.readAllBytes(path)
      Some(DamlStateValue.parseFrom(contents))
    } catch {
      case _: NoSuchFileException =>
        None
    }
  }

  private def updateState(stateUpdates: Map[DamlStateKey, DamlStateValue]): Unit = {
    for ((key, value) <- stateUpdates) {
      val path = StateKeys.resolveStateKey(stateDirectory, key)
      Files.createDirectories(path.getParent)
      Files.write(path, value.toByteArray)
    }
  }

  private def createDirectories(): Future[Unit] = Future {
    Files.createDirectories(root)
    Files.createDirectories(logDirectory)
    Files.createDirectories(logEntriesDirectory)
    Files.createDirectories(logIndexDirectory)
    Files.createDirectories(stateDirectory)
    ()
  }
}

object FileSystemLedgerReaderWriter {
  type Index = Int

  private val StartOffset: Index = 0

  def apply(
      ledgerId: LedgerId = Ref.LedgerString.assertFromString(UUID.randomUUID.toString),
      participantId: ParticipantId,
      root: Path,
  )(implicit executionContext: ExecutionContext): Future[FileSystemLedgerReaderWriter] = {
    val ledger = new FileSystemLedgerReaderWriter(ledgerId, participantId, root)
    ledger.createDirectories().map(_ => ledger)
  }
}
