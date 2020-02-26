// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.util.UUID
import java.util.concurrent.Semaphore

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlLogEntryId
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.{KeyValueCommitting, SequentialLogEntryId}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.{
  BatchingLedgerStateOperations,
  LedgerStateAccess,
  LedgerStateOperations,
  SubmissionValidator,
  ValidatingCommitter
}
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import com.digitalasset.resources.ResourceOwner
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}

private[memory] class LogEntry(val entryId: DamlLogEntryId, val payload: Array[Byte])

private[memory] object LogEntry {
  def apply(entryId: DamlLogEntryId, payload: Array[Byte]): LogEntry =
    new LogEntry(entryId, payload)
}

private[memory] class InMemoryState(
    val log: mutable.Buffer[LogEntry] = ArrayBuffer[LogEntry](),
    val state: mutable.Map[ByteString, Array[Byte]] = mutable.Map.empty,
) {
  val lockCurrentState = new Semaphore(1, true)

  def withLock[A](action: => A): A = {
    try {
      lockCurrentState.acquire()
      action
    } finally {
      lockCurrentState.release()
    }
  }

  def readLogEntry(index: Int): LogEntry = {
    withLock(log(index))
  }
}

// Dispatcher and InMemoryState are passed in to allow for a multi-participant setup.
final class InMemoryLedgerReaderWriter(
    override val ledgerId: LedgerId,
    override val participantId: ParticipantId,
    dispatcher: Dispatcher[Index],
    timeProvider: TimeProvider,
    inMemoryState: InMemoryState,
)(
    implicit executionContext: ExecutionContext,
    logCtx: LoggingContext,
) extends LedgerWriter
    with LedgerReader {

  private val committer = new ValidatingCommitter(
    participantId,
    () => timeProvider.getCurrentTime,
    SubmissionValidator
      .create(new InMemoryLedgerStateAccess(inMemoryState), () => sequentialLogEntryId.next()),
    dispatcher.signalNewHead,
  )

  override def currentHealth(): HealthStatus =
    Healthy

  override def commit(correlationId: String, envelope: Array[Byte]): Future[SubmissionResult] =
    committer.commit(correlationId, envelope)

  override def events(offset: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        offset
          .map(_.components.head.toInt)
          .getOrElse(StartIndex),
        OneAfterAnother[Int, List[LedgerRecord]](
          (index: Int, _) => index + 1,
          (index: Int) => Future.successful(List(retrieveLogEntry(index))),
        ),
      )
      .mapConcat { case (_, updates) => updates }

  private def retrieveLogEntry(index: Int): LedgerRecord = {
    val logEntry = inMemoryState.readLogEntry(index)
    LedgerRecord(Offset(Array(index.toLong)), logEntry.entryId, logEntry.payload)
  }
}

class InMemoryLedgerStateAccess(currentState: InMemoryState)(
    implicit executionContext: ExecutionContext)
    extends LedgerStateAccess[Index] {

  private object InMemoryLedgerStateOperations extends BatchingLedgerStateOperations[Index] {
    override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
      Future.successful {
        keys.map(keyBytes => currentState.state.get(ByteString.copyFrom(keyBytes)))
      }

    override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
      Future.successful {
        currentState.state ++= keyValuePairs.map {
          case (keyBytes, valueBytes) => ByteString.copyFrom(keyBytes) -> valueBytes
        }
      }

    override def appendToLog(key: Key, value: Value): Future[Index] =
      Future.successful {
        val damlLogEntryId = KeyValueCommitting.unpackDamlLogEntryId(key)
        val logEntry = LogEntry(damlLogEntryId, value)
        currentState.log += logEntry
        currentState.log.size
      }
  }

  override def inTransaction[T](body: LedgerStateOperations[Index] => Future[T]): Future[T] =
    Future
      .successful(currentState.lockCurrentState.acquire())
      .flatMap(_ => body(InMemoryLedgerStateOperations))
      .andThen {
        case _ =>
          currentState.lockCurrentState.release()
      }
}

object InMemoryLedgerReaderWriter {
  type Index = Int

  private val StartIndex: Index = 0

  private val NamespaceLogEntries = "L"

  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  private val sequentialLogEntryId = new SequentialLogEntryId(NamespaceLogEntries)

  def dispatcher: ResourceOwner[Dispatcher[Index]] =
    ResourceOwner.forCloseable(
      () =>
        Dispatcher(
          "in-memory-key-value-participant-state",
          zeroIndex = StartIndex,
          headAtInitialization = StartIndex,
      ))

  def singleParticipantOwner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      timeProvider: TimeProvider = DefaultTimeProvider,
  )(
      implicit executionContext: ExecutionContext,
      logCtx: LoggingContext,
  ): ResourceOwner[InMemoryLedgerReaderWriter] =
    for {
      dispatcher <- dispatcher
      readerWriter <- owner(
        initialLedgerId,
        participantId,
        dispatcher = dispatcher,
        inMemoryState = new InMemoryState)
    } yield readerWriter

  // passing the Dispatcher and InMemoryState from the outside allows us to share
  // the backing data for the LedgerReaderWriter and therefore setup multiple participants
  def owner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      timeProvider: TimeProvider = DefaultTimeProvider,
      dispatcher: Dispatcher[Index],
      inMemoryState: InMemoryState
  )(
      implicit executionContext: ExecutionContext,
      logCtx: LoggingContext,
  ): ResourceOwner[InMemoryLedgerReaderWriter] = {
    val ledgerId =
      initialLedgerId.getOrElse(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))
    ResourceOwner.successful(
      new InMemoryLedgerReaderWriter(ledgerId, participantId, dispatcher, timeProvider, inMemoryState))
  }
}
