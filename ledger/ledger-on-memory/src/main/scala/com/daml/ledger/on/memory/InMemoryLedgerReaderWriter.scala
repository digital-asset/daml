// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Instant
import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter._
import com.daml.ledger.on.memory.InMemoryState.MutableLog
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlSubmission
import com.daml.ledger.participant.state.kvutils.api.{LedgerEntry, LedgerReader, LedgerWriter}
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
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import com.digitalasset.resources.ResourceOwner
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

// Dispatcher and InMemoryState are passed in to allow for a multi-participant setup.
final class InMemoryLedgerReaderWriter(
    override val ledgerId: LedgerId,
    override val participantId: ParticipantId,
    timeProvider: TimeProvider,
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
)(implicit executionContext: ExecutionContext)
    extends LedgerWriter
    with LedgerReader {

  private val committer = new ValidatingCommitter(
    participantId,
    () => timeProvider.getCurrentTime,
    SubmissionValidator
      .create(new InMemoryLedgerStateAccess(state), () => sequentialLogEntryId.next()),
    dispatcher.signalNewHead,
  )

  override def currentHealth(): HealthStatus =
    Healthy

  override def commit(correlationId: String, submission: DamlSubmission): Future[SubmissionResult] =
    committer.commit(correlationId, submission)

  override def events(offset: Option[Offset]): Source[LedgerEntry, NotUsed] =
    dispatcher
      .startingAt(
        offset
          .map(_.components.head.toInt)
          .getOrElse(StartIndex),
        OneAfterAnother[Int, List[LedgerEntry]](
          (index: Int, _) => index + 1,
          (index: Int) => Future.successful(List(retrieveLogEntry(index))),
        ),
      )
      .mapConcat { case (_, updates) => updates }

  private def retrieveLogEntry(index: Int): LedgerEntry =
    state.withReadLock((log, _) => log(index))
}

class InMemoryLedgerStateAccess(currentState: InMemoryState)(
    implicit executionContext: ExecutionContext
) extends LedgerStateAccess[Index] {

  override def inTransaction[T](body: LedgerStateOperations[Index] => Future[T]): Future[T] =
    currentState.withFutureWriteLock { (log, state) =>
      body(new InMemoryLedgerStateOperations(log, state))
    }
}

private class InMemoryLedgerStateOperations(
    log: InMemoryState.MutableLog,
    state: InMemoryState.MutableState,
)(implicit executionContext: ExecutionContext)
    extends BatchingLedgerStateOperations[Index] {
  override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
    Future.successful {
      keys.map(keyBytes => state.get(ByteString.copyFrom(keyBytes)))
    }

  override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
    Future.successful {
      state ++= keyValuePairs.map {
        case (keyBytes, valueBytes) => ByteString.copyFrom(keyBytes) -> valueBytes
      }
    }

  override def appendToLog(key: Key, value: Value): Future[Index] =
    Future.successful {
      val entryId = KeyValueCommitting.unpackDamlLogEntryId(key)
      appendEntry(log, LedgerEntry.LedgerRecord(_, entryId, value))
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
      heartbeats: Source[Instant, NotUsed] = Source.empty,
  )(
      implicit materializer: Materializer,
      executionContext: ExecutionContext,
  ): ResourceOwner[InMemoryLedgerReaderWriter] = {
    val state = new InMemoryState
    for {
      dispatcher <- dispatcher
      _ = publishHeartbeats(state, dispatcher, heartbeats)
      readerWriter <- owner(
        initialLedgerId,
        participantId,
        timeProvider,
        dispatcher,
        state,
      )
    } yield readerWriter
  }

  // passing the `dispatcher` and `state` from the outside allows us to share
  // the backing data for the LedgerReaderWriter and therefore setup multiple participants
  def owner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      timeProvider: TimeProvider = DefaultTimeProvider,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
  )(implicit executionContext: ExecutionContext): ResourceOwner[InMemoryLedgerReaderWriter] = {
    val ledgerId =
      initialLedgerId.getOrElse(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))
    ResourceOwner.successful(
      new InMemoryLedgerReaderWriter(
        ledgerId,
        participantId,
        timeProvider,
        dispatcher,
        state,
      ))
  }

  private def publishHeartbeats(
      state: InMemoryState,
      dispatcher: Dispatcher[Index],
      heartbeats: Source[Instant, NotUsed]
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] =
    heartbeats
      .runWith(Sink.foreach(timestamp =>
        dispatcher.signalNewHead(state.withWriteLock { (log, _) =>
          appendEntry(log, LedgerEntry.Heartbeat(_, timestamp))
        })))
      .map(_ => ())

  private[memory] def appendEntry(log: MutableLog, createEntry: Offset => LedgerEntry): Int = {
    val offset = Offset(Array(log.size.toLong))
    val entry = createEntry(offset)
    log += entry
    log.size
  }
}
