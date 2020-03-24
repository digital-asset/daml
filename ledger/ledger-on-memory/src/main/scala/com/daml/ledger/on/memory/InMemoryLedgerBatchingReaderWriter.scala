// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Instant
import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.on.memory.InMemoryLedgerBatchingReaderWriter._
import com.daml.ledger.on.memory.InMemoryState.MutableLog
import com.daml.ledger.participant.state.kvutils.api.{
  BatchingLedgerWriter,
  DefaultBatchingQueue,
  LedgerEntry,
  LedgerReader,
  LedgerWriter
}
import com.daml.ledger.participant.state.kvutils.{Bytes, KVOffset, SequentialLogEntryId}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.logging.LoggingContext
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

// Dispatcher and InMemoryState are passed in to allow for a multi-participant setup.
final class InMemoryLedgerBatchingReaderWriter(
    override val ledgerId: LedgerId,
    override val participantId: ParticipantId,
    timeProvider: TimeProvider,
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
)(implicit materializer: Materializer)
    extends LedgerWriter
    with LedgerReader {

  private val theParticipantId = participantId // FIXME(JM): How to refer to the parent participantId below?
  private implicit val executionContext = materializer.executionContext

  private val committer = new BatchValidator(
    BatchValidationParameters.default,
    () => sequentialLogEntryId.next(),
    new InMemoryLedgerStateAccess2(state, dispatcher)
  )

  private val underlyingWriter = new LedgerWriter {
    override def currentHealth(): HealthStatus = Healthy
    override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] = {
      println("COMMITTING BATCH")
      committer
        .validateAndCommit(
          timeProvider.getCurrentTime,
          participantId,
          envelope
        )
        .map(_ => SubmissionResult.Acknowledged)
        .recover {
          case err =>
            SubmissionResult.InternalError(err.getMessage)
        }
    }
    override def participantId: ParticipantId = theParticipantId
  }

  private val batchingQueue =
    DefaultBatchingQueue(
      maxQueueSize = 100,
      maxBatchSizeBytes = 8L * 1024 * 1024,
      maxWaitDuration = 50.millis,
      maxConcurrentCommits = 10)

  private val batchingWriter =
    LoggingContext.newLoggingContext { implicit logCtx =>
      new BatchingLedgerWriter(batchingQueue, underlyingWriter)
    }

  override def events(startExclusive: Option[Offset]): Source[LedgerEntry, NotUsed] =
    dispatcher
      .startingAt(
        startExclusive
          .map(KVOffset.highestIndex(_).toInt)
          .getOrElse(StartIndex),
        OneAfterAnother[Index, List[LedgerEntry]](
          (index: Index) => index + 1,
          (index: Index) => Future.successful(List(retrieveLogEntry(index))),
        ),
      )
      .mapConcat { case (_, updates) => updates }

  private def retrieveLogEntry(index: Int): LedgerEntry =
    state.withReadLock((log, _) => log(index))

  override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] =
    batchingWriter.commit(correlationId, envelope)

  override def currentHealth(): HealthStatus = batchingWriter.currentHealth()
}

class InMemoryLedgerStateAccess2(currentState: InMemoryState, dispatcher: Dispatcher[Index])(
    implicit executionContext: ExecutionContext
) extends LedgerState {

  override def inTransaction[T](body: LedgerOps => Future[T]): Future[T] =
    currentState.withFutureWriteLock { (log, state) =>
      val ledgerOps = new InMemoryLedgerStateOps(log, state).toLedgerOps
      val result = body(ledgerOps)
      dispatcher.signalNewHead(log.size - 1)
      result
    }
}

private class InMemoryLedgerStateOps(
    log: InMemoryState.MutableLog,
    state: InMemoryState.MutableState
)(implicit executionContext: ExecutionContext)
    extends RawLedgerOps {
  override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
    Future.successful {
      keys.map(keyBytes => state.get(keyBytes))
    }

  override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
    Future.successful {
      state ++= keyValuePairs.map {
        case (keyBytes, valueBytes) => keyBytes -> valueBytes
      }
    }

  override def appendToLog(key: Key, value: Value): Future[Unit] =
    Future.successful {
      val _ = appendEntry(log, LedgerEntry.LedgerRecord(_, key, value))
    }
}

object InMemoryLedgerBatchingReaderWriter {
  type Index = Int

  private val StartIndex: Index = 0

  private val NamespaceLogEntries = "L"

  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  private val sequentialLogEntryId = new SequentialLogEntryId(NamespaceLogEntries)

  class SingleParticipantOwner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      timeProvider: TimeProvider = DefaultTimeProvider,
      heartbeats: Source[Instant, NotUsed] = Source.empty,
  )(implicit materializer: Materializer)
      extends ResourceOwner[InMemoryLedgerBatchingReaderWriter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[InMemoryLedgerBatchingReaderWriter] = {
      val state = new InMemoryState
      for {
        dispatcher <- dispatcher.acquire()
        _ = publishHeartbeats(state, dispatcher, heartbeats)
        readerWriter <- new Owner(
          initialLedgerId,
          participantId,
          timeProvider,
          dispatcher,
          state,
          materializer
        ).acquire()
      } yield readerWriter
    }
  }

  // passing the `dispatcher` and `state` from the outside allows us to share
  // the backing data for the LedgerReaderWriter and therefore setup multiple participants
  class Owner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      timeProvider: TimeProvider = DefaultTimeProvider,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      materializer: Materializer
  ) extends ResourceOwner[InMemoryLedgerBatchingReaderWriter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[InMemoryLedgerBatchingReaderWriter] = {
      val ledgerId =
        initialLedgerId.getOrElse(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))
      Resource.successful(
        new InMemoryLedgerBatchingReaderWriter(
          ledgerId,
          participantId,
          timeProvider,
          dispatcher,
          state,
        )(materializer))
    }
  }

  def dispatcher: ResourceOwner[Dispatcher[Index]] =
    ResourceOwner.forCloseable(
      () =>
        Dispatcher(
          "in-memory-batching-key-value-participant-state",
          zeroIndex = StartIndex,
          headAtInitialization = StartIndex,
      ))

  private def publishHeartbeats(
      state: InMemoryState,
      dispatcher: Dispatcher[Index],
      heartbeats: Source[Instant, NotUsed]
  )(implicit materializer: Materializer, executionContext: ExecutionContext): Future[Unit] =
    heartbeats
      .runWith(Sink.foreach { timestamp =>
        state.withWriteLock { (log, _) =>
          dispatcher.signalNewHead(appendEntry(log, LedgerEntry.Heartbeat(_, timestamp)))
        }
      })
      .map(_ => ())

  private[memory] def appendEntry(log: MutableLog, createEntry: Offset => LedgerEntry): Int = {
    val entryAtIndex = log.size
    val offset = KVOffset.fromLong(entryAtIndex.toLong)
    val entry = createEntry(offset)
    log += entry
    entryAtIndex
  }
}
