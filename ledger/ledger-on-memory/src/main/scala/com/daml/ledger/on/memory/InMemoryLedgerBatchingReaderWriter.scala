// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

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

  private implicit val executionContext = materializer.executionContext

  private val committer = BatchValidator(
    () => sequentialLogEntryId.next(),
    new InMemoryLedgerStateAccess2(state, dispatcher)
  )

  private val theParticipantId = participantId // FIXME(JM): How to refer to the parent participantId below?
  private val underlyingWriter = new LedgerWriter {
    override def currentHealth(): HealthStatus = Healthy
    override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] = {
      committer
        .validateAndCommit(
          timeProvider.getCurrentTime,
          participantId,
          correlationId,
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
      maxQueueSize = 1000,
      maxBatchSizeBytes = 16L * 1024 * 1024,
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
) extends BatchLedgerState {

  override def inTransaction[T](body: BatchLedgerOps => Future[T]): Future[T] = {
    val inMemoryOps = new InMemoryLedgerStateOps(currentState)

    // TODO(JM): This implementation is slightly silly as we have multi-threaded access
    // to the ledger ops. That is the reason why we don't hold the "InMemoryState" lock:
    // it wouldn't actually help against the multiple access here. Instead we only take the
    // lock when we're writing to the state. This gives us more interleaving, but has the downside
    // that if state is written after appending to log, we may end up waking consumers too early
    // (as another participant might've written to store and signalled).
    // A better implementation would concurrency-safe data structures to allow for the concurrent writes,
    // and we'd just use the semaphore for transactionality (e.g. one participant writes to store at a time).
    body(inMemoryOps).map { result =>
      inMemoryOps.getNewHead().foreach(dispatcher.signalNewHead)
      result
    }
  }
}

private class InMemoryLedgerStateOps(
    inMemoryState: InMemoryState
)(implicit executionContext: ExecutionContext)
    extends BatchLedgerOps {

  private val newHead = new AtomicInteger(-1)
  def getNewHead(): Option[Int] = {
    val n = newHead.get()
    if (n >= 0) Some(n) else None
  }

  override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
    Future.successful {
      inMemoryState.withReadLock { (_, state) =>
        keys.map(keyBytes => state.get(keyBytes))
      }
    }

  override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
    Future.successful {
      inMemoryState.withWriteLock[Unit] { (_, state) =>
        state ++= keyValuePairs.map {
          case (keyBytes, valueBytes) => keyBytes -> valueBytes
        }
      }
    }

  override def appendToLog(key: Key, value: Value): Future[Unit] =
    Future {
      inMemoryState.withWriteLock { (log, _) =>
        val idx = appendEntry(log, LedgerEntry.LedgerRecord(_, key, value))
        newHead.updateAndGet(idx.max(_))
        ()
      }
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
