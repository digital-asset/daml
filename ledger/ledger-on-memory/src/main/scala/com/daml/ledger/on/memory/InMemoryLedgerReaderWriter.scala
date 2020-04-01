// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter._
import com.daml.ledger.on.memory.InMemoryState.MutableLog
import com.daml.ledger.participant.state.kvutils.api.{LedgerEntry, LedgerReader, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.{Bytes, KVOffset, SequentialLogEntryId}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator._
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.health.{HealthStatus, Healthy}
import com.digitalasset.platform.akkastreams.dispatcher.Dispatcher
import com.digitalasset.platform.akkastreams.dispatcher.SubSource.OneAfterAnother
import com.digitalasset.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

// Dispatcher and InMemoryState are passed in to allow for a multi-participant setup.
final class InMemoryLedgerReaderWriter(
    override val ledgerId: LedgerId,
    override val participantId: ParticipantId,
    metricRegistry: MetricRegistry,
    timeProvider: TimeProvider,
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
)(implicit executionContext: ExecutionContext)
    extends LedgerWriter
    with LedgerReader {

  private val committer = new ValidatingCommitter(
    () => timeProvider.getCurrentTime,
    SubmissionValidator
      .create(
        new InMemoryLedgerStateAccess(state),
        allocateNextLogEntryId = () => sequentialLogEntryId.next(),
        metricRegistry = metricRegistry,
      ),
    dispatcher.signalNewHead,
  )

  override def currentHealth(): HealthStatus =
    Healthy

  override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] =
    committer.commit(correlationId, envelope, participantId)

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
      keys.map(keyBytes => state.get(keyBytes))
    }

  override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] =
    Future.successful {
      state ++= keyValuePairs.map {
        case (keyBytes, valueBytes) => keyBytes -> valueBytes
      }
    }

  override def appendToLog(key: Key, value: Value): Future[Index] =
    Future.successful {
      appendEntry(log, LedgerEntry.LedgerRecord(_, key, value))
    }
}

object InMemoryLedgerReaderWriter {
  type Index = Int

  private val StartIndex: Index = 0

  private val NamespaceLogEntries = "L"

  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  private val sequentialLogEntryId = new SequentialLogEntryId(NamespaceLogEntries)

  class SingleParticipantOwner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      metricRegistry: MetricRegistry,
      timeProvider: TimeProvider = DefaultTimeProvider,
  )(implicit materializer: Materializer)
      extends ResourceOwner[InMemoryLedgerReaderWriter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[InMemoryLedgerReaderWriter] = {
      val state = new InMemoryState
      for {
        dispatcher <- dispatcher.acquire()
        readerWriter <- new Owner(
          initialLedgerId,
          participantId,
          metricRegistry,
          timeProvider,
          dispatcher,
          state,
        ).acquire()
      } yield readerWriter
    }
  }

  // passing the `dispatcher` and `state` from the outside allows us to share
  // the backing data for the LedgerReaderWriter and therefore setup multiple participants
  class Owner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      metricRegistry: MetricRegistry,
      timeProvider: TimeProvider = DefaultTimeProvider,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
  ) extends ResourceOwner[InMemoryLedgerReaderWriter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[InMemoryLedgerReaderWriter] = {
      val ledgerId =
        initialLedgerId.getOrElse(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))
      Resource.successful(
        new InMemoryLedgerReaderWriter(
          ledgerId,
          participantId,
          metricRegistry,
          timeProvider,
          dispatcher,
          state,
        ))
    }
  }

  def dispatcher: ResourceOwner[Dispatcher[Index]] =
    ResourceOwner.forCloseable(
      () =>
        Dispatcher(
          "in-memory-key-value-participant-state",
          zeroIndex = StartIndex,
          headAtInitialization = StartIndex,
      ))

  private[memory] def appendEntry(log: MutableLog, createEntry: Offset => LedgerEntry): Int = {
    val entryAtIndex = log.size
    val offset = KVOffset.fromLong(entryAtIndex.toLong)
    val entry = createEntry(offset)
    log += entry
    entryAtIndex
  }
}
