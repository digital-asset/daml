// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.api.util.TimeProvider
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter._
import com.daml.ledger.on.memory.InMemoryState.MutableLog
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateValue
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.caching.Cache
import com.daml.ledger.participant.state.kvutils.{Bytes, KVOffset, SequentialLogEntryId}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator._
import com.daml.lf.data.Ref
import com.daml.metrics.MetricName
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

// Dispatcher and InMemoryState are passed in to allow for a multi-participant setup.
final class InMemoryLedgerReaderWriter private (
    override val ledgerId: LedgerId,
    override val participantId: ParticipantId,
    metricRegistry: MetricRegistry,
    timeProvider: TimeProvider,
    stateValueCache: Cache[Bytes, DamlStateValue],
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
)(implicit executionContext: ExecutionContext)
    extends LedgerWriter
    with LedgerReader {

  private val committer = new ValidatingCommitter(
    () => timeProvider.getCurrentTime,
    SubmissionValidator
      .createForTimeMode(
        InMemoryLedgerStateAccess,
        allocateNextLogEntryId = () => sequentialLogEntryId.next(),
        stateValueCache = stateValueCache,
        metricRegistry = metricRegistry,
        inStaticTimeMode = timeProvider != TimeProvider.UTC
      ),
    dispatcher.signalNewHead,
  )

  override def currentHealth(): HealthStatus =
    Healthy

  override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] =
    committer.commit(correlationId, envelope, participantId)

  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        startExclusive
          .map(KVOffset.highestIndex(_).toInt)
          .getOrElse(StartIndex),
        RangeSource((startExclusive, endInclusive) =>
          Source.fromIterator(() => {
            val entries: Seq[(LedgerRecord, Index)] =
              Metrics.readLog.time(() =>
                state.readLog(_.zipWithIndex.slice(startExclusive + 1, endInclusive + 1)))
            entries.iterator.map { case (entry, index) => index -> entry }
          }))
      )
      .map { case (_, updates) => updates }

  object InMemoryLedgerStateAccess extends LedgerStateAccess[Index] {
    override def inTransaction[T](body: LedgerStateOperations[Index] => Future[T]): Future[T] =
      state.write { (log, state) =>
        body(new InMemoryLedgerStateOperations(log, state))
      }
  }

  private class InMemoryLedgerStateOperations(
      log: InMemoryState.MutableLog,
      state: InMemoryState.MutableState,
  ) extends BatchingLedgerStateOperations[Index] {
    override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
      Future.successful(Metrics.readState.time(() => keys.map(state.get)))

    override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] = {
      Metrics.writeState.time { () =>
        state ++= keyValuePairs
      }
      Future.unit
    }

    override def appendToLog(key: Key, value: Value): Future[Index] =
      Future.successful(
        Metrics.appendToLog.time(() => appendEntry(log, LedgerRecord(_, key, value))))
  }

  private object Metrics {
    private val Prefix = MetricName.DAML :+ "ledger"

    val readLog: Timer = metricRegistry.timer(Prefix :+ "log" :+ "read")
    val appendToLog: Timer = metricRegistry.timer(Prefix :+ "log" :+ "append")
    val readState: Timer = metricRegistry.timer(Prefix :+ "state" :+ "read")
    val writeState: Timer = metricRegistry.timer(Prefix :+ "state" :+ "write")
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
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: Cache[Bytes, DamlStateValue] = Cache.none,
      metricRegistry: MetricRegistry,
  )(implicit materializer: Materializer)
      extends ResourceOwner[InMemoryLedgerReaderWriter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[InMemoryLedgerReaderWriter] = {
      val state = InMemoryState.empty
      for {
        dispatcher <- dispatcher.acquire()
        readerWriter <- new Owner(
          initialLedgerId,
          participantId,
          metricRegistry,
          timeProvider,
          stateValueCache,
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
      stateValueCache: Cache[Bytes, DamlStateValue] = Cache.none,
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
          stateValueCache,
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

  private[memory] def appendEntry(log: MutableLog, createEntry: Offset => LedgerRecord): Index = {
    val entryAtIndex = log.size
    val offset = KVOffset.fromLong(entryAtIndex.toLong)
    val entry = createEntry(offset)
    log += entry
    entryAtIndex
  }
}
