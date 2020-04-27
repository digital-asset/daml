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
import com.daml.ledger.validator.LedgerStateOperations.{Key, MetricPrefix, Value}
import com.daml.ledger.validator._
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.metrics.Timed
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
    engine: Engine,
)(implicit executionContext: ExecutionContext)
    extends LedgerWriter
    with LedgerReader {

  private val committer = new ValidatingCommitter(
    () => timeProvider.getCurrentTime,
    SubmissionValidator
      .createForTimeMode(
        engine,
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

  @SuppressWarnings(Array("org.wartremover.warts.Any")) // so we can use `.view`
  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    dispatcher
      .startingAt(
        startExclusive
          .map(KVOffset.highestIndex(_).toInt)
          .getOrElse(StartIndex),
        RangeSource((startExclusive, endInclusive) =>
          Source.fromIterator(() => {
            Timed.value(
              Metrics.readLog,
              state
                .readLog(
                  _.view.zipWithIndex.map(_.swap).slice(startExclusive + 1, endInclusive + 1))
                .iterator)
          }))
      )
      .map { case (_, updates) => updates }

  object InMemoryLedgerStateAccess extends LedgerStateAccess[Index] {
    override def inTransaction[T](body: LedgerStateOperations[Index] => Future[T]): Future[T] =
      state.write { (log, state) =>
        body(
          new TimedLedgerStateOperations(
            new InMemoryLedgerStateOperations(log, state),
            metricRegistry))
      }
  }

  private final class InMemoryLedgerStateOperations(
      log: InMemoryState.MutableLog,
      state: InMemoryState.MutableState,
  ) extends BatchingLedgerStateOperations[Index] {
    override def readState(keys: Seq[Key]): Future[Seq[Option[Value]]] =
      Future.successful(keys.map(state.get))

    override def writeState(keyValuePairs: Seq[(Key, Value)]): Future[Unit] = {
      state ++= keyValuePairs
      Future.unit
    }

    override def appendToLog(key: Key, value: Value): Future[Index] =
      Future.successful(appendEntry(log, LedgerRecord(_, key, value)))
  }

  private object Metrics {
    val readLog: Timer = metricRegistry.timer(MetricPrefix :+ "log" :+ "read")
  }
}

object InMemoryLedgerReaderWriter {
  type Index = Int

  private val StartIndex: Index = 0

  private val NamespaceLogEntries = "L"

  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  private val sequentialLogEntryId = new SequentialLogEntryId(NamespaceLogEntries)

  final class SingleParticipantOwner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: Cache[Bytes, DamlStateValue] = Cache.none,
      metricRegistry: MetricRegistry,
      engine: Engine,
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
          engine
        ).acquire()
      } yield readerWriter
    }
  }

  // passing the `dispatcher` and `state` from the outside allows us to share
  // the backing data for the LedgerReaderWriter and therefore setup multiple participants
  final class Owner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      metricRegistry: MetricRegistry,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: Cache[Bytes, DamlStateValue] = Cache.none,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      engine: Engine,
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
          engine,
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
