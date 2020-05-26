// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.on.memory.InMemoryLedgerReaderWriter._
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateValue
import com.daml.ledger.participant.state.kvutils.api.{LedgerReader, LedgerRecord, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.{Bytes, SequentialLogEntryId}
import com.daml.ledger.participant.state.v1._
import com.daml.ledger.validator._
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}

// Dispatcher and InMemoryState are passed in to allow for a multi-participant setup.
final class InMemoryLedgerReaderWriter private (
    override val ledgerId: LedgerId,
    override val participantId: ParticipantId,
    metrics: Metrics,
    timeProvider: TimeProvider,
    stateValueCache: Cache[Bytes, DamlStateValue],
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
    engine: Engine,
)(implicit executionContext: ExecutionContext)
    extends LedgerWriter
    with LedgerReader {

  private val reader = new InMemoryLedgerReader(ledgerId, dispatcher, state, metrics)
  private val committer = new ValidatingCommitter(
    () => timeProvider.getCurrentTime,
    SubmissionValidator
      .createForTimeMode(
        new InMemoryLedgerStateAccess(state, metrics),
        allocateNextLogEntryId = () => sequentialLogEntryId.next(),
        stateValueCache = stateValueCache,
        engine = engine,
        metrics = metrics,
        inStaticTimeMode = timeProvider != TimeProvider.UTC
      ),
    dispatcher.signalNewHead,
  )

  override def currentHealth(): HealthStatus =
    Healthy

  override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] =
    committer.commit(correlationId, envelope, participantId)

  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    reader.events(startExclusive)
}

object InMemoryLedgerReaderWriter {
  private val NamespaceLogEntries = "L"

  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  private val sequentialLogEntryId = new SequentialLogEntryId(NamespaceLogEntries)

  final class SingleParticipantOwner(
      initialLedgerId: Option[LedgerId],
      participantId: ParticipantId,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: Cache[Bytes, DamlStateValue] = Cache.none,
      metrics: Metrics,
      engine: Engine,
  )(implicit materializer: Materializer)
      extends ResourceOwner[InMemoryLedgerReaderWriter] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[InMemoryLedgerReaderWriter] = {
      val state = InMemoryState.empty
      for {
        dispatcher <- InMemoryLedgerReader.dispatcher.acquire()
        readerWriter <- new Owner(
          initialLedgerId,
          participantId,
          metrics,
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
      metrics: Metrics,
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
          metrics,
          timeProvider,
          stateValueCache,
          dispatcher,
          state,
          engine,
        ))
    }
  }
}
