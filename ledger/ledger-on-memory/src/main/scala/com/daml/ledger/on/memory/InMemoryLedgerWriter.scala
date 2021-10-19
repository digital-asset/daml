// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.Instant

import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.on.memory.InMemoryLedgerWriter._
import com.daml.ledger.participant.state.kvutils.api.{CommitMetadata, LedgerWriter}
import com.daml.ledger.participant.state.kvutils.export.LedgerDataExporter
import com.daml.ledger.participant.state.kvutils.store.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.{KVOffsetBuilder, KeyValueCommitting, Raw}
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.validator.caching.{CachingStateReader, ImmutablesOnlyCacheUpdatePolicy}
import com.daml.ledger.validator.preexecution.{
  EqualityBasedPostExecutionConflictDetector,
  PreExecutingSubmissionValidator,
  PreExecutingValidatingCommitter,
  RawKeyValuePairsWithLogEntry,
  RawPostExecutionWriter,
  RawPreExecutingCommitStrategy,
  TimeBasedWriteSetSelector,
}
import com.daml.ledger.validator.reading.{DamlLedgerStateReader, LedgerStateReader}
import com.daml.ledger.validator.{SerializingStateReader, StateKeySerializationStrategy}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.telemetry.TelemetryContext

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

final class InMemoryLedgerWriter private[memory] (
    override val participantId: Ref.ParticipantId,
    dispatcher: Dispatcher[Index],
    offsetBuilder: KVOffsetBuilder,
    now: () => Instant,
    state: InMemoryState,
    committer: Committer,
    committerExecutionContext: ExecutionContext,
    metrics: Metrics,
) extends LedgerWriter {

  override def commit(
      correlationId: String,
      envelope: Raw.Envelope,
      metadata: CommitMetadata,
  )(implicit telemetryContext: TelemetryContext): Future[SubmissionResult] =
    committer
      .commit(
        participantId,
        correlationId,
        envelope,
        exportRecordTime = now(),
        ledgerStateAccess = new InMemoryLedgerStateAccess(offsetBuilder, state, metrics),
      )(committerExecutionContext)
      .andThen { case Success(SubmissionResult.Acknowledged) =>
        dispatcher.signalNewHead(state.newHeadSinceLastWrite())
      }(committerExecutionContext)

  override def currentHealth(): HealthStatus = Healthy
}

object InMemoryLedgerWriter {

  private[memory] val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  private[memory] type StateValueCache = Cache[DamlStateKey, DamlStateValue]

  private[memory] type Committer = PreExecutingValidatingCommitter[
    Option[DamlStateValue],
    RawPreExecutingCommitStrategy.ReadSet,
    RawKeyValuePairsWithLogEntry,
  ]

  final class Owner(
      participantId: Ref.ParticipantId,
      keySerializationStrategy: StateKeySerializationStrategy,
      metrics: Metrics,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: StateValueCache = Cache.none,
      dispatcher: Dispatcher[Index],
      offsetBuilder: KVOffsetBuilder,
      state: InMemoryState,
      engine: Engine,
      committerExecutionContext: ExecutionContext,
  ) extends ResourceOwner[LedgerWriter] {
    private val now = () => timeProvider.getCurrentTime

    override def acquire()(implicit context: ResourceContext): Resource[LedgerWriter] =
      for {
        ledgerDataExporter <- LedgerDataExporter.Owner.acquire()
      } yield new InMemoryLedgerWriter(
        participantId,
        dispatcher,
        offsetBuilder,
        now,
        state,
        newCommitter(ledgerDataExporter),
        committerExecutionContext,
        metrics,
      )

    private def newCommitter(ledgerDataExporter: LedgerDataExporter): Committer = {
      val keyValueCommitting = new KeyValueCommitting(
        engine,
        metrics,
      )
      new Committer(
        transformStateReader = transformStateReader(keySerializationStrategy, stateValueCache),
        validator = new PreExecutingSubmissionValidator(
          keyValueCommitting,
          new RawPreExecutingCommitStrategy(keySerializationStrategy),
          metrics = metrics,
        ),
        postExecutionConflictDetector = new EqualityBasedPostExecutionConflictDetector,
        postExecutionWriteSetSelector = new TimeBasedWriteSetSelector(now),
        postExecutionWriter = new RawPostExecutionWriter,
        ledgerDataExporter = ledgerDataExporter,
      )
    }

    private def transformStateReader(
        keySerializationStrategy: StateKeySerializationStrategy,
        cache: Cache[DamlStateKey, DamlStateValue],
    )(stateReader: LedgerStateReader): DamlLedgerStateReader = {
      CachingStateReader(
        cache,
        ImmutablesOnlyCacheUpdatePolicy,
        SerializingStateReader(keySerializationStrategy)(stateReader),
      )
    }
  }

}
