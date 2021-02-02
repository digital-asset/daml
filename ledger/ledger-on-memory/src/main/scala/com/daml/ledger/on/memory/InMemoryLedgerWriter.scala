// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.stream.Materializer
import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.api.{
  BatchingLedgerWriter,
  BatchingLedgerWriterConfig,
  CommitMetadata,
  LedgerWriter,
}
import com.daml.ledger.participant.state.kvutils.export.LedgerDataExporter
import com.daml.ledger.participant.state.kvutils.{KeyValueCommitting, Raw}
import com.daml.ledger.participant.state.v1.{ParticipantId, SubmissionResult}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorFactory,
  BatchedValidatingCommitter,
  ConflictDetection,
}
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
import com.daml.ledger.validator.{
  SerializingStateReader,
  StateKeySerializationStrategy,
  ValidateAndCommit,
}
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

final class InMemoryLedgerWriter private[memory] (
    override val participantId: ParticipantId,
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
    validateAndCommit: ValidateAndCommit,
) extends LedgerWriter {
  override def commit(
      correlationId: String,
      envelope: Raw.Value,
      metadata: CommitMetadata,
  ): Future[SubmissionResult] =
    validateAndCommit(correlationId, envelope, participantId)
      .andThen { case Success(SubmissionResult.Acknowledged) =>
        dispatcher.signalNewHead(state.newHeadSinceLastWrite())
      }(DirectExecutionContext)

  override def currentHealth(): HealthStatus = Healthy
}

object InMemoryLedgerWriter {

  private[memory] val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  private[memory] type StateValueCache = Cache[DamlStateKey, DamlStateValue]

  final class BatchingOwner(
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig,
      participantId: ParticipantId,
      metrics: Metrics,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: StateValueCache = Cache.none,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      engine: Engine,
  )(implicit materializer: Materializer)
      extends ResourceOwner[LedgerWriter] {
    override def acquire()(implicit context: ResourceContext): Resource[LedgerWriter] =
      for {
        ledgerDataExporter <- LedgerDataExporter.Owner.acquire()
        keyValueCommitting = createKeyValueCommitting(metrics, timeProvider, engine)
        committer = createBatchedCommitter(keyValueCommitting, ledgerDataExporter)
        writer = new InMemoryLedgerWriter(participantId, dispatcher, state, committer)
        // We need to generate batched submissions for the validator in order to improve throughput.
        // Hence, we have a BatchingLedgerWriter collect and forward batched submissions to the
        // in-memory committer.
        batchingWriter <- newLoggingContext { implicit loggingContext =>
          ResourceOwner
            .forCloseable(() => BatchingLedgerWriter(batchingLedgerWriterConfig, writer))
            .acquire()
        }
      } yield batchingWriter

    private def createBatchedCommitter(
        keyValueCommitting: KeyValueCommitting,
        ledgerDataExporter: LedgerDataExporter,
    )(implicit materializer: Materializer): ValidateAndCommit = {
      val validator = BatchedSubmissionValidator[Index](
        BatchedSubmissionValidatorFactory.defaultParametersFor(
          batchingLedgerWriterConfig.enableBatching
        ),
        keyValueCommitting,
        new ConflictDetection(metrics),
        metrics,
        ledgerDataExporter,
      )
      val committer = BatchedValidatingCommitter[Index](
        () => timeProvider.getCurrentTime,
        validator,
        stateValueCache,
      )
      locally {
        implicit val executionContext: ExecutionContext = materializer.executionContext

        def validateAndCommit(
            correlationId: String,
            submissionEnvelope: Raw.Value,
            submittingParticipantId: ParticipantId,
        ) =
          new InMemoryLedgerStateAccess(state, metrics).inTransaction { ledgerStateOperations =>
            committer.commit(
              correlationId,
              submissionEnvelope,
              submittingParticipantId,
              ledgerStateOperations,
            )
          }

        validateAndCommit
      }
    }
  }

  final class Owner(
      participantId: ParticipantId,
      keySerializationStrategy: StateKeySerializationStrategy,
      metrics: Metrics,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: StateValueCache = Cache.none,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      engine: Engine,
      committerExecutionContext: ExecutionContext,
  ) extends ResourceOwner[LedgerWriter] {
    override def acquire()(implicit context: ResourceContext): Resource[LedgerWriter] = {
      val keyValueCommitting = createKeyValueCommitting(metrics, timeProvider, engine)
      for {
        ledgerDataExporter <- LedgerDataExporter.Owner.acquire()
      } yield {
        val committer = createPreExecutingCommitter(keyValueCommitting, ledgerDataExporter)
        new InMemoryLedgerWriter(participantId, dispatcher, state, committer)
      }
    }

    private def createPreExecutingCommitter(
        keyValueCommitting: KeyValueCommitting,
        ledgerDataExporter: LedgerDataExporter,
    ): ValidateAndCommit = {
      val now = () => timeProvider.getCurrentTime
      val committer = new PreExecutingValidatingCommitter[
        Option[DamlStateValue],
        RawPreExecutingCommitStrategy.ReadSet,
        RawKeyValuePairsWithLogEntry,
      ](
        transformStateReader = transformStateReader(keySerializationStrategy, stateValueCache),
        validator = new PreExecutingSubmissionValidator(
          keyValueCommitting,
          new RawPreExecutingCommitStrategy(keySerializationStrategy),
          metrics,
        ),
        postExecutionConflictDetector = new EqualityBasedPostExecutionConflictDetector,
        postExecutionWriteSetSelector = new TimeBasedWriteSetSelector(now),
        postExecutionWriter = new RawPostExecutionWriter,
        ledgerDataExporter = ledgerDataExporter,
      )
      locally {
        implicit val executionContext: ExecutionContext = committerExecutionContext

        def validateAndCommit(
            correlationId: String,
            submissionEnvelope: Raw.Value,
            submittingParticipantId: ParticipantId,
        ) =
          committer.commit(
            submittingParticipantId,
            correlationId,
            submissionEnvelope,
            now(),
            new InMemoryLedgerStateAccess(state, metrics),
          )

        validateAndCommit
      }
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

  private def createKeyValueCommitting(
      metrics: Metrics,
      timeProvider: TimeProvider,
      engine: Engine,
  ): KeyValueCommitting =
    new KeyValueCommitting(engine, metrics, inStaticTimeMode = needStaticTimeModeFor(timeProvider))

  private def needStaticTimeModeFor(timeProvider: TimeProvider): Boolean =
    timeProvider != TimeProvider.UTC

}
