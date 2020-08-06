// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.api.util.TimeProvider
import com.daml.caching.Cache
import com.daml.ledger.api.health.{HealthStatus, Healthy}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.api._
import com.daml.ledger.participant.state.kvutils.{
  Bytes,
  Fingerprint,
  FingerprintPlaceholder,
  KeyValueCommitting
}
import com.daml.ledger.participant.state.v1.{LedgerId, Offset, ParticipantId, SubmissionResult}
import com.daml.ledger.validator.LedgerStateOperations.Value
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorFactory,
  BatchedValidatingCommitter,
  ConflictDetection
}
import com.daml.ledger.validator.caching.ImmutablesOnlyCacheUpdatePolicy
import com.daml.ledger.validator.preexecution._
import com.daml.ledger.validator.{StateKeySerializationStrategy, ValidateAndCommit}
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

final class InMemoryLedgerReaderWriter(
    override val participantId: ParticipantId,
    override val ledgerId: LedgerId,
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
    validateAndCommit: ValidateAndCommit,
    metrics: Metrics)(implicit executionContext: ExecutionContext)
    extends LedgerReader
    with LedgerWriter {
  override def commit(
      correlationId: String,
      envelope: Bytes,
      metadata: CommitMetadata,
  ): Future[SubmissionResult] =
    validateAndCommit(correlationId, envelope, participantId)
      .andThen {
        case Success(SubmissionResult.Acknowledged) =>
          dispatcher.signalNewHead(state.newHeadSinceLastWrite())
      }

  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    reader.events(startExclusive)

  override def currentHealth(): HealthStatus = Healthy

  private val reader = new InMemoryLedgerReader(ledgerId, dispatcher, state, metrics)
}

object InMemoryLedgerReaderWriter {
  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  def apply(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      validateAndCommit: ValidateAndCommit,
      metrics: Metrics,
  )(
      implicit executionContext: ExecutionContext,
  ): InMemoryLedgerReaderWriter =
    new InMemoryLedgerReaderWriter(
      participantId,
      ledgerId,
      dispatcher,
      state,
      validateAndCommit,
      metrics
    )

  final class BatchingOwner(
      ledgerId: LedgerId,
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig,
      participantId: ParticipantId,
      metrics: Metrics,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: Cache[DamlStateKey, DamlStateValue] = Cache.none,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      engine: Engine,
  )(implicit materializer: Materializer)
      extends ResourceOwner[KeyValueLedger] {
    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[KeyValueLedger] = {
      val keyValueCommitting =
        createKeyValueCommitting(metrics, timeProvider, engine)

      val committer =
        createBatchedCommitter(
          keyValueCommitting,
          batchingLedgerWriterConfig,
          state,
          metrics,
          timeProvider,
          stateValueCache)

      val readerWriter =
        InMemoryLedgerReaderWriter(ledgerId, participantId, dispatcher, state, committer, metrics)

      // We need to generate batched submissions for the validator in order to improve throughput.
      // Hence, we have a BatchingLedgerWriter collect and forward batched submissions to the
      // in-memory committer.
      val ledgerWriter = newLoggingContext { implicit loggingContext =>
        BatchingLedgerWriter(batchingLedgerWriterConfig, readerWriter)
      }

      Resource.successful(createKeyValueLedger(readerWriter, ledgerWriter))
    }
  }

  final class SingleParticipantBatchingOwner(
      ledgerId: LedgerId,
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig,
      participantId: ParticipantId,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCache: Cache[DamlStateKey, DamlStateValue] = Cache.none,
      metrics: Metrics,
      engine: Engine,
  )(implicit materializer: Materializer)
      extends ResourceOwner[KeyValueLedger] {

    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[KeyValueLedger] = {
      val state = InMemoryState.empty
      for {
        dispatcher <- dispatcherOwner.acquire()
        readerWriter <- new BatchingOwner(
          ledgerId,
          batchingLedgerWriterConfig,
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

  final class PreExecutingOwner(
      ledgerId: LedgerId,
      participantId: ParticipantId,
      keySerializationStrategy: StateKeySerializationStrategy,
      metrics: Metrics,
      timeProvider: TimeProvider = DefaultTimeProvider,
      stateValueCacheForPreExecution: Cache[DamlStateKey, (DamlStateValue, Fingerprint)] =
        Cache.none,
      dispatcher: Dispatcher[Index],
      state: InMemoryState,
      engine: Engine,
  ) extends ResourceOwner[KeyValueLedger] {
    override def acquire()(
        implicit executionContext: ExecutionContext): Resource[KeyValueLedger] = {
      val keyValueCommitting =
        createKeyValueCommitting(metrics, timeProvider, engine)

      val committer =
        createPreExecutingCommitter(
          keyValueCommitting,
          keySerializationStrategy,
          state,
          metrics,
          timeProvider,
          stateValueCacheForPreExecution)

      val readerWriter =
        InMemoryLedgerReaderWriter(ledgerId, participantId, dispatcher, state, committer, metrics)

      Resource.successful(createKeyValueLedger(readerWriter, readerWriter))
    }
  }

  private def createBatchedCommitter(
      keyValueCommitting: KeyValueCommitting,
      batchingLedgerWriterConfig: BatchingLedgerWriterConfig,
      state: InMemoryState,
      metrics: Metrics,
      timeProvider: TimeProvider,
      stateValueCache: Cache[DamlStateKey, DamlStateValue],
  )(
      implicit materializer: Materializer,
      executionContext: ExecutionContext,
  ): ValidateAndCommit = {
    val validator = BatchedSubmissionValidator[Index](
      BatchedSubmissionValidatorFactory.defaultParametersFor(
        batchingLedgerWriterConfig.enableBatching),
      keyValueCommitting,
      new ConflictDetection(metrics),
      metrics
    )
    val committer = BatchedValidatingCommitter[Index](
      () => timeProvider.getCurrentTime,
      validator,
      stateValueCache)
    def validateAndCommit(
        correlationId: String,
        submissionEnvelope: Bytes,
        submittingParticipantId: ParticipantId) =
      new InMemoryLedgerStateAccess(state, metrics).inTransaction { ledgerStateOperations =>
        committer
          .commit(correlationId, submissionEnvelope, submittingParticipantId, ledgerStateOperations)
      }
    validateAndCommit
  }

  private def createPreExecutingCommitter(
      keyValueCommitting: KeyValueCommitting,
      keySerializationStrategy: StateKeySerializationStrategy,
      state: InMemoryState,
      metrics: Metrics,
      timeProvider: TimeProvider,
      stateValueCacheForPreExecution: Cache[DamlStateKey, (DamlStateValue, Fingerprint)],
  )(
      implicit executionContext: ExecutionContext,
  ): ValidateAndCommit = {
    val commitStrategy = new LogAppenderPreExecutingCommitStrategy(keySerializationStrategy)
    val valueToFingerprint: Option[Value] => Fingerprint =
      _.getOrElse(FingerprintPlaceholder)
    val validator = new PreExecutingSubmissionValidator[RawKeyValuePairsWithLogEntry](
      keyValueCommitting,
      metrics,
      keySerializationStrategy,
      commitStrategy)
    val committer = new PreExecutingValidatingCommitter(
      () => timeProvider.getCurrentTime,
      keySerializationStrategy,
      validator,
      valueToFingerprint,
      new PostExecutionFinalizer[Index](valueToFingerprint),
      stateValueCache = stateValueCacheForPreExecution,
      ImmutablesOnlyCacheUpdatePolicy
    )
    def validateAndCommit(
        correlationId: String,
        submissionEnvelope: Bytes,
        submittingParticipantId: ParticipantId) =
      committer.commit(
        correlationId,
        submissionEnvelope,
        submittingParticipantId,
        new InMemoryLedgerStateAccess(state, metrics))
    validateAndCommit
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
