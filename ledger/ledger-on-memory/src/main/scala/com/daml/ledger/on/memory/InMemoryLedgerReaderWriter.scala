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
import com.daml.ledger.participant.state.kvutils.{Bytes, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.{LedgerId, Offset, ParticipantId, SubmissionResult}
import com.daml.ledger.validator.StateAccessingValidatingCommitter
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorFactory,
  BatchedValidatingCommitter,
  ConflictDetection
}
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
    committer: StateAccessingValidatingCommitter[Index],
    metrics: Metrics)(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends LedgerReader
    with LedgerWriter {
  override def commit(
      correlationId: String,
      envelope: Bytes,
      metadata: CommitMetadata,
  ): Future[SubmissionResult] =
    committer
      .commit(correlationId, envelope, participantId, ledgerStateAccess)
      .andThen {
        case Success(SubmissionResult.Acknowledged) =>
          dispatcher.signalNewHead(state.newHeadSinceLastWrite())
      }

  override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
    reader.events(startExclusive)

  override def currentHealth(): HealthStatus = Healthy

  private val reader = new InMemoryLedgerReader(ledgerId, dispatcher, state, metrics)

  private val ledgerStateAccess = new InMemoryLedgerStateAccess(state, metrics)
}

object InMemoryLedgerReaderWriter {
  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  final class SingleParticipantOwner(
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
        readerWriter <- new Owner(
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

  final class Owner(
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
        new KeyValueCommitting(
          engine,
          metrics,
          inStaticTimeMode = needStaticTimeModeFor(timeProvider))
      val validator = BatchedSubmissionValidator[Index](
        BatchedSubmissionValidatorFactory.defaultParametersFor(
          batchingLedgerWriterConfig.enableBatching),
        keyValueCommitting,
        new ConflictDetection(metrics),
        metrics
      )
      val committer =
        BatchedValidatingCommitter[Index](
          () => timeProvider.getCurrentTime,
          validator,
          stateValueCache)
      val readerWriter =
        new InMemoryLedgerReaderWriter(
          participantId,
          ledgerId,
          dispatcher,
          state,
          committer,
          metrics
        )
      // We need to generate batched submissions for the validator in order to improve throughput.
      // Hence, we have a BatchingLedgerWriter collect and forward batched submissions to the
      // in-memory committer.
      val batchingLedgerWriter = newLoggingContext { implicit logCtx =>
        BatchingLedgerWriter(batchingLedgerWriterConfig, readerWriter)
      }
      Resource.successful(createKeyValueLedger(readerWriter, batchingLedgerWriter))
    }
  }

  private def needStaticTimeModeFor(timeProvider: TimeProvider): Boolean =
    timeProvider != TimeProvider.UTC
}
