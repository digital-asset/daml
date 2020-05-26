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
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.api._
import com.daml.ledger.participant.state.kvutils.{Bytes, KeyValueCommitting}
import com.daml.ledger.participant.state.v1.{LedgerId, Offset, ParticipantId, SubmissionResult}
import com.daml.ledger.validator._
import com.daml.ledger.validator.batch.{
  BatchedSubmissionValidator,
  BatchedSubmissionValidatorParameters,
  ConflictDetection
}
import com.daml.lf.data.Ref
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext.newLoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

final class InMemoryBatchedLedgerReaderWriter(
    override val participantId: ParticipantId,
    override val ledgerId: LedgerId,
    dispatcher: Dispatcher[Index],
    state: InMemoryState,
    committer: BatchedValidatingCommitter[Index],
    metrics: Metrics)(implicit materializer: Materializer, executionContext: ExecutionContext)
    extends LedgerReader
    with LedgerWriter {
  override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] =
    ledgerStateAccess
      .inTransaction { ledgerStateOperations =>
        committer
          .commit(correlationId, envelope, participantId, ledgerStateOperations)
      }
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

object InMemoryBatchedLedgerReaderWriter {
  val DefaultTimeProvider: TimeProvider = TimeProvider.UTC

  final class SingleParticipantOwner(
      initialLedgerId: Option[LedgerId],
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
        dispatcher <- InMemoryLedgerReader.dispatcher.acquire()
        readerWriter <- new Owner(
          initialLedgerId,
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
      initialLedgerId: Option[LedgerId],
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
      val ledgerId =
        initialLedgerId.getOrElse(Ref.LedgerString.assertFromString(UUID.randomUUID.toString))
      val keyValueCommitting =
        new KeyValueCommitting(
          engine,
          metrics,
          inStaticTimeMode = needStaticTimeModeFor(timeProvider))
      val validator = BatchedSubmissionValidator[Index](
        BatchedSubmissionValidatorParameters.default,
        keyValueCommitting,
        new ConflictDetection(metrics),
        metrics,
        engine)
      val committer =
        BatchedValidatingCommitter[Index](
          () => timeProvider.getCurrentTime,
          validator,
          stateValueCache)
      val readerWriter =
        new InMemoryBatchedLedgerReaderWriter(
          participantId,
          ledgerId,
          dispatcher,
          state,
          committer,
          metrics
        )
      if (batchingLedgerWriterConfig.enableBatching) {
        val combinedReaderWriter = newLoggingContext { implicit logCtx =>
          val batchingLedgerWriter = new BatchingLedgerWriter(
            BatchingQueueFactory.batchingQueueFrom(batchingLedgerWriterConfig),
            readerWriter)
          new LedgerReader with LedgerWriter {
            override def events(startExclusive: Option[Offset]): Source[LedgerRecord, NotUsed] =
              readerWriter.events(startExclusive)

            override def ledgerId(): LedgerId = readerWriter.ledgerId

            override def currentHealth(): HealthStatus = readerWriter.currentHealth()

            override def participantId: ParticipantId = readerWriter.participantId

            override def commit(correlationId: String, envelope: Bytes): Future[SubmissionResult] =
              batchingLedgerWriter.commit(correlationId, envelope)
          }
        }
        Resource.successful(combinedReaderWriter)
      } else {
        Resource.successful(readerWriter)
      }
    }
  }

  private def needStaticTimeModeFor(timeProvider: TimeProvider): Boolean =
    timeProvider != TimeProvider.UTC
}
