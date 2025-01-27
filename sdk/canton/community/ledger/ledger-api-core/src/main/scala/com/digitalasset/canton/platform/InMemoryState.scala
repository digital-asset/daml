// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.services.admin.PartyAllocation
import com.digitalasset.canton.platform.apiserver.services.tracking.{
  InFlight,
  StreamTracker,
  SubmissionTracker,
}
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
  OffsetCheckpointCache,
}
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/** Wrapper and life-cycle manager for the in-memory Ledger API state. */
class InMemoryState(
    val participantId: Ref.ParticipantId,
    val ledgerEndCache: MutableLedgerEndCache,
    val contractStateCaches: ContractStateCaches,
    val offsetCheckpointCache: OffsetCheckpointCache,
    val inMemoryFanoutBuffer: InMemoryFanoutBuffer,
    val stringInterningView: StringInterningView,
    val dispatcherState: DispatcherState,
    val submissionTracker: SubmissionTracker,
    val partyAllocationTracker: PartyAllocation.Tracker,
    val commandProgressTracker: CommandProgressTracker,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  final def initialized: Boolean = dispatcherState.isRunning

  val cachesUpdatedUpto: AtomicReference[Option[Offset]] =
    new AtomicReference[Option[Offset]](None)

  /** (Re-)initializes the participant in-memory state to a specific ledger end.
    *
    * NOTE: This method is not thread-safe. Calling it concurrently leads to undefined behavior.
    */
  final def initializeTo(
      ledgerEndO: Option[LedgerEnd]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    def resetInMemoryState(): Future[Unit] =
      for {
        // First stop the active dispatcher (if exists) to ensure
        // termination of existing Ledger API subscriptions and to also ensure
        // that new Ledger API subscriptions racing with `initializeTo`
        // do not observe an inconsistent state.
        _ <- dispatcherState.stopDispatcher()
        // Reset the Ledger API caches to the latest ledger end
        _ <- Future {
          contractStateCaches.reset(ledgerEndO.map(_.lastOffset))
          inMemoryFanoutBuffer.flush()
          ledgerEndCache.set(ledgerEndO)
          submissionTracker.close()
        }
        // Start a new Ledger API offset dispatcher
        _ = dispatcherState.startDispatcher(ledgerEndO.map(_.lastOffset))
      } yield ()

    def inMemoryStateIsUptodate: Boolean =
      ledgerEndCache() == ledgerEndO &&
        dispatcherState.getDispatcher.getHead() == ledgerEndO.map(_.lastOffset) &&
        cachesUpdatedUpto.get() == ledgerEndO.map(_.lastOffset)

    def ledgerEndComparisonLog: String =
      s"inMemoryLedgerEnd:$ledgerEndCache} persistedLedgerEnd:$ledgerEndO dispatcher-head:${dispatcherState.getDispatcher
          .getHead()} cachesAreUpdateUpto:${cachesUpdatedUpto.get()}"

    if (!dispatcherState.isRunning) {
      logger.info(s"Initializing participant in-memory state to ledger end: $ledgerEndO")
      resetInMemoryState()
    } else if (inMemoryStateIsUptodate) {
      logger.info(
        s"Participant in-memory state is uptodate, continue without reset. $ledgerEndComparisonLog"
      )
      Future.unit
    } else {
      logger.info(
        s"Participant in-memory state/persisted ledger end mismatch: reseting in-memory state. $ledgerEndComparisonLog"
      )
      resetInMemoryState()
    }
  }
}

object InMemoryState {
  def owner(
      participantId: Ref.ParticipantId,
      commandProgressTracker: CommandProgressTracker,
      apiStreamShutdownTimeout: Duration,
      bufferedStreamsPageSize: Int,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Int,
      maxCommandsInFlight: Int,
      metrics: LedgerApiServerMetrics,
      executionContext: ExecutionContext,
      tracer: Tracer,
      loggerFactory: NamedLoggerFactory,
  )(
      mutableLedgerEndCache: MutableLedgerEndCache,
      stringInterningView: StringInterningView,
  )(implicit traceContext: TraceContext): ResourceOwner[InMemoryState] = {
    val initialLedgerEnd = LedgerEnd.beforeBegin

    for {
      dispatcherState <- DispatcherState.owner(apiStreamShutdownTimeout, loggerFactory)
      submissionTracker <- SubmissionTracker.owner(
        maxCommandsInFlight,
        metrics,
        tracer,
        loggerFactory,
      )
      partyAllocationTracker <- StreamTracker
        .owner(
          "party-added",
          (item: PartyAllocation.Completed) => Some(item.submissionId),
          InFlight.Unlimited,
          loggerFactory,
        )
    } yield new InMemoryState(
      participantId = participantId,
      ledgerEndCache = mutableLedgerEndCache,
      dispatcherState = dispatcherState,
      contractStateCaches = ContractStateCaches.build(
        initialLedgerEnd.map(_.lastOffset),
        maxContractStateCacheSize,
        maxContractKeyStateCacheSize,
        metrics,
        loggerFactory,
      )(executionContext),
      offsetCheckpointCache = new OffsetCheckpointCache,
      inMemoryFanoutBuffer = new InMemoryFanoutBuffer(
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        maxBufferedChunkSize = bufferedStreamsPageSize,
        loggerFactory = loggerFactory,
      ),
      stringInterningView = stringInterningView,
      submissionTracker = submissionTracker,
      partyAllocationTracker = partyAllocationTracker,
      commandProgressTracker = commandProgressTracker,
      loggerFactory = loggerFactory,
    )(executionContext)
  }
}
