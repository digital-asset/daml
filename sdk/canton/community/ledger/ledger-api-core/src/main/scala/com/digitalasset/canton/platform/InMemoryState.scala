// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.apiserver.services.tracking.SubmissionTracker
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
  OffsetCheckpointCache,
}
import com.digitalasset.canton.platform.store.interning.StringInterningView
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

/** Wrapper and life-cycle manager for the in-memory Ledger API state. */
private[platform] class InMemoryState(
    val ledgerEndCache: MutableLedgerEndCache,
    val contractStateCaches: ContractStateCaches,
    val offsetCheckpointCache: OffsetCheckpointCache,
    val inMemoryFanoutBuffer: InMemoryFanoutBuffer,
    val stringInterningView: StringInterningView,
    val dispatcherState: DispatcherState,
    val submissionTracker: SubmissionTracker,
    val commandProgressTracker: CommandProgressTracker,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  final def initialized: Boolean = dispatcherState.isRunning

  /** (Re-)initializes the participant in-memory state to a specific ledger end.
    *
    * NOTE: This method is not thread-safe. Calling it concurrently leads to undefined behavior.
    */
  final def initializeTo(
      ledgerEnd: LedgerEnd
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.info(s"Initializing participant in-memory state to ledger end: $ledgerEnd")

    for {
      // First stop the active dispatcher (if exists) to ensure
      // termination of existing Ledger API subscriptions and to also ensure
      // that new Ledger API subscriptions racing with `initializeTo`
      // do not observe an inconsistent state.
      _ <- dispatcherState.stopDispatcher()
      // Reset the Ledger API caches to the latest ledger end
      _ <- Future {
        contractStateCaches.reset(ledgerEnd.lastOffset)
        inMemoryFanoutBuffer.flush()
        ledgerEndCache.set(
          (ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId, ledgerEnd.lastPublicationTime)
        )
        submissionTracker.close()
      }
      // Start a new Ledger API offset dispatcher
      _ = dispatcherState.startDispatcher(ledgerEnd.lastOffset)
    } yield ()
  }
}

object InMemoryState {
  def owner(
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
    } yield new InMemoryState(
      ledgerEndCache = mutableLedgerEndCache,
      dispatcherState = dispatcherState,
      contractStateCaches = ContractStateCaches.build(
        initialLedgerEnd.lastOffset,
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
      commandProgressTracker = commandProgressTracker,
      loggerFactory = loggerFactory,
    )(executionContext)
  }
}
