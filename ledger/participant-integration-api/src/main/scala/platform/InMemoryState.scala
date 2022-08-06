// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.services.tracking.SubmissionTracker
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
}
import com.daml.platform.store.interning.{StringInterningView, UpdatingStringInterningView}
import com.daml.platform.store.packagemeta.PackageMetadataView

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

/** Wrapper and life-cycle manager for the in-memory Ledger API state. */
private[platform] class InMemoryState(
    val ledgerEndCache: MutableLedgerEndCache,
    val contractStateCaches: ContractStateCaches,
    val inMemoryFanoutBuffer: InMemoryFanoutBuffer,
    val stringInterningView: StringInterningView,
    val dispatcherState: DispatcherState,
    val packageMetadataView: PackageMetadataView,
    val submissionTracker: SubmissionTracker,
)(implicit executionContext: ExecutionContext) {
  private val logger = ContextualizedLogger.get(getClass)

  final def initialized: Boolean = dispatcherState.isRunning

  /** (Re-)initializes the participant in-memory state to a specific ledger end.
    *
    * NOTE: This method is not thread-safe. Calling it concurrently leads to undefined behavior.
    */
  final def initializeTo(ledgerEnd: LedgerEnd)(
      updateStringInterningView: (UpdatingStringInterningView, LedgerEnd) => Future[Unit],
      updatePackageMetadataView: PackageMetadataView => Future[Unit],
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    logger.info(s"Initializing participant in-memory state to ledger end: $ledgerEnd")

    for {
      // First stop the active dispatcher (if exists) to ensure
      // termination of existing Ledger API subscriptions and to also ensure
      // that new Ledger API subscriptions racing with `initializeTo`
      // do not observe an inconsistent state.
      _ <- dispatcherState.stopDispatcher()
      // Reset the string interning view to the latest ledger end
      _ <- updateStringInterningView(stringInterningView, ledgerEnd)
      // Reset the package metadata view
      _ <- updatePackageMetadataView(packageMetadataView)
      // Reset the Ledger API caches to the latest ledger end
      _ <- Future {
        contractStateCaches.reset(ledgerEnd.lastOffset)
        inMemoryFanoutBuffer.flush()
        ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
      }
      // Start a new Ledger API offset dispatcher
      _ = dispatcherState.startDispatcher(ledgerEnd.lastOffset)
    } yield ()
  }
}

object InMemoryState {
  def owner(
      apiStreamShutdownTimeout: Duration,
      bufferedStreamsPageSize: Int,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Int,
      metrics: Metrics,
      executionContext: ExecutionContext,
  )(implicit loggingContext: LoggingContext): ResourceOwner[InMemoryState] = {
    val initialLedgerEnd = LedgerEnd.beforeBegin

    for {
      dispatcherState <- DispatcherState.owner(apiStreamShutdownTimeout)
      submissionTracker <- SubmissionTracker.owner
    } yield new InMemoryState(
      ledgerEndCache = MutableLedgerEndCache()
        .tap(
          _.set((initialLedgerEnd.lastOffset, initialLedgerEnd.lastEventSeqId))
        ),
      dispatcherState = dispatcherState,
      contractStateCaches = ContractStateCaches.build(
        initialLedgerEnd.lastOffset,
        maxContractStateCacheSize,
        maxContractKeyStateCacheSize,
        metrics,
      )(executionContext, loggingContext),
      inMemoryFanoutBuffer = new InMemoryFanoutBuffer(
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        maxBufferedChunkSize = bufferedStreamsPageSize,
      ),
      stringInterningView = new StringInterningView,
      packageMetadataView = PackageMetadataView.create,
      submissionTracker = submissionTracker,
    )(executionContext)
  }
}
