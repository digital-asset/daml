// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{
  ContractStateCaches,
  InMemoryFanoutBuffer,
  MutableLedgerEndCache,
}
import com.daml.platform.store.interning.{StringInterningView, UpdatingStringInterningView}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

/** Wrapper and life-cycle manager for the in-memory Ledger API state. */
private[platform] class InMemoryState(
    val ledgerEndCache: MutableLedgerEndCache,
    val contractStateCaches: ContractStateCaches,
    val transactionsBuffer: InMemoryFanoutBuffer,
    val stringInterningView: StringInterningView,
    val dispatcherState: DispatcherState,
)(implicit executionContext: ExecutionContext) {
  private val logger = ContextualizedLogger.get(getClass)

  final def initialized: Boolean = dispatcherState.initialized

  /** (Re-)initializes the participant in-memory state to a specific ledger end.
    *
    * NOTE: This method is not thread-safe. Calling it concurrently leads to undefined behavior.
    */
  final def initializeTo(ledgerEnd: LedgerEnd)(
      updateStringInterningView: (UpdatingStringInterningView, LedgerEnd) => Future[Unit]
  )(implicit loggingContext: LoggingContext): Future[Unit] = {
    logger.info(s"Initializing participant in-memory state to ledger end: $ledgerEnd")

    // TODO LLP: Reset the in-memory state only if the initialization ledgerEnd
    //           is different than the ledgerEndCache.
    for {
      _ <- updateStringInterningView(stringInterningView, ledgerEnd)
      _ <- Future {
        contractStateCaches.reset(ledgerEnd.lastOffset)
        transactionsBuffer.flush()
        ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
      }
      // TODO LLP: Consider the implementation of a two-stage reset with:
      //            - first teardown existing dispatcher
      //            - reset caches
      //            - start new dispatcher
      _ <- dispatcherState.reset(ledgerEnd)
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
      transactionsBuffer = new InMemoryFanoutBuffer(
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        maxBufferedChunkSize = bufferedStreamsPageSize,
      ),
      stringInterningView = new StringInterningView,
    )(executionContext)
  }
}
