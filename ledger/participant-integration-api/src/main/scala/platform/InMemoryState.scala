// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
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
    val inMemoryFanoutBuffer: InMemoryFanoutBuffer,
    val stringInterningView: StringInterningView,
    val dispatcherState: DispatcherState,
)(implicit executionContext: ExecutionContext) {
  private val logger = ContextualizedLogger.get(getClass)
  @volatile private var _initialized = false
  @volatile private var _dirty = false

  final def initialized: Boolean = _initialized

  /** (Re-)initializes the participant in-memory state to a specific ledger end.
    *
    * NOTE: This method is not thread-safe. Calling it concurrently leads to undefined behavior.
    */
  final def initializeTo(ledgerEnd: LedgerEnd)(
      updateStringInterningView: (UpdatingStringInterningView, LedgerEnd) => Future[Unit]
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    conditionallyInitialize(ledgerEnd) { () =>
      for {
        // First stop the active dispatcher (if exists) to ensure
        // that Ledger API subscriptions racing with `initializeTo`
        // do not observe an inconsistent state.
        _ <- dispatcherState.stopDispatcher()
        // Reset the string interning view to the latest ledger end
        _ <- updateStringInterningView(stringInterningView, ledgerEnd)
        // Reset the Ledger API caches to the latest ledger end
        _ <- Future {
          contractStateCaches.reset(ledgerEnd.lastOffset)
          inMemoryFanoutBuffer.flush()
          ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
        }
        // Start a new Ledger API offset dispatcher
        _ = dispatcherState.startDispatcher(ledgerEnd)
      } yield {
        // Set the in-memory state initialized
        _initialized = true
        // Reset the dirty flag
        _dirty = false
      }
    }

  final def setDirty(): Unit = _dirty = true

  private def conditionallyInitialize(
      ledgerEnd: LedgerEnd
  )(initialize: () => Future[Unit])(implicit loggingContext: LoggingContext): Future[Unit] =
    if (!_initialized) {
      logger.info(s"Initializing participant in-memory state to ledger end: $ledgerEnd.")
      initialize()
    } else if (_dirty) {
      logger.warn(
        s"Dirty state detected on initialization. Re-setting the in-memory state ledger end: $ledgerEnd."
      )
      initialize()
    } else {
      val currentLedgerEndCache = ledgerEndCache()
      if (
        loggedEqualityCheck[Offset](
          ledgerEnd.lastOffset,
          currentLedgerEndCache._1,
          "ledger end cache offset",
        ) ||
        loggedEqualityCheck[Long](
          ledgerEnd.lastEventSeqId,
          currentLedgerEndCache._2,
          "ledger end cache event sequential id",
        ) || loggedEqualityCheck[Int](
          ledgerEnd.lastStringInterningId,
          stringInterningView.lastId,
          "last string interning id",
        )
      ) {
        logger.warn(
          s"Re-initialization ledger end mismatches in-memory state reference. Re-setting the in-memory state ledger end: $ledgerEnd."
        )
        initialize()
      } else {
        Future.unit
      }
    }

  private def loggedEqualityCheck[T](expected: T, actual: T, name: String)(implicit
      loggingContext: LoggingContext
  ): Boolean =
    (expected != actual)
      .tap { notEqual =>
        if (notEqual)
          logger.warn(
            s"Mismatching state ledger end references for $name: expected ($expected) vs actual ($actual)."
          )
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
      inMemoryFanoutBuffer = new InMemoryFanoutBuffer(
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        maxBufferedChunkSize = bufferedStreamsPageSize,
      ),
      stringInterningView = new StringInterningView,
    )(executionContext)
  }
}
