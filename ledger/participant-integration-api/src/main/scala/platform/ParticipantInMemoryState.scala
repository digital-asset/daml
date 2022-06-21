// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{ContractStateCaches, EventsBuffer, MutableLedgerEndCache}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker
import com.daml.platform.store.interning.{StringInterningView, UpdatingStringInterningView}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

/** Wrapper and life-cycle manager for the in-memory participant state. */
private[platform] class ParticipantInMemoryState(
    val ledgerEndCache: MutableLedgerEndCache,
    val contractStateCaches: ContractStateCaches,
    val transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    val stringInterningView: StringInterningView,
    val dispatcherState: DispatcherState,
    updateStringInterningView: (UpdatingStringInterningView, LedgerEnd) => Future[Unit],
)(implicit executionContext: ExecutionContext) {
  @volatile private var _initialized = false
  private val logger = ContextualizedLogger.get(getClass)

  final def initialized: Boolean = _initialized

  /** (Re-)initializes the participant in-memory state to a specific ledger end.
    *
    * NOTE: This method is not thread-safe. Calling it concurrently leads to undefined behavior.
    */
  final def initializeTo(
      ledgerEnd: LedgerEnd
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
      _ <- dispatcherState.reset(ledgerEnd)
    } yield {
      _initialized = true
    }
  }
}

object ParticipantInMemoryState {
  def owner(
      apiStreamShutdownTimeout: Duration,
      bufferedStreamsPageSize: Int,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Int,
      updateStringInterningView: (UpdatingStringInterningView, LedgerEnd) => Future[Unit],
      metrics: Metrics,
      executionContext: ExecutionContext,
  )(implicit loggingContext: LoggingContext): ResourceOwner[ParticipantInMemoryState] = {
    val initialLedgerEnd = LedgerEnd.beforeBegin

    for {
      dispatcherState <- DispatcherState.owner(apiStreamShutdownTimeout)
    } yield new ParticipantInMemoryState(
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
      transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
        maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
        metrics = metrics,
        bufferQualifier = "transactions",
        maxBufferedChunkSize = bufferedStreamsPageSize,
        isRangeEndMarker = _.isInstanceOf[LedgerEndMarker],
      ),
      stringInterningView = new StringInterningView,
      updateStringInterningView = updateStringInterningView,
    )(executionContext)
  }
}
