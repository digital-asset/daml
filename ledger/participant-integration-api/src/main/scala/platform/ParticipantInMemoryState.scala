// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{ContractStateCaches, EventsBuffer, MutableLedgerEndCache}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.StringInterningView

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.chaining._

// TODO LLP: Revisit this portion
class ParticipantInMemoryState(
    apiStreamShutdownTimeout: Duration,
    maxContractStateCacheSize: Long,
    maxContractKeyStateCacheSize: Long,
    val transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    val stringInterningView: StringInterningView,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext) {
  private val logger = ContextualizedLogger.get(getClass)

  @volatile private var _ledgerEndCache: Option[MutableLedgerEndCache] = None
  @volatile private var _contractStateCaches: Option[ContractStateCaches] = None
  @volatile private var _ledgerApiDispatcher: Option[Dispatcher[Offset]] = None

  lazy val ledgerEndCache: MutableLedgerEndCache =
    _ledgerEndCache.getOrElse(throw new RuntimeException("TODO LLP: Uninitialized"))
  lazy val contractStateCaches: ContractStateCaches =
    _contractStateCaches.getOrElse(throw new RuntimeException("TODO LLP: Uninitialized"))
  lazy val ledgerApiDispatcher: Dispatcher[Offset] =
    _ledgerApiDispatcher.getOrElse(throw new RuntimeException("TODO LLP: Uninitialized"))

  def initialized(ledgerEnd: LedgerEnd)(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[Unit] =
    for {
      ledgerApiDispatcher <- Dispatcher
        .owner[Offset](
          name = "ledger-api",
          zeroIndex = Offset.beforeBegin,
          headAtInitialization = ledgerEnd.lastOffset,
          shutdownTimeout = apiStreamShutdownTimeout,
          onShutdownTimeout = () =>
            logger.warn(
              s"Shutdown of API streams did not finish in ${apiStreamShutdownTimeout.toSeconds} seconds. System shutdown continues."
            ),
        )
    } yield {
      _ledgerApiDispatcher = Some(ledgerApiDispatcher)
      _ledgerEndCache = Some(
        MutableLedgerEndCache().tap(_.set((ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId)))
      )
      _contractStateCaches = Some(
        ContractStateCaches.build(
          ledgerEnd.lastOffset,
          maxContractStateCacheSize,
          maxContractKeyStateCacheSize,
          metrics,
        )(executionContext, loggingContext)
      )
    }

  def reset(lastPersistedLedgerEnd: Offset): Unit =
    _contractStateCaches.synchronized {
      if (_contractStateCaches.nonEmpty)
        contractStateCaches.reset(lastPersistedLedgerEnd)
      transactionsBuffer.flush()
    }
}

object ParticipantInMemoryState {
  def owner(
      apiStreamShutdownTimeout: Duration,
      bufferedStreamsPageSize: Int,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Int,
      metrics: Metrics,
  ): ResourceOwner[ParticipantInMemoryState] = for {
    cachesUpdaterExecutorService <- ResourceOwner.forExecutorService(() =>
      Executors.newSingleThreadExecutor(
        new Thread(_).tap(_.setName("ledger-api-caches-updater-thread"))
      )
    )
    cachesUpdaterExecutionContext = ExecutionContext.fromExecutor(cachesUpdaterExecutorService)
  } yield new ParticipantInMemoryState(
    apiStreamShutdownTimeout = apiStreamShutdownTimeout,
    maxContractStateCacheSize = maxContractStateCacheSize,
    maxContractKeyStateCacheSize = maxContractKeyStateCacheSize,
    transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
      maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
      metrics = metrics,
      bufferQualifier = "transactions",
      maxBufferedChunkSize = bufferedStreamsPageSize,
    ),
    stringInterningView = new StringInterningView,
    metrics = metrics,
  )(cachesUpdaterExecutionContext)
}
