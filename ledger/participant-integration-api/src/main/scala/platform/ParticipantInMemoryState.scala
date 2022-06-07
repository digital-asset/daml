// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import com.daml.ledger.offset.Offset
import com.daml.ledger.resources.ResourceOwner
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.ParticipantInMemoryState.{buildDispatcher, shutdownDispatcher}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.backend.{ParameterStorageBackend, StringInterningStorageBackend}
import com.daml.platform.store.cache.{ContractStateCaches, EventsBuffer, MutableLedgerEndCache}
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interning.{StringInterningView, UpdatingStringInterningView}
import com.daml.timer.Timeout._

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.chaining._

// TODO LLP: Revisit this portion
class ParticipantInMemoryState(
    val ledgerEndCache: MutableLedgerEndCache,
    val contractStateCaches: ContractStateCaches,
    val transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    val stringInterningView: StringInterningView,
    initialLedgerApiDispatcher: Dispatcher[Offset],
    apiStreamShutdownTimeout: Duration,
    metrics: Metrics,
)(implicit executionContext: ExecutionContext) {
  private val logger = ContextualizedLogger.get(getClass)
  @volatile private var _ledgerApiDispatcherOwner = initialLedgerApiDispatcher

  def dispatcher: Dispatcher[Offset] = _ledgerApiDispatcherOwner

  def resetTo(
      ledgerEnd: LedgerEnd,
      dbDispatcher: DbDispatcher,
      stringInterningStorageBackend: StringInterningStorageBackend,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = {
    // TODO LLP: Add closed guard for ParticipantInMemoryState
    logger.info(s"Resetting participant in-memory state to ledger end: $ledgerEnd")

    for {
      _ <- shutdownDispatcher(_ledgerApiDispatcherOwner)(apiStreamShutdownTimeout)
      _ = _ledgerApiDispatcherOwner = buildDispatcher(ledgerEnd)
      _ <- Future {
        contractStateCaches.reset(ledgerEnd.lastOffset)
        transactionsBuffer.flush()
        ledgerEndCache.set(ledgerEnd.lastOffset -> ledgerEnd.lastEventSeqId)
      }
      _ <- updateStringInterningView(
        dbDispatcher = dbDispatcher,
        updatingStringInterningView = stringInterningView,
        ledgerEnd = ledgerEnd,
        stringInterningStorageBackend = stringInterningStorageBackend,
      )
    } yield ()
  }

  private def updateStringInterningView(
      dbDispatcher: DbDispatcher,
      updatingStringInterningView: UpdatingStringInterningView,
      ledgerEnd: ParameterStorageBackend.LedgerEnd,
      stringInterningStorageBackend: StringInterningStorageBackend,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    updatingStringInterningView.update(ledgerEnd.lastStringInterningId)(
      (fromExclusive, toInclusive) =>
        implicit loggingContext =>
          dbDispatcher.executeSql(metrics.daml.index.db.loadStringInterningEntries) {
            stringInterningStorageBackend.loadStringInterningEntries(
              fromExclusive,
              toInclusive,
            )
          }
    )
}

object ParticipantInMemoryState {
  private val logger = ContextualizedLogger.get(getClass)
  def owner(
      ledgerEnd: LedgerEnd,
      apiStreamShutdownTimeout: Duration,
      bufferedStreamsPageSize: Int,
      maxContractStateCacheSize: Long,
      maxContractKeyStateCacheSize: Long,
      maxTransactionsInMemoryFanOutBufferSize: Int,
      metrics: Metrics,
      servicesExecutionContext: ExecutionContext,
      cachesUpdaterExecutionContext: ExecutionContext,
  )(implicit loggingContext: LoggingContext): ResourceOwner[ParticipantInMemoryState] =
    ResourceOwner.forReleasable(() =>
      new ParticipantInMemoryState(
        ledgerEndCache =
          MutableLedgerEndCache().tap(_.set((ledgerEnd.lastOffset, ledgerEnd.lastEventSeqId))),
        initialLedgerApiDispatcher = buildDispatcher(ledgerEnd),
        contractStateCaches = ContractStateCaches.build(
          ledgerEnd.lastOffset,
          maxContractStateCacheSize,
          maxContractKeyStateCacheSize,
          metrics,
        )(servicesExecutionContext, loggingContext),
        transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
          maxBufferSize = maxTransactionsInMemoryFanOutBufferSize,
          metrics = metrics,
          bufferQualifier = "transactions",
          maxBufferedChunkSize = bufferedStreamsPageSize,
        ),
        stringInterningView = new StringInterningView,
        metrics = metrics,
        apiStreamShutdownTimeout = apiStreamShutdownTimeout,
      )(cachesUpdaterExecutionContext)
    ) { participantInMemoryState =>
      shutdownDispatcher(participantInMemoryState.dispatcher)(apiStreamShutdownTimeout)
      participantInMemoryState.dispatcher.shutdown()
    }

  private def buildDispatcher(ledgerEnd: LedgerEnd): Dispatcher[Offset] =
    Dispatcher(
      name = "ledger-api",
      zeroIndex = Offset.beforeBegin,
      headAtInitialization = ledgerEnd.lastOffset,
    )

  private def shutdownDispatcher(
      dispatcher: Dispatcher[Offset]
  )(apiStreamShutdownTimeout: Duration)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    dispatcher
      .shutdown()
      .withTimeout(apiStreamShutdownTimeout)(
        logger.warn(
          s"Shutdown of API streams did not finish in ${apiStreamShutdownTimeout.toSeconds} seconds. System shutdown continues."
        )
      )
}
