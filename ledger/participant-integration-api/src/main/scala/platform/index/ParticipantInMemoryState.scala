// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.cache.{EventsBuffer, MutableContractStateCaches}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

// TODO LLP consider pulling in the ledger end cache and string interning view
class ParticipantInMemoryState(
    val mutableContractStateCaches: MutableContractStateCaches,
    val transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    val completionsBuffer: EventsBuffer[TransactionLogUpdate],
    updateLedgerApiLedgerEnd: LedgerEnd => Unit,
    metrics: Metrics,
) {
  private val logger = ContextualizedLogger.get(getClass)
  private val buffersUpdaterExecutionContext =
    ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

  def updateBatch(update: TransactionLogUpdate)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    Timed.future(
      metrics.daml.commands.updateBuffers,
      Future {
        val offset = update.offset
        transactionsBuffer.push(offset, update)
        completionsBuffer.push(offset, update)

        val contractStateEventsBatch = BuffersUpdater
          .convertToContractStateEvents(update)
          .toSeq
        if (contractStateEventsBatch.nonEmpty)
          mutableContractStateCaches.pushBatch(contractStateEventsBatch)

        update match {
          case marker: LedgerEndMarker =>
            val lastEvtSeqIdInBatch = marker.lastEventSeqId
            updateLedgerApiLedgerEnd(LedgerEnd(offset, lastEvtSeqIdInBatch, -1))
            logger.debug(s"Updated ledger end at offset $offset - $lastEvtSeqIdInBatch")
          case _ =>
            logger.debug(s"Updated caches at offset $offset")
        }
      }(buffersUpdaterExecutionContext),
    )
}

object ParticipantInMemoryState {
  def build(
      apiServerConfig: ApiServerConfig,
      metrics: Metrics,
      servicesExecutionContext: ExecutionContext,
      updateLedgerApiLedgerEnd: LedgerEnd => Unit,
  ) = new ParticipantInMemoryState(
    mutableContractStateCaches = MutableContractStateCaches.build(
      maxKeyCacheSize = apiServerConfig.maxContractKeyStateCacheSize,
      maxContractsCacheSize = apiServerConfig.maxContractStateCacheSize,
      metrics = metrics,
    )(servicesExecutionContext),
    completionsBuffer = new EventsBuffer[TransactionLogUpdate](
      maxBufferSize = apiServerConfig.maxTransactionsInMemoryFanOutBufferSize,
      metrics = metrics,
      bufferQualifier = "completions",
      ignoreMarker = _.isInstanceOf[LedgerEndMarker],
    ),
    transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
      // TODO LLP differentiate to completions
      maxBufferSize = apiServerConfig.maxTransactionsInMemoryFanOutBufferSize,
      metrics = metrics,
      bufferQualifier = "transactions",
      ignoreMarker = !_.isInstanceOf[TransactionLogUpdate.TransactionAccepted],
    ),
    updateLedgerApiLedgerEnd = updateLedgerApiLedgerEnd,
    metrics = metrics,
  )
}
