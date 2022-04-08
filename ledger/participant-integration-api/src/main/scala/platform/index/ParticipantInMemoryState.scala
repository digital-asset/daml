// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.index.ParticipantInMemoryState.toContractStateEvents
import com.daml.platform.store.appendonlydao.events.{Contract, ContractStateEvent, Key, Party}
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

        // Update in-memory fan-out buffers
        transactionsBuffer.push(offset, update)
        completionsBuffer.push(offset, update)

        val contractStateEventsBatch = toContractStateEvents(update)

        if (contractStateEventsBatch.nonEmpty) {
          // Update the mutable contract state cache
          mutableContractStateCaches.pushBatch(contractStateEventsBatch)
        }

        // Update the Ledger API ledger end trigger the general dispatcher
        update match {
          case marker: LedgerEndMarker =>
            val lastEvtSeqIdInBatch = marker.lastEventSeqId
            // We can update the ledger api ledger end only at intervals corresponding to
            // indexer-forwarded batches border, since only there we have the eventSequentialId
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
    // TODO LLP Use specialized types of event buffer entries for completions/transactions
    completionsBuffer = new EventsBuffer[TransactionLogUpdate](
      maxBufferSize = apiServerConfig.maxTransactionsInMemoryFanOutBufferSize,
      metrics = metrics,
      bufferQualifier = "completions",
      ignoreMarker = _.isInstanceOf[LedgerEndMarker],
    ),
    transactionsBuffer = new EventsBuffer[TransactionLogUpdate](
      // TODO LLP differentiate to completions config
      maxBufferSize = apiServerConfig.maxTransactionsInMemoryFanOutBufferSize,
      metrics = metrics,
      bufferQualifier = "transactions",
      ignoreMarker = !_.isInstanceOf[TransactionLogUpdate.TransactionAccepted],
    ),
    updateLedgerApiLedgerEnd = updateLedgerApiLedgerEnd,
    metrics = metrics,
  )

  private val toContractStateEvents: TransactionLogUpdate => Vector[ContractStateEvent] = {
    case tx: TransactionLogUpdate.TransactionAccepted =>
      tx.events.iterator.collect {
        case createdEvent: TransactionLogUpdate.CreatedEvent =>
          ContractStateEvent.Created(
            contractId = createdEvent.contractId,
            contract = Contract(
              template = createdEvent.templateId,
              arg = createdEvent.createArgument,
              agreementText = createdEvent.createAgreementText.getOrElse(""),
            ),
            globalKey = createdEvent.contractKey.map(k =>
              Key.assertBuild(createdEvent.templateId, k.unversioned)
            ),
            ledgerEffectiveTime = createdEvent.ledgerEffectiveTime,
            stakeholders = createdEvent.flatEventWitnesses.map(Party.assertFromString),
            eventOffset = createdEvent.eventOffset,
            eventSequentialId = createdEvent.eventSequentialId,
          )
        case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
          ContractStateEvent.Archived(
            contractId = exercisedEvent.contractId,
            globalKey = exercisedEvent.contractKey.map(k =>
              Key.assertBuild(exercisedEvent.templateId, k.unversioned)
            ),
            stakeholders = exercisedEvent.flatEventWitnesses.map(Party.assertFromString),
            eventOffset = exercisedEvent.eventOffset,
            eventSequentialId = exercisedEvent.eventSequentialId,
          )
      }.toVector
    case TransactionLogUpdate.LedgerEndMarker(eventOffset, eventSequentialId) =>
      Vector(ContractStateEvent.LedgerEndMarker(eventOffset, eventSequentialId))
    case _: TransactionLogUpdate.SubmissionRejected =>
      // Nothing to update the caches with on rejections
      Vector.empty
  }
}
