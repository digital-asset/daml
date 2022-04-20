// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import com.daml.ledger.offset.Offset
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.index.ParticipantInMemoryState.toContractStateEvents
import com.daml.platform.store.DbType
import com.daml.platform.store.appendonlydao.events.{Contract, ContractStateEvent, Key, Party}
import com.daml.platform.store.backend.StorageBackendFactory
import com.daml.platform.store.cache.{
  EventsBuffer,
  MutableContractStateCaches,
  MutableLedgerEndCache,
}
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.LedgerEndMarker
import com.daml.platform.store.interning.StringInterningView

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

// TODO LLP consider pulling in the ledger end cache and string interning view
class ParticipantInMemoryState(
    val ledgerEndCache: MutableLedgerEndCache,
    val dispatcher: Dispatcher[Offset],
    val mutableContractStateCaches: MutableContractStateCaches,
    val transactionsBuffer: EventsBuffer[TransactionLogUpdate],
    val completionsBuffer: EventsBuffer[TransactionLogUpdate],
    val stringInterningView: StringInterningView,
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
            // We can update the ledger api ledger end only at intervals corresponding to
            // indexer-forwarded batches border, since only there we have the eventSequentialId
            updateLedgerApiLedgerEnd(marker.offset, marker.lastEventSeqId)
          case _ =>
            logger.debug(s"Updated caches at offset $offset")
        }
      }(buffersUpdaterExecutionContext),
    )

  def updateLedgerApiLedgerEnd(lastOffset: Offset, lastEventSequentialId: Long)(implicit
      loggingContext: LoggingContext
  ): Unit = {
    ledgerEndCache.set((lastOffset, lastEventSequentialId))
    // the order here is very important: first we need to make data available for point-wise lookups
    // and SQL queries, and only then we can make it available on the streams.
    // (consider example: completion arrived on a stream, but the transaction cannot be looked up)
    dispatcher.signalNewHead(lastOffset)
    logger.debug(s"Updated ledger end at offset $lastOffset - $lastEventSequentialId")
  }
}

object ParticipantInMemoryState {
  def owner(
      apiServerConfig: ApiServerConfig,
      metrics: Metrics,
      servicesExecutionContext: ExecutionContext,
      jdbcUrl: String,
  ) =
    Dispatcher
      .owner[Offset](
        name = "sql-ledger",
        zeroIndex = Offset.beforeBegin,
        headAtInitialization = Offset.beforeBegin,
      )
      .map(dispatcher =>
        new ParticipantInMemoryState(
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
          stringInterningView = {
            val dbType = DbType.jdbcType(jdbcUrl)
            val storageBackendFactory = StorageBackendFactory.of(dbType)
            val stringInterningStorageBackend =
              storageBackendFactory.createStringInterningStorageBackend

            new StringInterningView(
              loadPrefixedEntries = (fromExclusive, toInclusive, dbDispatcher) =>
                implicit loggingContext =>
                  dbDispatcher.executeSql(metrics.daml.index.db.loadStringInterningEntries) {
                    stringInterningStorageBackend.loadStringInterningEntries(
                      fromExclusive,
                      toInclusive,
                    )
                  }
            )
          },
          metrics = metrics,
          dispatcher = dispatcher,
          ledgerEndCache = MutableLedgerEndCache(),
        )
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
