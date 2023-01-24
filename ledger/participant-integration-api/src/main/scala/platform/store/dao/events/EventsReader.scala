// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction_service.{
  GetEventsByContractIdResponse,
  GetEventsByContractKeyResponse,
}
import com.daml.ledger.offset.Offset
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, Party}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.logging.LoggingContext
import com.daml.metrics.{DatabaseMetrics, Metrics}
import com.daml.platform
import com.daml.platform.store.EventSequentialId
import com.daml.platform.store.backend.{EventStorageBackend, ParameterStorageBackend}
import com.daml.platform.store.dao.{DbDispatcher, EventProjectionProperties, LedgerDaoEventsReader}

import scala.concurrent.{ExecutionContext, Future}

private[dao] sealed class EventsReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val parameterStorageBackend: ParameterStorageBackend,
    val metrics: Metrics,
    val lfValueTranslation: LfValueTranslation,
)(implicit ec: ExecutionContext)
    extends LedgerDaoEventsReader {

  protected val dbMetrics: metrics.daml.index.db.type = metrics.daml.index.db

  override def getEventsByContractId(contractId: ContractId, requestingParties: Set[Party])(implicit
      loggingContext: LoggingContext
  ): Future[GetEventsByContractIdResponse] = {

    val dbMetric: DatabaseMetrics = dbMetrics.getEventsByContractId

    val eventProjectionProperties = EventProjectionProperties(
      verbose = true, // Used by LfEngineToApi
      witnessTemplateIdFilter = requestingParties
        .map(_ -> Set.empty[Identifier])
        .toMap, // Needed to get create arguments mapped
      witnessInterfaceViewFilter = Map.empty, // We do not need interfaces mapped
    )

    for {
      rawEvents <- dbDispatcher.executeSql(dbMetric)(
        eventStorageBackend.eventReaderQueries.fetchContractIdEvents(
          contractId,
          requestingParties = requestingParties,
        )
      )

      filteredRawEvents = rawEvents.filter(
        _.event.witnesses.exists(requestingParties.map(identity))
      )

      deserialized <- Future.traverse(filteredRawEvents) {
        _.event.applyDeserialization(lfValueTranslation, eventProjectionProperties)
      }

    } yield {
      GetEventsByContractIdResponse(deserialized)
    }
  }

  override def getEventsByContractKey(
      contractKey: Value,
      templateId: Ref.Identifier,
      requestingParties: Set[Party],
      maxEvents: Int,
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Future[GetEventsByContractKeyResponse] = {
    val dbMetric: DatabaseMetrics = dbMetrics.getEventsByContractKey
    val keyHash: String = platform.Key.assertBuild(templateId, contractKey).hash.bytes.toHexString

    val eventProjectionProperties = EventProjectionProperties(
      // Used by LfEngineToApi
      verbose = true,
      // Needed to get create arguments mapped
      witnessTemplateIdFilter = requestingParties.map(_ -> Set.empty[Identifier]).toMap,
      // We do not need interfaces mapped
      witnessInterfaceViewFilter = Map.empty,
    )

    for {

      (rawEvents, prunedOffset) <- dbDispatcher.executeSql(dbMetric) { conn =>
        def lookupSeq(o: Offset) = eventStorageBackend
          .maxEventSequentialIdOfAnObservableEvent(o)(conn)
          .getOrElse(EventSequentialId.beforeBegin)

        val startSeqExclusive = lookupSeq(startExclusive)
        val endSeqInclusive = lookupSeq(endInclusive)

        val prunedOffset = parameterStorageBackend.prunedUpToInclusive(conn)

        val rawEvents = eventStorageBackend.eventReaderQueries.fetchContractKeyEvents(
          keyHash,
          requestingParties,
          maxEvents,
          startSeqExclusive,
          endSeqInclusive,
        )(conn)

        (rawEvents, prunedOffset)
      }

      filteredRawEvents = rawEvents.filter(
        _.event.witnesses.exists(requestingParties.map(identity))
      )

      deserialized <- Future.traverse(filteredRawEvents) {
        _.event.applyDeserialization(lfValueTranslation, eventProjectionProperties)
      }

    } yield {

      def toAbsolute(o: Offset) = LedgerOffset(LedgerOffset.Value.Absolute(o.toHexString))

      val lastOffset = rawEvents.lastOption
        .filter(_ => rawEvents.size >= maxEvents)
        .map(e => toAbsolute(e.eventOffset))
      GetEventsByContractKeyResponse(deserialized, lastOffset, prunedOffset.map(toAbsolute))
    }
  }
}
