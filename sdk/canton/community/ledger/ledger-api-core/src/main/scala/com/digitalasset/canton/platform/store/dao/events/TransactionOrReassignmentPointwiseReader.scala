// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.update_service.GetUpdateResponse
import com.daml.ledger.api.v2.update_service.GetUpdateResponse.Update
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.InternalUpdateFormat
import com.digitalasset.canton.platform.store.LedgerApiContractStore
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawThinAcsDeltaEvent,
  RawThinEvent,
  RawThinLedgerEffectsEvent,
}
import com.digitalasset.canton.platform.store.backend.common.{
  EventPayloadSourceForUpdatesAcsDelta,
  EventPayloadSourceForUpdatesLedgerEffects,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher

import scala.concurrent.{ExecutionContext, Future}

final class TransactionOrReassignmentPointwiseReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val metrics: LedgerApiServerMetrics,
    val lfValueTranslation: LfValueTranslation,
    val queryValidRange: QueryValidRange,
    val contractStore: LedgerApiContractStore,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  val directEC: DirectExecutionContext = DirectExecutionContext(logger)

  private def fetchRawAcsDeltaEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Vector[RawThinAcsDeltaEvent]] = for {
    activateEvents <- dbDispatcher.executeSql(
      dbMetrics.updatesAcsDeltaPointwise.fetchEventActivatePayloads
    )(
      eventStorageBackend.fetchEventPayloadsAcsDelta(target =
        EventPayloadSourceForUpdatesAcsDelta.Activate
      )(
        eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
        requestingPartiesForTx = internalUpdateFormat.includeTransactions
          .flatMap(_.internalEventFormat.templatePartiesFilter.allFilterParties),
        requestingPartiesForReassignment = internalUpdateFormat.includeReassignments
          .flatMap(_.templatePartiesFilter.allFilterParties),
      )
    )
    deactivateEvents <- dbDispatcher.executeSql(
      dbMetrics.updatesAcsDeltaPointwise.fetchEventDeactivatePayloads
    )(
      eventStorageBackend.fetchEventPayloadsAcsDelta(target =
        EventPayloadSourceForUpdatesAcsDelta.Deactivate
      )(
        eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
        requestingPartiesForTx = internalUpdateFormat.includeTransactions
          .flatMap(_.internalEventFormat.templatePartiesFilter.allFilterParties),
        requestingPartiesForReassignment = internalUpdateFormat.includeReassignments
          .flatMap(_.templatePartiesFilter.allFilterParties),
      )
    )
  } yield {
    (activateEvents ++ deactivateEvents).sortBy(_.eventSeqId)
  }

  private def fetchRawLedgerEffectsEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Vector[RawThinLedgerEffectsEvent]] = for {
    activateEvents <- dbDispatcher.executeSql(
      dbMetrics.updatesLedgerEffectsPointwise.fetchEventActivatePayloads
    )(
      eventStorageBackend.fetchEventPayloadsLedgerEffects(target =
        EventPayloadSourceForUpdatesLedgerEffects.Activate
      )(
        eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
        requestingPartiesForTx = internalUpdateFormat.includeTransactions
          .flatMap(_.internalEventFormat.templatePartiesFilter.allFilterParties),
        requestingPartiesForReassignment = internalUpdateFormat.includeReassignments
          .flatMap(_.templatePartiesFilter.allFilterParties),
      )
    )
    deactivateEvents <- dbDispatcher.executeSql(
      dbMetrics.updatesLedgerEffectsPointwise.fetchEventDeactivatePayloads
    )(
      eventStorageBackend.fetchEventPayloadsLedgerEffects(target =
        EventPayloadSourceForUpdatesLedgerEffects.Deactivate
      )(
        eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
        requestingPartiesForTx = internalUpdateFormat.includeTransactions
          .flatMap(_.internalEventFormat.templatePartiesFilter.allFilterParties),
        requestingPartiesForReassignment = internalUpdateFormat.includeReassignments
          .flatMap(_.templatePartiesFilter.allFilterParties),
      )
    )
    variousWitnessedEvents <- dbDispatcher.executeSql(
      dbMetrics.updatesLedgerEffectsPointwise.fetchEventVariousWitnessedPayloads
    )(
      eventStorageBackend.fetchEventPayloadsLedgerEffects(target =
        EventPayloadSourceForUpdatesLedgerEffects.VariousWitnessed
      )(
        eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
        requestingPartiesForTx = internalUpdateFormat.includeTransactions
          .flatMap(_.internalEventFormat.templatePartiesFilter.allFilterParties),
        requestingPartiesForReassignment = internalUpdateFormat.includeReassignments
          .flatMap(_.templatePartiesFilter.allFilterParties),
      )
    )
  } yield {
    (activateEvents ++ deactivateEvents ++ variousWitnessedEvents).sortBy(_.eventSeqId)
  }

  private def fetchAndFilterEvents(
      rawEvents: Future[Vector[RawThinEvent]],
      internalUpdateFormat: InternalUpdateFormat,
      timer: MetricHandle.Timer,
  )(implicit lcwt: LoggingContextWithTrace): Future[Option[GetUpdateResponse]] =
    // Fetching all events from the event sequential id range
    rawEvents
      // Looking up fat contracts if needed
      .flatMap(UpdateReader.withFatContractIfNeeded(contractStore))
      // Checking if events are not pruned
      .flatMap(
        queryValidRange.filterPrunedEvents(entry => Offset.tryFromLong(entry._1.offset))
      )
      // Mapping to fat RawEvents
      .map(UpdateReader.tryToResolveFatInstance)
      // Filtering by template filters
      .map(UpdateReader.filterRawEvents(internalUpdateFormat))
      .flatMap(rawEvents =>
        Timed.future(
          timer = timer,
          future = UpdateReader.toApiUpdate[GetUpdateResponse](
            reassignmentEventProjectionProperties =
              internalUpdateFormat.includeReassignments.map(_.eventProjectionProperties),
            transactionEventProjectionProperties = internalUpdateFormat.includeTransactions
              .map(_.internalEventFormat.eventProjectionProperties),
            lfValueTranslation = lfValueTranslation,
          )(rawEvents)(
            convertReassignment = (r: Reassignment) => GetUpdateResponse(Update.Reassignment(r)),
            convertTransaction = (t: Transaction) => GetUpdateResponse(Update.Transaction(t)),
          ),
        )
      )

  def lookupUpdateBy(
      eventSeqIdRange: (Long, Long),
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]] = {
    val (firstEventSeqId, lastEventSeqId) = eventSeqIdRange

    internalUpdateFormat.includeTransactions.map(_.transactionShape) match {
      case Some(TransactionShape.AcsDelta) =>
        fetchAndFilterEvents(
          rawEvents = fetchRawAcsDeltaEvents(
            firstEventSequentialId = firstEventSeqId,
            lastEventSequentialId = lastEventSeqId,
            internalUpdateFormat = internalUpdateFormat,
          ),
          internalUpdateFormat = internalUpdateFormat,
          timer = dbMetrics.updatesAcsDeltaPointwise.translationTimer,
        )
      case Some(TransactionShape.LedgerEffects) =>
        fetchAndFilterEvents(
          rawEvents = fetchRawLedgerEffectsEvents(
            firstEventSequentialId = firstEventSeqId,
            lastEventSequentialId = lastEventSeqId,
            internalUpdateFormat = internalUpdateFormat,
          ),
          internalUpdateFormat = internalUpdateFormat,
          timer = dbMetrics.updatesLedgerEffectsPointwise.translationTimer,
        )
      case None if internalUpdateFormat.includeReassignments.isDefined =>
        fetchAndFilterEvents(
          rawEvents = fetchRawAcsDeltaEvents(
            firstEventSequentialId = firstEventSeqId,
            lastEventSequentialId = lastEventSeqId,
            internalUpdateFormat = internalUpdateFormat,
          ),
          internalUpdateFormat = internalUpdateFormat,
          timer = dbMetrics.updatesLedgerEffectsPointwise.translationTimer,
        )
      case None =>
        Future.successful(None)
    }
  }
}
