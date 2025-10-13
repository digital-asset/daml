// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.participant.store.ContractStore
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawAcsDeltaEventLegacy,
  RawCreatedEventLegacy,
  RawEventLegacy,
  RawLedgerEffectsEventLegacy,
}
import com.digitalasset.canton.platform.store.backend.common.{
  EventPayloadSourceForUpdatesAcsDeltaLegacy,
  EventPayloadSourceForUpdatesLedgerEffectsLegacy,
}
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions.toTransaction
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, EventProjectionProperties}
import com.digitalasset.canton.platform.{
  FatContract,
  InternalTransactionFormat,
  Party,
  TemplatePartiesFilter,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.{ExecutionContext, Future}

final class TransactionPointwiseReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val metrics: LedgerApiServerMetrics,
    val lfValueTranslation: LfValueTranslation,
    val queryValidRange: QueryValidRange,
    val contractStore: ContractStore,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  val directEC: DirectExecutionContext = DirectExecutionContext(logger)

  private def fetchRawAcsDeltaEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Option[Set[Party]],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Vector[EventStorageBackend.Entry[RawAcsDeltaEventLegacy]]] = for {
    createEvents <- dbDispatcher.executeSql(
      dbMetrics.updatesAcsDeltaPointwise.fetchEventCreatePayloadsLegacy
    )(
      eventStorageBackend.fetchEventPayloadsAcsDeltaLegacy(target =
        EventPayloadSourceForUpdatesAcsDeltaLegacy.Create
      )(
        eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
        requestingParties = requestingParties,
      )
    )

    consumingEvents <-
      dbDispatcher.executeSql(
        dbMetrics.updatesAcsDeltaPointwise.fetchEventConsumingPayloadsLegacy
      )(
        eventStorageBackend.fetchEventPayloadsAcsDeltaLegacy(target =
          EventPayloadSourceForUpdatesAcsDeltaLegacy.Consuming
        )(
          eventSequentialIds = (IdRange(firstEventSequentialId, lastEventSequentialId)),
          requestingParties = requestingParties,
        )
      )

  } yield {
    (createEvents ++ consumingEvents).sortBy(_.eventSequentialId)
  }

  private def fetchRawLedgerEffectsEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Option[Set[Party]],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Vector[EventStorageBackend.Entry[RawLedgerEffectsEventLegacy]]] = for {
    createEvents <-
      dbDispatcher.executeSql(
        dbMetrics.updatesAcsDeltaPointwise.fetchEventConsumingPayloadsLegacy
      )(
        eventStorageBackend.fetchEventPayloadsLedgerEffectsLegacy(target =
          EventPayloadSourceForUpdatesLedgerEffectsLegacy.Create
        )(
          eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
          requestingParties = requestingParties,
        )
      )

    consumingEvents <-
      dbDispatcher.executeSql(
        dbMetrics.updatesAcsDeltaPointwise.fetchEventConsumingPayloadsLegacy
      )(
        eventStorageBackend.fetchEventPayloadsLedgerEffectsLegacy(target =
          EventPayloadSourceForUpdatesLedgerEffectsLegacy.Consuming
        )(
          eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
          requestingParties = requestingParties,
        )
      )

    nonConsumingEvents <-
      dbDispatcher.executeSql(
        dbMetrics.updatesAcsDeltaPointwise.fetchEventConsumingPayloadsLegacy
      )(
        eventStorageBackend.fetchEventPayloadsLedgerEffectsLegacy(target =
          EventPayloadSourceForUpdatesLedgerEffectsLegacy.NonConsuming
        )(
          eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
          requestingParties = requestingParties,
        )
      )

  } yield {
    (createEvents ++ consumingEvents ++ nonConsumingEvents).sortBy(_.eventSequentialId)
  }

  private def deserializeEntryAcsDelta(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(entry: (Entry[RawAcsDeltaEventLegacy], Option[FatContract]))(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[Event]] =
    UpdateReader.deserializeRawAcsDeltaEvent(eventProjectionProperties, lfValueTranslation)(entry)

  private def deserializeEntryLedgerEffects(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(entry: (Entry[RawLedgerEffectsEventLegacy], Option[FatContract]))(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[Event]] =
    UpdateReader.deserializeRawLedgerEffectsEvent(eventProjectionProperties, lfValueTranslation)(
      entry
    )

  private def fetchAndFilterEvents[T <: RawEventLegacy](
      fetchRawEvents: Future[Vector[Entry[T]]],
      templatePartiesFilter: TemplatePartiesFilter,
      deserializeEntry: ((Entry[T], Option[FatContract])) => Future[Entry[Event]],
      timer: MetricHandle.Timer,
  )(implicit traceContext: TraceContext): Future[Seq[Entry[Event]]] =
    // Fetching all events from the event sequential id range
    fetchRawEvents
      // Filtering by template filters
      .map(UpdateReader.filterRawEvents(templatePartiesFilter))
      // Checking if events are not pruned
      .flatMap(
        queryValidRange.filterPrunedEvents[Entry[T]](entry => Offset.tryFromLong(entry.offset))
      )
      .flatMap(rawEventsPruned =>
        for {
          // Fetching all contracts for the filtered assigned events
          contractsM <- contractStore
            .lookupBatchedNonCached(
              rawEventsPruned.collect(_.event match {
                case created: RawCreatedEventLegacy => created.internalContractId
              })
            )
            .map(_.view.mapValues(_.inst).toMap)
            .failOnShutdownTo(AbortedDueToShutdown.Error().asGrpcError)
          // Deserialization of lf values
          deserialized <-
            Timed.future(
              timer = timer,
              future = Future.delegate {
                implicit val ec: ExecutionContext =
                  directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
                MonadUtil.sequentialTraverse(rawEventsPruned)(entry =>
                  entry.event match {
                    case created: RawCreatedEventLegacy =>
                      val fatContractO = contractsM.get(created.internalContractId)
                      deserializeEntry(entry -> fatContractO)
                    case _ =>
                      deserializeEntry(entry -> None)
                  }
                )
              },
            )
        } yield {
          deserialized
        }
      )

  def lookupTransactionBy(
      eventSeqIdRange: (Long, Long),
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Transaction]] = {
    val requestingParties: Option[Set[Party]] =
      internalTransactionFormat.internalEventFormat.templatePartiesFilter.allFilterParties
    val eventProjectionProperties: EventProjectionProperties =
      internalTransactionFormat.internalEventFormat.eventProjectionProperties
    val templatePartiesFilter = internalTransactionFormat.internalEventFormat.templatePartiesFilter
    val txShape = internalTransactionFormat.transactionShape

    val (firstEventSeqId, lastEventSeqId) = eventSeqIdRange

    val events = txShape match {
      case TransactionShape.AcsDelta =>
        fetchAndFilterEvents(
          fetchRawEvents = fetchRawAcsDeltaEvents(
            firstEventSequentialId = firstEventSeqId,
            lastEventSequentialId = lastEventSeqId,
            requestingParties = requestingParties,
          ),
          templatePartiesFilter = templatePartiesFilter,
          deserializeEntry =
            deserializeEntryAcsDelta(eventProjectionProperties, lfValueTranslation),
          timer = dbMetrics.updatesAcsDeltaPointwise.translationTimer,
        )
      case TransactionShape.LedgerEffects =>
        fetchAndFilterEvents(
          fetchRawEvents = fetchRawLedgerEffectsEvents(
            firstEventSequentialId = firstEventSeqId,
            lastEventSequentialId = lastEventSeqId,
            requestingParties = requestingParties,
          ),
          templatePartiesFilter = templatePartiesFilter,
          deserializeEntry =
            deserializeEntryLedgerEffects(eventProjectionProperties, lfValueTranslation),
          timer = dbMetrics.updatesLedgerEffectsPointwise.translationTimer,
        )
    }

    events.map(entries =>
      // Conversion to API response type
      toTransaction(
        entries = entries,
        transactionShape = txShape,
      )
    )
  }

}
