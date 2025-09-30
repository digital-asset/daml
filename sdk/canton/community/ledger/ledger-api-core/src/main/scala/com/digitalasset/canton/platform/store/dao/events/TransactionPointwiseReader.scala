// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.metrics.Timed
import com.daml.metrics.api.MetricHandle
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawEvent,
  RawFlatEvent,
  RawTreeEvent,
}
import com.digitalasset.canton.platform.store.backend.common.{
  EventPayloadSourceForUpdatesAcsDelta,
  EventPayloadSourceForUpdatesLedgerEffects,
}
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions.toTransaction
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, EventProjectionProperties}
import com.digitalasset.canton.platform.{InternalTransactionFormat, Party, TemplatePartiesFilter}
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.{ExecutionContext, Future}

final class TransactionPointwiseReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val metrics: LedgerApiServerMetrics,
    val lfValueTranslation: LfValueTranslation,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  val directEC: DirectExecutionContext = DirectExecutionContext(logger)

  private def fetchRawFlatEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Option[Set[Party]],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Vector[EventStorageBackend.Entry[RawFlatEvent]]] = for {
    createEvents <- dbDispatcher.executeSql(
      dbMetrics.updatesAcsDeltaPointwise.fetchEventCreatePayloads
    )(
      eventStorageBackend.fetchEventPayloadsAcsDelta(target =
        EventPayloadSourceForUpdatesAcsDelta.Create
      )(
        eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
        requestingParties = requestingParties,
      )
    )

    consumingEvents <-
      dbDispatcher.executeSql(
        dbMetrics.updatesAcsDeltaPointwise.fetchEventConsumingPayloads
      )(
        eventStorageBackend.fetchEventPayloadsAcsDelta(target =
          EventPayloadSourceForUpdatesAcsDelta.Consuming
        )(
          eventSequentialIds = (IdRange(firstEventSequentialId, lastEventSequentialId)),
          requestingParties = requestingParties,
        )
      )

  } yield {
    (createEvents ++ consumingEvents).sortBy(_.eventSequentialId)
  }

  private def fetchRawTreeEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Option[Set[Party]],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Vector[EventStorageBackend.Entry[RawTreeEvent]]] = for {
    createEvents <-
      dbDispatcher.executeSql(
        dbMetrics.updatesAcsDeltaPointwise.fetchEventConsumingPayloads
      )(
        eventStorageBackend.fetchEventPayloadsLedgerEffects(target =
          EventPayloadSourceForUpdatesLedgerEffects.Create
        )(
          eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
          requestingParties = requestingParties,
        )
      )

    consumingEvents <-
      dbDispatcher.executeSql(
        dbMetrics.updatesAcsDeltaPointwise.fetchEventConsumingPayloads
      )(
        eventStorageBackend.fetchEventPayloadsLedgerEffects(target =
          EventPayloadSourceForUpdatesLedgerEffects.Consuming
        )(
          eventSequentialIds = IdRange(firstEventSequentialId, lastEventSequentialId),
          requestingParties = requestingParties,
        )
      )

    nonConsumingEvents <-
      dbDispatcher.executeSql(
        dbMetrics.updatesAcsDeltaPointwise.fetchEventConsumingPayloads
      )(
        eventStorageBackend.fetchEventPayloadsLedgerEffects(target =
          EventPayloadSourceForUpdatesLedgerEffects.NonConsuming
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
  )(entry: Entry[RawFlatEvent])(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[Event]] =
    UpdateReader.deserializeRawFlatEvent(eventProjectionProperties, lfValueTranslation)(entry)

  private def deserializeEntryLedgerEffects(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(entry: Entry[RawTreeEvent])(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[Event]] =
    UpdateReader.deserializeRawTreeEvent(eventProjectionProperties, lfValueTranslation)(entry)

  private def fetchAndFilterEvents[T <: RawEvent](
      fetchRawEvents: Future[Vector[Entry[T]]],
      templatePartiesFilter: TemplatePartiesFilter,
      deserializeEntry: Entry[T] => Future[Entry[Event]],
      timer: MetricHandle.Timer,
  ): Future[Seq[Entry[Event]]] =
    for {
      // Fetching all events from the event sequential id range
      rawEvents <- fetchRawEvents
      // Filtering by template filters
      filteredRawEvents = UpdateReader.filterRawEvents(templatePartiesFilter)(rawEvents)
      // Deserialization of lf values
      deserialized <- Timed.future(
        timer = timer,
        future = Future.delegate {
          implicit val ec: ExecutionContext =
            directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
          MonadUtil.sequentialTraverse(filteredRawEvents)(deserializeEntry)
        },
      )
    } yield {
      deserialized
    }

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
          fetchRawEvents = fetchRawFlatEvents(
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
          fetchRawEvents = fetchRawTreeEvents(
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
