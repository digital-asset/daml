// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.metrics.Timed
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawAssignEventLegacy,
  RawEventLegacy,
  RawReassignmentEventLegacy,
  RawUnassignEventLegacy,
}
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, EventProjectionProperties}
import com.digitalasset.canton.platform.{InternalEventFormat, Party, TemplatePartiesFilter}

import scala.concurrent.{ExecutionContext, Future}

final class ReassignmentPointwiseReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val metrics: LedgerApiServerMetrics,
    val lfValueTranslation: LfValueTranslation,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  val directEC: DirectExecutionContext = DirectExecutionContext(logger)

  private def fetchRawReassignmentEvents(
      eventSeqIdRange: IdRange,
      requestingParties: Option[Set[Party]],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Vector[Entry[RawReassignmentEventLegacy]]] = for {
    assignEvents: Vector[Entry[RawReassignmentEventLegacy]] <-
      dbDispatcher.executeSql(
        dbMetrics.reassignmentPointwise.fetchEventAssignPayloadsLegacy
      )(
        eventStorageBackend.assignEventBatchLegacy(
          eventSeqIdRange,
          requestingParties,
        )
      )

    unassignEvents: Vector[Entry[RawReassignmentEventLegacy]] <-
      dbDispatcher.executeSql(
        dbMetrics.reassignmentPointwise.fetchEventUnassignPayloadsLegacy
      )(
        eventStorageBackend.unassignEventBatchLegacy(
          eventSeqIdRange,
          requestingParties,
        )
      )

  } yield {
    (assignEvents ++ unassignEvents).sortBy(_.eventSequentialId)
  }

  private def toApiAssigned(eventProjectionProperties: EventProjectionProperties)(
      rawAssignEntries: Seq[Entry[RawAssignEventLegacy]]
  )(implicit lc: LoggingContextWithTrace): Future[Option[Reassignment]] =
    Timed.future(
      future = Future.delegate {
        implicit val ec: ExecutionContext =
          directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
        UpdateReader.toApiAssigned(eventProjectionProperties, lfValueTranslation)(rawAssignEntries)
      },
      timer = dbMetrics.reassignmentPointwise.translationTimer,
    )

  def entriesToReassignment(
      eventProjectionProperties: EventProjectionProperties
  )(
      rawReassignmentEntries: Seq[Entry[RawReassignmentEventLegacy]]
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Option[Reassignment]] = for {
    assignO <- toApiAssigned(eventProjectionProperties)(
      rawReassignmentEntries.collect(entry =>
        entry.event match {
          case rawAssign: RawAssignEventLegacy => entry.copy(event = rawAssign)
        }
      )
    )
    unassignO = UpdateReader.toApiUnassigned(
      rawReassignmentEntries.collect(entry =>
        entry.event match {
          case rawUnassign: RawUnassignEventLegacy => entry.copy(event = rawUnassign)
        }
      )
    )

  } yield assignO.orElse(unassignO)

  private def fetchAndFilterEvents[T <: RawEventLegacy](
      fetchRawEvents: Future[Vector[Entry[T]]],
      templatePartiesFilter: TemplatePartiesFilter,
      toResponse: Seq[Entry[T]] => Future[Option[Reassignment]],
  ): Future[Option[Reassignment]] =
    for {
      // Fetching all events from the event sequential id range
      rawEvents <- fetchRawEvents
      // Filtering by template filters
      filteredRawEvents = UpdateReader.filterRawEvents(templatePartiesFilter)(rawEvents)
      // Deserialization of lf values
      deserialized <- toResponse(filteredRawEvents)
    } yield {
      deserialized
    }

  def lookupReassignmentBy(
      eventSeqIdRange: (Long, Long),
      internalEventFormat: InternalEventFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[Reassignment]] = {
    val requestingParties: Option[Set[Party]] =
      internalEventFormat.templatePartiesFilter.allFilterParties
    val eventProjectionProperties: EventProjectionProperties =
      internalEventFormat.eventProjectionProperties
    val templatePartiesFilter = internalEventFormat.templatePartiesFilter
    val (firstEventSeqId, lastEventSeqId) = eventSeqIdRange

    fetchAndFilterEvents[RawReassignmentEventLegacy](
      fetchRawEvents = fetchRawReassignmentEvents(
        eventSeqIdRange = IdRange(firstEventSeqId, lastEventSeqId),
        requestingParties = requestingParties,
      ),
      templatePartiesFilter = templatePartiesFilter,
      toResponse = entriesToReassignment(eventProjectionProperties),
    )
  }

}
