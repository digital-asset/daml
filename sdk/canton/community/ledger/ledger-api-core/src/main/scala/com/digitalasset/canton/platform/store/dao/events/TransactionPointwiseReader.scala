// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.transaction.TreeEvent
import com.daml.ledger.api.v2.update_service.{GetTransactionResponse, GetTransactionTreeResponse}
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  Entry,
  RawEvent,
  RawFlatEvent,
  RawTreeEvent,
}
import com.digitalasset.canton.platform.store.backend.common.TransactionPointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, EventProjectionProperties}
import com.digitalasset.canton.platform.{InternalTransactionFormat, Party, TemplatePartiesFilter}
import com.digitalasset.canton.util.MonadUtil

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

// TODO(#23504) cleanup
sealed trait TransactionPointwiseReaderLegacy {
  type EventT
  type RawEventT <: RawEvent
  type RespT

  def dbDispatcher: DbDispatcher
  def eventStorageBackend: EventStorageBackend
  def lfValueTranslation: LfValueTranslation
  val metrics: LedgerApiServerMetrics
  val dbMetric: DatabaseMetrics
  val directEC: DirectExecutionContext
  implicit def ec: ExecutionContext

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  protected def fetchTransaction(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Set[Party],
  )(connection: Connection): Vector[EventStorageBackend.Entry[RawEventT]]

  protected def deserializeEntry(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(
      entry: Entry[RawEventT]
  )(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[EventT]]

  protected def toTransactionResponse(
      events: Seq[Entry[EventT]]
  ): Option[RespT]

  final def lookupTransactionBy(
      lookupKey: LookupKey,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[RespT]] = {
    val requestingPartiesStrings: Set[String] = requestingParties.toSet[String]
    for {
      // Fetching event sequential id range corresponding to the requested transaction id
      eventSeqIdRangeO <- dbDispatcher.executeSql(dbMetric)(
        eventStorageBackend.transactionPointwiseQueries.fetchIdsFromTransactionMeta(lookupKey =
          lookupKey
        )
      )
      response <- eventSeqIdRangeO match {
        case Some((firstEventSeqId, lastEventSeqId)) =>
          for {
            // Fetching all events from the event sequential id range
            rawEvents <- dbDispatcher.executeSql(dbMetric)(
              fetchTransaction(
                firstEventSequentialId = firstEventSeqId,
                lastEventSequentialId = lastEventSeqId,
                requestingParties = requestingParties,
              )
            )
            // Filtering by requesting parties
            filteredRawEvents = rawEvents.filter(
              _.event.witnessParties.exists(requestingPartiesStrings)
            )
            // Deserialization of lf values
            deserialized <- Timed.value(
              timer = dbMetric.translationTimer,
              value = Future.delegate {
                implicit val ec: ExecutionContext =
                  directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
                MonadUtil.sequentialTraverse(filteredRawEvents)(
                  deserializeEntry(eventProjectionProperties, lfValueTranslation)
                )
              },
            )
          } yield {
            // Conversion to API response type
            toTransactionResponse(deserialized)
          }
        case None => Future.successful[Option[RespT]](None)
      }
    } yield response
  }
}

final class TransactionTreePointwiseReader(
    override val dbDispatcher: DbDispatcher,
    override val eventStorageBackend: EventStorageBackend,
    override val metrics: LedgerApiServerMetrics,
    override val lfValueTranslation: LfValueTranslation,
    override val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends TransactionPointwiseReaderLegacy
    with NamedLogging {

  override type EventT = TreeEvent
  override type RawEventT = RawTreeEvent
  override type RespT = GetTransactionTreeResponse

  override val dbMetric: DatabaseMetrics = dbMetrics.lookupTransactionTreeById
  override val directEC: DirectExecutionContext = DirectExecutionContext(logger)

  override protected def fetchTransaction(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Set[Party],
  )(connection: Connection): Vector[Entry[RawEventT]] =
    eventStorageBackend.transactionPointwiseQueries.fetchTreeTransactionEvents(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      requestingParties = Some(requestingParties),
    )(connection)

  override protected def toTransactionResponse(events: Seq[Entry[EventT]]): Option[RespT] =
    TransactionConversions.toGetTransactionTreeResponse(events)

  override protected def deserializeEntry(
      eventProjectionProperties: EventProjectionProperties,
      lfValueTranslation: LfValueTranslation,
  )(entry: Entry[RawTreeEvent])(implicit
      loggingContext: LoggingContextWithTrace,
      ec: ExecutionContext,
  ): Future[Entry[TreeEvent]] =
    UpdateReader.deserializeTreeEvent(eventProjectionProperties, lfValueTranslation)(entry)
}

final class TransactionPointwiseReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val metrics: LedgerApiServerMetrics,
    val lfValueTranslation: LfValueTranslation,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  val dbMetric: DatabaseMetrics = dbMetrics.lookupPointwiseTransaction
  val directEC: DirectExecutionContext = DirectExecutionContext(logger)

  private def fetchRawFlatEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Option[Set[Party]],
  )(connection: Connection): Vector[EventStorageBackend.Entry[RawFlatEvent]] =
    eventStorageBackend.transactionPointwiseQueries.fetchFlatTransactionEvents(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      requestingParties = requestingParties,
    )(connection)

  private def fetchRawTreeEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Option[Set[Party]],
  )(connection: Connection): Vector[EventStorageBackend.Entry[RawTreeEvent]] =
    eventStorageBackend.transactionPointwiseQueries.fetchTreeTransactionEvents(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      requestingParties = requestingParties,
    )(connection)

  private def toTransactionResponse(events: Seq[Entry[Event]]): Option[GetTransactionResponse] =
    TransactionConversions.toGetTransactionResponse(events)

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

  private def filterRawEvents[T <: RawEvent](templatePartiesFilter: TemplatePartiesFilter)(
      rawEvents: Seq[Entry[T]]
  ): Seq[Entry[T]] = {
    val templateWildcardPartiesO = templatePartiesFilter.templateWildcardParties
    val templateSpecifiedPartiesMap = templatePartiesFilter.relation.map {
      case (identifier, partiesO) => (identifier, partiesO.map(_.map(_.toString)))
    }

    templateWildcardPartiesO match {
      // the filter allows all parties for all templates (wildcard)
      case None => rawEvents
      case Some(templateWildcardParties) =>
        val templateWildcardPartiesStrings: Set[String] = templateWildcardParties.toSet[String]
        rawEvents.filter(entry =>
          // at least one of the witnesses exist in the template wildcard filter
          entry.event.witnessParties.exists(templateWildcardPartiesStrings) ||
            (templateSpecifiedPartiesMap.get(entry.event.templateId) match {
              // the event's template id was not found in the filters
              case None => false
              case Some(partiesO) => partiesO.fold(true)(entry.event.witnessParties.exists)
            })
        )
    }
  }

  private def fetchAndFilterEvents[T <: RawEvent](
      fetchRawEvents: Connection => Vector[Entry[T]],
      templatePartiesFilter: TemplatePartiesFilter,
      deserializeEntry: Entry[T] => Future[Entry[Event]],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Seq[Entry[Event]]] =
    for {
      // Fetching all events from the event sequential id range
      rawEvents <- dbDispatcher.executeSql(dbMetric)(fetchRawEvents)
      // Filtering by template filters
      filteredRawEvents = filterRawEvents(templatePartiesFilter)(rawEvents)
      // Deserialization of lf values
      deserialized <- Timed.value(
        timer = dbMetric.translationTimer,
        value = Future.delegate {
          implicit val ec: ExecutionContext =
            directEC // Scala 2 implicit scope override: shadow the outer scope's implicit by name
          MonadUtil.sequentialTraverse(filteredRawEvents)(deserializeEntry)
        },
      )
    } yield {
      deserialized
    }

  def lookupTransactionBy(
      lookupKey: LookupKey,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] = {
    val requestingParties: Option[Set[Party]] =
      internalTransactionFormat.internalEventFormat.templatePartiesFilter.allFilterParties
    val eventProjectionProperties: EventProjectionProperties =
      internalTransactionFormat.internalEventFormat.eventProjectionProperties
    val templatePartiesFilter = internalTransactionFormat.internalEventFormat.templatePartiesFilter
    val txShape = internalTransactionFormat.transactionShape

    for {
      // Fetching event sequential id range corresponding to the requested transaction id
      eventSeqIdRangeO <- dbDispatcher.executeSql(dbMetric)(
        eventStorageBackend.transactionPointwiseQueries.fetchIdsFromTransactionMeta(lookupKey =
          lookupKey
        )
      )
      response <- eventSeqIdRangeO match {
        case Some((firstEventSeqId, lastEventSeqId)) =>
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
              )
          }
          events.map(
            // Conversion to API response type
            toTransactionResponse
          )
        case None => Future.successful[Option[GetTransactionResponse]](None)
      }
    } yield response
  }

}
