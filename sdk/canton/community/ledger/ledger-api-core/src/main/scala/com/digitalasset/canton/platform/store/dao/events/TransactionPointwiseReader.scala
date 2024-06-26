// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.transaction.TreeEvent
import com.daml.ledger.api.v2.update_service.{GetTransactionResponse, GetTransactionTreeResponse}
import com.daml.metrics.{DatabaseMetrics, Timed}
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.Entry
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions
import com.digitalasset.canton.platform.store.dao.events.TransactionsReader.deserializeEntry
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, EventProjectionProperties}
import com.digitalasset.daml.lf.data.Ref

import java.sql.Connection
import scala.concurrent.{ExecutionContext, Future}

sealed trait TransactionPointwiseReader {
  type EventT
  type RawEventT <: Raw[EventT]
  type RespT

  def dbDispatcher: DbDispatcher
  def eventStorageBackend: EventStorageBackend
  def lfValueTranslation: LfValueTranslation
  val metrics: LedgerApiServerMetrics
  val dbMetric: DatabaseMetrics
  implicit def ec: ExecutionContext

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  protected def fetchTransaction(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(connection: Connection): Vector[EventStorageBackend.Entry[RawEventT]]

  protected def toTransactionResponse(
      events: Vector[Entry[EventT]]
  ): Option[RespT]

  final def lookupTransactionById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[RespT]] = {
    val requestingPartiesStrings: Set[String] = requestingParties.toSet[String]
    for {
      // Fetching event sequential id range corresponding to the requested transaction id
      eventSeqIdRangeO <- dbDispatcher.executeSql(dbMetric)(
        eventStorageBackend.transactionPointwiseQueries.fetchIdsFromTransactionMeta(transactionId =
          transactionId
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
                eventProjectionProperties = eventProjectionProperties,
              )
            )
            // Filtering by requesting parties
            filteredRawEvents = rawEvents.filter(
              _.event.witnesses.exists(requestingPartiesStrings)
            )
            // Deserialization of lf values
            deserialized <- Timed.value(
              timer = dbMetric.translationTimer,
              value = Future.traverse(filteredRawEvents)(
                deserializeEntry(eventProjectionProperties, lfValueTranslation)
              ),
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
)(implicit val ec: ExecutionContext)
    extends TransactionPointwiseReader {

  override type EventT = TreeEvent
  override type RawEventT = Raw.TreeEvent
  override type RespT = GetTransactionTreeResponse

  override val dbMetric: DatabaseMetrics = dbMetrics.lookupTransactionTreeById

  override protected def fetchTransaction(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(connection: Connection): Vector[Entry[RawEventT]] = {
    eventStorageBackend.transactionPointwiseQueries.fetchTreeTransactionEvents(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      requestingParties = requestingParties,
    )(connection)
  }

  override protected def toTransactionResponse(events: Vector[Entry[EventT]]): Option[RespT] = {
    TransactionConversions.toGetTransactionResponse(events)
  }
}

final class TransactionFlatPointwiseReader(
    override val dbDispatcher: DbDispatcher,
    override val eventStorageBackend: EventStorageBackend,
    override val metrics: LedgerApiServerMetrics,
    override val lfValueTranslation: LfValueTranslation,
)(implicit val ec: ExecutionContext)
    extends TransactionPointwiseReader {

  override type EventT = Event
  override type RawEventT = Raw.FlatEvent
  override type RespT = GetTransactionResponse

  override val dbMetric: DatabaseMetrics = dbMetrics.lookupFlatTransactionById

  override protected def fetchTransaction(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(connection: Connection): Vector[EventStorageBackend.Entry[RawEventT]] = {
    eventStorageBackend.transactionPointwiseQueries.fetchFlatTransactionEvents(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      requestingParties = requestingParties,
    )(connection)
  }

  override protected def toTransactionResponse(events: Vector[Entry[EventT]]): Option[RespT] = {
    TransactionConversions.toGetFlatTransactionResponse(events)
  }
}
