// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import java.sql.Connection

import com.daml.ledger.api.v1.event.Event
import com.daml.ledger.api.v1.transaction.TreeEvent
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.{DatabaseMetrics, Metrics, Timed}
import com.daml.platform.Party
import com.daml.platform.store.backend.EventStorageBackend
import com.daml.platform.store.backend.EventStorageBackend.Entry
import com.daml.platform.store.dao.events.EventsTable.TransactionConversions
import com.daml.platform.store.dao.events.TransactionsReader.deserializeEntry
import com.daml.platform.store.dao.{DbDispatcher, EventProjectionProperties}

import scala.concurrent.{ExecutionContext, Future}

sealed trait PointWiseTransactionFetching {
  type EventT
  type RawEventT <: Raw[EventT]
  type RespT

  def dbDispatcher: DbDispatcher
  def eventStorageBackend: EventStorageBackend
  def lfValueTranslation: LfValueTranslation
  val metrics: Metrics
  val dbMetric: DatabaseMetrics
  implicit def ec: ExecutionContext

  protected val dbMetrics: metrics.daml.index.db.type = metrics.daml.index.db

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
  )(implicit loggingContext: LoggingContext): Future[Option[RespT]] = {
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

final class TreeTransactionPointwiseReader(
    override val dbDispatcher: DbDispatcher,
    override val eventStorageBackend: EventStorageBackend,
    override val metrics: Metrics,
    override val lfValueTranslation: LfValueTranslation,
)(implicit val ec: ExecutionContext)
    extends PointWiseTransactionFetching {

  override type EventT = TreeEvent
  override type RawEventT = Raw.TreeEvent
  override type RespT = GetTransactionResponse

  override val dbMetric: DatabaseMetrics = dbMetrics.lookupTransactionTreeById

  override protected def fetchTransaction(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(connection: Connection): Vector[EventStorageBackend.Entry[RawEventT]] = {
    eventStorageBackend.transactionPointwiseQueries.fetchTreeTransaction(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      requestingParties = requestingParties,
    )(connection)
  }

  override protected def toTransactionResponse(events: Vector[Entry[EventT]]): Option[RespT] = {
    TransactionConversions.toGetTransactionResponse(events)
  }
}

final class FlatTransactionPointwiseReader(
    override val dbDispatcher: DbDispatcher,
    override val eventStorageBackend: EventStorageBackend,
    override val metrics: Metrics,
    override val lfValueTranslation: LfValueTranslation,
)(implicit val ec: ExecutionContext)
    extends PointWiseTransactionFetching {

  override type EventT = Event
  override type RawEventT = Raw.FlatEvent
  override type RespT = GetFlatTransactionResponse

  override val dbMetric: DatabaseMetrics = dbMetrics.lookupFlatTransactionById

  override protected def fetchTransaction(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
      requestingParties: Set[Party],
      eventProjectionProperties: EventProjectionProperties,
  )(connection: Connection): Vector[EventStorageBackend.Entry[RawEventT]] = {
    eventStorageBackend.transactionPointwiseQueries.fetchFlatTransaction(
      firstEventSequentialId = firstEventSequentialId,
      lastEventSequentialId = lastEventSequentialId,
      requestingParties = requestingParties,
    )(connection)
  }

  override protected def toTransactionResponse(events: Vector[Entry[EventT]]): Option[RespT] = {
    TransactionConversions.toGetFlatTransactionResponse(events)
  }
}
