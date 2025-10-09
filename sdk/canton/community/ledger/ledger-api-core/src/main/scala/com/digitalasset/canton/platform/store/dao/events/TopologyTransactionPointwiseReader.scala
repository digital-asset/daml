// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.digitalasset.canton.ledger.api.TopologyFormat
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawParticipantAuthorization
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.IdRange
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

final class TopologyTransactionPointwiseReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val metrics: LedgerApiServerMetrics,
    val lfValueTranslation: LfValueTranslation,
    val queryValidRange: QueryValidRange,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  private def fetchRawTopologyEvents(
      firstEventSequentialId: Long,
      lastEventSequentialId: Long,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Vector[RawParticipantAuthorization]] =
    dbDispatcher.executeSql(
      dbMetrics.topologyTransactionsPointwise.fetchTopologyPartyEventPayloads
    )(
      eventStorageBackend.topologyPartyEventBatch(
        IdRange(firstEventSequentialId, lastEventSequentialId)
      )
    )

  private def fetchAndFilterEvents(
      fetchRawEvents: Future[Vector[RawParticipantAuthorization]],
      requestingParties: Option[Set[Party]], // None is a party-wildcard
      toResponse: Vector[RawParticipantAuthorization] => Future[Option[TopologyTransaction]],
  )(implicit traceContext: TraceContext): Future[Option[TopologyTransaction]] =
    // Fetching all events from the event sequential id range
    fetchRawEvents
      // Filter out events that do not include the parties
      .map(
        _.filter(event =>
          requestingParties.fold(true)(parties => parties.map(_.toString).contains(event.partyId))
        )
      )
      // Checking if events are not pruned
      .flatMap(queryValidRange.filterPrunedEvents[RawParticipantAuthorization](_.offset))
      // Convert to api response
      .flatMap(filteredEventsPruned => toResponse(filteredEventsPruned.toVector))

  def lookupTopologyTransaction(
      eventSeqIdRange: (Long, Long),
      topologyFormat: TopologyFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[TopologyTransaction]] = {
    // None is a party-wildcard
    val requestingParties: Option[Set[Party]] =
      topologyFormat.participantAuthorizationFormat
        .fold[Option[Set[Party]]](Some(Set.empty))(_.parties)
    val (firstEventSeqId, lastEventSeqId) = eventSeqIdRange

    fetchAndFilterEvents(
      fetchRawEvents = fetchRawTopologyEvents(
        firstEventSequentialId = firstEventSeqId,
        lastEventSequentialId = lastEventSeqId,
      ),
      requestingParties = requestingParties,
      toResponse = (topologyEvents: Vector[RawParticipantAuthorization]) =>
        Future.successful(
          TransactionConversions.toTopologyTransaction(topologyEvents).map(_._2)
        ),
    )
  }

}
