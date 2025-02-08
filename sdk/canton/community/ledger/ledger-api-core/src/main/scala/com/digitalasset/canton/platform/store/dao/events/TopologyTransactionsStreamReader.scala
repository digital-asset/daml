// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ParticipantAuthorizationFormat
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.Party
import com.digitalasset.canton.platform.store.backend.EventStorageBackend
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.RawParticipantAuthorization
import com.digitalasset.canton.platform.store.dao.PaginatingAsyncStream.IdPaginationState
import com.digitalasset.canton.platform.store.dao.events.EventsTable.TransactionConversions
import com.digitalasset.canton.platform.store.dao.events.TopologyTransactionsStreamReader.{
  IdDbQuery,
  PayloadDbQuery,
  TopologyTransactionsStreamQueryParams,
}
import com.digitalasset.canton.platform.store.dao.{DbDispatcher, PaginatingAsyncStream}
import com.digitalasset.canton.platform.store.utils.{
  ConcurrencyLimiter,
  QueueBasedConcurrencyLimiter,
}
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Attributes
import org.apache.pekko.stream.scaladsl.Source

import java.sql.Connection
import scala.concurrent.ExecutionContext
import scala.util.chaining.*

class TopologyTransactionsStreamReader(
    globalIdQueriesLimiter: ConcurrencyLimiter,
    globalPayloadQueriesLimiter: ConcurrencyLimiter,
    experimentalEnableTopologyEvents: Boolean,
    dbDispatcher: DbDispatcher,
    queryValidRange: QueryValidRange,
    eventStorageBackend: EventStorageBackend,
    metrics: LedgerApiServerMetrics,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

  private val dbMetrics = metrics.index.db

  def streamTopologyTransactions(
      topologyTransactionsStreamQueryParams: TopologyTransactionsStreamQueryParams
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(Offset, TopologyTransaction), NotUsed] = {
    import topologyTransactionsStreamQueryParams.*

    val assignedEventIdQueriesLimiter =
      new QueueBasedConcurrencyLimiter(maxParallelIdQueries, executionContext)

    def fetchIds(
        maxParallelIdQueriesLimiter: QueueBasedConcurrencyLimiter,
        maxOutputBatchCount: Int,
        metric: DatabaseMetrics,
        idDbQuery: IdDbQuery,
    )(implicit
        loggingContext: LoggingContextWithTrace
    ): Source[Iterable[Long], NotUsed] = {
      val partiesO: Vector[Option[Party]] = participantAuthorizationFormat.parties match {
        case Some(parties) => parties.map(Some(_)).toVector
        // fetch ids for all the parties
        case None => Vector(None)
      }
      partiesO
        .map { partyO =>
          paginatingAsyncStream.streamIdsFromSeekPagination(
            idPageSizing = idPageSizing,
            idPageBufferSize = maxPagesPerIdPagesBuffer,
            initialFromIdExclusive = queryRange.startInclusiveEventSeqId,
          )(
            fetchPage = (state: IdPaginationState) => {
              maxParallelIdQueriesLimiter.execute {
                globalIdQueriesLimiter.execute {
                  dbDispatcher.executeSql(metric) {
                    if (experimentalEnableTopologyEvents)
                      idDbQuery.fetchIds(
                        stakeholder = partyO,
                        startExclusive = state.fromIdExclusive,
                        endInclusive = queryRange.endInclusiveEventSeqId,
                        limit = state.pageSize,
                      )
                    else
                      _ => Vector.empty
                  }
                }
              }
            }
          )
        }
        .pipe(EventIdsUtils.sortAndDeduplicateIds)
        .batchN(
          maxBatchSize = maxPayloadsPerPayloadsPage,
          maxBatchCount = maxOutputBatchCount,
        )
    }

    def fetchPayloads(
        ids: Source[Iterable[Long], NotUsed],
        maxParallelPayloadQueries: Int,
        dbMetric: DatabaseMetrics,
        payloadDbQuery: PayloadDbQuery,
    ): Source[RawParticipantAuthorization, NotUsed] = {
      // Pekko requires for this buffer's size to be a power of two.
      val inputBufferSize = Utils.largestSmallerOrEqualPowerOfTwo(maxParallelPayloadQueries)
      ids.async
        .addAttributes(Attributes.inputBuffer(initial = inputBufferSize, max = inputBufferSize))
        .mapAsync(maxParallelPayloadQueries)(ids =>
          payloadQueriesLimiter.execute {
            globalPayloadQueriesLimiter.execute {
              dbDispatcher.executeSql(dbMetric) { implicit connection =>
                queryValidRange.withRangeNotPruned(
                  minOffsetInclusive = queryRange.startInclusiveOffset,
                  maxOffsetInclusive = queryRange.endInclusiveOffset,
                  errorPruning = (prunedOffset: Offset) =>
                    s"Topology events request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} precedes pruned offset ${prunedOffset.unwrap}",
                  errorLedgerEnd = (ledgerEndOffset: Option[Offset]) =>
                    s"Topology events request from ${queryRange.startInclusiveOffset.unwrap} to ${queryRange.endInclusiveOffset.unwrap} is beyond ledger end offset ${ledgerEndOffset
                        .fold(0L)(_.unwrap)}",
                ) {
                  payloadDbQuery.fetchPayloads(eventSequentialIds = ids)(connection)
                }
              }
            }
          }
        )
        .mapConcat(identity)
    }

    val ids =
      fetchIds(
        maxParallelIdQueriesLimiter = assignedEventIdQueriesLimiter,
        maxOutputBatchCount = maxParallelPayloadQueries + 1,
        metric = dbMetrics.topologyTransactionsStream.fetchTopologyPartyEventIds,
        idDbQuery = eventStorageBackend.fetchTopologyPartyEventIds,
      )
    val payloads =
      fetchPayloads(
        ids = ids,
        maxParallelPayloadQueries = maxParallelPayloadQueries,
        dbMetric = dbMetrics.topologyTransactionsStream.fetchTopologyPartyEventPayloads,
        payloadDbQuery = eventStorageBackend.topologyPartyEventBatch,
      )

    UpdateReader
      .groupContiguous(payloads)(by = _.updateId)
      .mapConcat(TransactionConversions.toTopologyTransaction)
  }

}

object TopologyTransactionsStreamReader {
  final case class TopologyTransactionsStreamQueryParams(
      queryRange: EventsRange,
      payloadQueriesLimiter: ConcurrencyLimiter,
      idPageSizing: IdPageSizing,
      participantAuthorizationFormat: ParticipantAuthorizationFormat,
      maxParallelIdQueries: Int,
      maxPagesPerIdPagesBuffer: Int,
      maxPayloadsPerPayloadsPage: Int,
      maxParallelPayloadQueries: Int,
  )

  @FunctionalInterface
  trait IdDbQuery {
    def fetchIds(
        stakeholder: Option[Party],
        startExclusive: Long,
        endInclusive: Long,
        limit: Int,
    ): Connection => Vector[Long]
  }

  @FunctionalInterface
  trait PayloadDbQuery {
    def fetchPayloads(
        eventSequentialIds: Iterable[Long]
    ): Connection => Vector[RawParticipantAuthorization]
  }
}
