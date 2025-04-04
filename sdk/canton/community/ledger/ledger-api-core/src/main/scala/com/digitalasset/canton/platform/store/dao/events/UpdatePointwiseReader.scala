// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.update_service.GetUpdateResponse
import com.daml.ledger.api.v2.update_service.GetUpdateResponse.Update
import com.daml.metrics.DatabaseMetrics
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.InternalUpdateFormat
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.platform.store.backend.{EventStorageBackend, ParameterStorageBackend}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.platform.store.dao.events.UpdatePointwiseReader.getOffset

import scala.concurrent.{ExecutionContext, Future}

final class UpdatePointwiseReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val parameterStorageBackend: ParameterStorageBackend,
    val metrics: LedgerApiServerMetrics,
    transactionPointwiseReader: TransactionPointwiseReader,
    reassignmentPointwiseReader: ReassignmentPointwiseReader,
    topologyTransactionPointwiseReader: TopologyTransactionPointwiseReader,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging {

  protected val dbMetrics: metrics.index.db.type = metrics.index.db

  val dbMetric: DatabaseMetrics = dbMetrics.lookupPointwiseUpdateFetchEventIds

  def lookupUpdateBy(
      lookupKey: LookupKey,
      internalUpdateFormat: InternalUpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]] =
    for {
      // Fetching event sequential id range corresponding to the requested update id or offset
      eventSeqIdRangeO <- dbDispatcher.executeSql(dbMetric)(
        eventStorageBackend.updatePointwiseQueries.fetchIdsFromTransactionMeta(
          lookupKey = lookupKey
        )
      )

      transactionUpdate: Future[Option[Update]] =
        eventSeqIdRangeO
          .flatMap(eventSeqIdRange =>
            internalUpdateFormat.includeTransactions
              .map(
                transactionPointwiseReader
                  .lookupTransactionBy(eventSeqIdRange, _)
                  .map(_.map(Update.Transaction.apply))
              )
          )
          .getOrElse(Future.successful(None))

      reassignmentUpdate: Future[Option[Update]] = eventSeqIdRangeO
        .flatMap(eventSeqIdRange =>
          internalUpdateFormat.includeReassignments.map(
            reassignmentPointwiseReader
              .lookupReassignmentBy(eventSeqIdRange, _)
              .map(_.map(Update.Reassignment.apply))
          )
        )
        .getOrElse(Future.successful(None))

      topologyTransactionUpdate: Future[Option[Update]] =
        eventSeqIdRangeO
          .flatMap(eventSeqIdRange =>
            internalUpdateFormat.includeTopologyEvents
              .map(
                topologyTransactionPointwiseReader
                  .lookupTopologyTransaction(eventSeqIdRange, _)
                  .map(_.map(Update.TopologyTransaction.apply))
              )
          )
          .getOrElse(Future.successful(None))

      agg <- Future
        .sequence(
          Seq(
            transactionUpdate,
            reassignmentUpdate,
            topologyTransactionUpdate,
          )
        )
        .map(_.flatten)

      prunedUpToInclusive <- dbDispatcher.executeSql(metrics.index.db.fetchPruningOffsetsMetrics)(
        parameterStorageBackend.prunedUpToInclusive
      )

      notPruned = agg.filter(update => getOffset(update) > prunedUpToInclusive.fold(0L)(_.unwrap))

    } yield {
      // only a single update should exist for a specific offset or update id
      notPruned.headOption.map(GetUpdateResponse.apply)
    }

}

object UpdatePointwiseReader {
  private def getOffset(update: Update): Long = update match {
    case Update.Empty => throw new RuntimeException("The update was unexpectedly empty.")
    case Update.Transaction(tx) => tx.offset
    case Update.Reassignment(reassignment) => reassignment.offset
    case Update.TopologyTransaction(topologyTx) => topologyTx.offset
  }
}
