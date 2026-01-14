// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import scala.concurrent.{ExecutionContext, Future}

final class UpdatePointwiseReader(
    val dbDispatcher: DbDispatcher,
    val eventStorageBackend: EventStorageBackend,
    val parameterStorageBackend: ParameterStorageBackend,
    val metrics: LedgerApiServerMetrics,
    transactionPointwiseReader: TransactionOrReassignmentPointwiseReader,
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
        eventStorageBackend.updatePointwiseQueries.fetchIdsFromUpdateMeta(
          lookupKey = lookupKey
        )
      )

      transactionUpdate: Future[Option[GetUpdateResponse]] =
        eventSeqIdRangeO
          .flatMap(eventSeqIdRange =>
            Option.when(
              internalUpdateFormat.includeReassignments.isDefined ||
                internalUpdateFormat.includeTransactions.isDefined
            )(
              transactionPointwiseReader
                .lookupUpdateBy(eventSeqIdRange, internalUpdateFormat)
            )
          )
          .getOrElse(Future.successful(None))

      topologyTransactionUpdate: Future[Option[GetUpdateResponse]] =
        eventSeqIdRangeO
          .flatMap(eventSeqIdRange =>
            internalUpdateFormat.includeTopologyEvents
              .map(
                topologyTransactionPointwiseReader
                  .lookupTopologyTransaction(eventSeqIdRange, _)
                  .map(
                    _.map(topologyUpdate =>
                      GetUpdateResponse(
                        Update.TopologyTransaction(
                          topologyUpdate
                        )
                      )
                    )
                  )
              )
          )
          .getOrElse(Future.successful(None))

      agg <- Future
        .sequence(
          Seq(
            transactionUpdate,
            topologyTransactionUpdate,
          )
        )
        .map(_.flatten)
    } yield {
      // only a single update should exist for a specific offset or update id
      agg.headOption
    }

}
