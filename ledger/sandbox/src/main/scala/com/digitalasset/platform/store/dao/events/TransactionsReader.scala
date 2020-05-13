// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{Offset, TransactionId}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset
import com.daml.platform.store.dao.{DbDispatcher, PaginatingAsyncStream}
import com.daml.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf

import scala.concurrent.{ExecutionContext, Future}

/**
  * @param dispatcher Executes the queries prepared by this object
  * @param executionContext Runs transformations on data fetched from the database
  * @param pageSize The number of events to fetch at a time the database when serving streaming calls
  * @see [[PaginatingAsyncStream]]
  */
private[dao] final class TransactionsReader(
    dispatcher: DbDispatcher,
    executionContext: ExecutionContext,
    pageSize: Int,
    metrics: Metrics,
) {

  private val dbMetrics = metrics.daml.index.db

  private def offsetFor(response: GetTransactionsResponse): Offset =
    ApiOffset.assertFromString(response.transactions.head.offset)

  private def offsetFor(response: GetTransactionTreesResponse): Offset =
    ApiOffset.assertFromString(response.transactions.head.offset)

  def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  ): Source[(Offset, GetTransactionsResponse), NotUsed] = {
    val events =
      PaginatingAsyncStream(pageSize) { offset =>
        val query =
          EventsTable
            .preparePagedGetFlatTransactions(
              startExclusive = startExclusive,
              endInclusive = endInclusive,
              filter = filter,
              pageSize = pageSize,
              rowOffset = offset,
            )
            .withFetchSize(Some(pageSize))
        dispatcher.executeSql(dbMetrics.getFlatTransactions) { implicit connection =>
          query.asVectorOf(EventsTable.rawFlatEventParser)
        }
      }

    groupContiguous(events)(by = _.transactionId)
      .flatMapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionsResponse(
          verbose = verbose,
          deserializationTimer = dbMetrics.getFlatTransactions.translationTimer,
        )(events)
        Source(response.map(r => offsetFor(r) -> r))
      }
  }

  def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetFlatTransactionResponse]] = {
    val query = EventsTable.prepareLookupFlatTransactionById(transactionId, requestingParties)
    dispatcher
      .executeSql(
        databaseMetrics = dbMetrics.lookupFlatTransactionById,
        extraLog = Some(s"tx: $transactionId, parties = ${requestingParties.mkString(", ")}"),
      ) { implicit connection =>
        query.asVectorOf(EventsTable.rawFlatEventParser)
      }
      .map(
        EventsTable.Entry.toGetFlatTransactionResponse(
          verbose = true,
          deserializationTimer = dbMetrics.lookupFlatTransactionById.translationTimer,
        )
      )(executionContext)
  }

  def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      verbose: Boolean,
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] = {
    val events =
      PaginatingAsyncStream(pageSize) { offset =>
        val query =
          EventsTable
            .preparePagedGetTransactionTrees(
              startExclusive = startExclusive,
              endInclusive = endInclusive,
              requestingParties = requestingParties,
              pageSize = pageSize,
              rowOffset = offset,
            )
            .withFetchSize(Some(pageSize))
        dispatcher.executeSql(dbMetrics.getTransactionTrees) { implicit connection =>
          query.asVectorOf(EventsTable.rawTreeEventParser)
        }
      }

    groupContiguous(events)(by = _.transactionId)
      .flatMapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionTreesResponse(
          verbose = verbose,
          deserializationTimer = dbMetrics.getTransactionTrees.translationTimer,
        )(events)
        Source(response.map(r => offsetFor(r) -> r))
      }
  }

  def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetTransactionResponse]] = {
    val query = EventsTable.prepareLookupTransactionTreeById(transactionId, requestingParties)
    dispatcher
      .executeSql(
        databaseMetrics = dbMetrics.lookupTransactionTreeById,
        extraLog = Some(s"tx: $transactionId, parties = ${requestingParties.mkString(", ")}"),
      ) { implicit connection =>
        query.asVectorOf(EventsTable.rawTreeEventParser)
      }
      .map(
        EventsTable.Entry.toGetTransactionResponse(
          verbose = true,
          deserializationTimer = dbMetrics.lookupTransactionTreeById.translationTimer,
        ))(executionContext)
  }

  def getActiveContracts(
      activeAt: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  ): Source[GetActiveContractsResponse, NotUsed] = {
    val events =
      PaginatingAsyncStream(pageSize) { offset =>
        val query =
          EventsTable
            .preparePagedGetActiveContracts(
              activeAt = activeAt,
              filter = filter,
              pageSize = pageSize,
              rowOffset = offset,
            )
            .withFetchSize(Some(pageSize))
        dispatcher.executeSql(dbMetrics.getActiveContracts) { implicit connection =>
          query.asVectorOf(EventsTable.rawFlatEventParser)
        }
      }

    groupContiguous(events)(by = _.transactionId)
      .flatMapConcat { events =>
        Source(
          EventsTable.Entry.toGetActiveContractsResponse(
            verbose = verbose,
            deserializationTimer = dbMetrics.getActiveContracts.translationTimer,
          )(events)
        )
      }
  }

}
