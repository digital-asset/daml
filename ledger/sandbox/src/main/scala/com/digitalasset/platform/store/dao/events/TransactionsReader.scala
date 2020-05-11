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

  // Metrics names
  private val getFlatTransactions: String = "get_flat_transactions"
  private val lookupFlatTransactionById: String = "lookup_flat_transaction_by_id"
  private val getTransactionTrees: String = "get_transaction_trees"
  private val lookupTransactionTreeById: String = "lookup_transaction_tree_by_id"
  private val getActiveContracts: String = "get_active_contracts"

  // Timers for deserialization on per-query basis
  private val getFlatTransactionsDeserializationTimer =
    metrics.daml.index.db.deserialization(getFlatTransactions)
  private val lookupFlatTransactionByIdDeserializationTimer =
    metrics.daml.index.db.deserialization(lookupFlatTransactionById)
  private val getTransactionTreesDeserializationTimer =
    metrics.daml.index.db.deserialization(getTransactionTrees)
  private val lookupTransactionTreeByIdDeserializationTimer =
    metrics.daml.index.db.deserialization(lookupTransactionTreeById)
  private val getActiveContractsDeserializationTimer =
    metrics.daml.index.db.deserialization(getActiveContracts)

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
        dispatcher.executeSql(getFlatTransactions) { implicit connection =>
          query.asVectorOf(EventsTable.rawFlatEventParser)
        }
      }

    groupContiguous(events)(by = _.transactionId)
      .flatMapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionsResponse(
          verbose = verbose,
          deserializationTimer = getFlatTransactionsDeserializationTimer,
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
        description = lookupFlatTransactionById,
        extraLog = Some(s"tx: $transactionId, parties = ${requestingParties.mkString(", ")}"),
      ) { implicit connection =>
        query.asVectorOf(EventsTable.rawFlatEventParser)
      }
      .map(
        EventsTable.Entry.toGetFlatTransactionResponse(
          verbose = true,
          deserializationTimer = lookupFlatTransactionByIdDeserializationTimer,
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
        dispatcher.executeSql(getTransactionTrees) { implicit connection =>
          query.asVectorOf(EventsTable.rawTreeEventParser)
        }
      }

    groupContiguous(events)(by = _.transactionId)
      .flatMapConcat { events =>
        val response = EventsTable.Entry.toGetTransactionTreesResponse(
          verbose = verbose,
          deserializationTimer = getTransactionTreesDeserializationTimer,
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
        description = lookupTransactionTreeById,
        extraLog = Some(s"tx: $transactionId, parties = ${requestingParties.mkString(", ")}"),
      ) { implicit connection =>
        query.asVectorOf(EventsTable.rawTreeEventParser)
      }
      .map(
        EventsTable.Entry.toGetTransactionResponse(
          verbose = true,
          deserializationTimer = lookupTransactionTreeByIdDeserializationTimer,
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
        dispatcher.executeSql(getActiveContracts) { implicit connection =>
          query.asVectorOf(EventsTable.rawFlatEventParser)
        }
      }

    groupContiguous(events)(by = _.transactionId)
      .flatMapConcat { events =>
        Source(
          EventsTable.Entry.toGetActiveContractsResponse(
            verbose = verbose,
            deserializationTimer = getActiveContractsDeserializationTimer,
          )(events)
        )
      }
  }

}
