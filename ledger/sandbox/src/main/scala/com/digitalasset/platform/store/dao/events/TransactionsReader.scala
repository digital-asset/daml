// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao.events

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1.{Offset, TransactionId}
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionsResponse
}
import com.digitalasset.platform.ApiOffset
import com.digitalasset.platform.store.dao.{DbDispatcher, PaginatingAsyncStream}
import com.digitalasset.platform.store.SimpleSqlAsVectorOf.SimpleSqlAsVectorOf

import scala.concurrent.{ExecutionContext, Future}

private[dao] object TransactionsReader {

  private def offsetFor(response: GetTransactionsResponse): Offset =
    ApiOffset.assertFromString(response.transactions.head.offset)

  def apply(dispatcher: DbDispatcher, executionContext: ExecutionContext): TransactionsReader =
    new TransactionsReader {

      override def getFlatTransactions(
          startExclusive: Offset,
          endInclusive: Offset,
          filter: FilterRelation,
          pageSize: Int,
          verbose: Boolean,
      ): Source[(Offset, GetTransactionsResponse), NotUsed] = {
        val events =
          PaginatingAsyncStream(pageSize) { offset =>
            val query =
              EventsTable.preparePagedGetFlatTransactions(
                startExclusive = startExclusive,
                endInclusive = endInclusive,
                filter = filter,
                pageSize = pageSize,
                rowOffset = offset,
              )
            dispatcher.executeSql("get_flat_transactions") { implicit connection =>
              query.asVectorOf(EventsTable.flatEventParser(verbose = verbose))
            }
          }

        groupContiguous(events)(by = _.transactionId)
          .flatMapConcat { events =>
            val response = EventsTable.Entry.toGetTransactionsResponse(events)
            Source(response.map(r => offsetFor(r) -> r))
          }
      }

      override def lookupFlatTransactionById(
          transactionId: TransactionId,
          requestingParties: Set[Party],
      ): Future[Option[GetFlatTransactionResponse]] = {
        val query = EventsTable.prepareLookupFlatTransactionById(transactionId, requestingParties)
        dispatcher
          .executeSql(
            description = "lookup_flat_transaction_by_id",
            extraLog = Some(s"tx: $transactionId, parties = ${requestingParties.mkString(", ")}"),
          ) { implicit connection =>
            query.as(EventsTable.flatEventParser(verbose = true).*)
          }
          .map(EventsTable.Entry.toGetFlatTransactionResponse)(executionContext)
      }

      override def lookupTransactionTreeById(
          transactionId: TransactionId,
          requestingParties: Set[Party],
      ): Future[Option[GetTransactionResponse]] = {
        val query = EventsTable.prepareLookupTransactionTreeById(transactionId, requestingParties)
        dispatcher
          .executeSql(
            description = "lookup_transaction_tree_by_id",
            extraLog = Some(s"tx: $transactionId, parties = ${requestingParties.mkString(", ")}"),
          ) { implicit connection =>
            query.as(EventsTable.treeEventParser(verbose = true).*)
          }
          .map(EventsTable.Entry.toTransactionTree)(executionContext)
      }

    }
}

private[dao] trait TransactionsReader {

  def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: Map[Party, Set[Identifier]],
      pageSize: Int,
      verbose: Boolean,
  ): Source[(Offset, GetTransactionsResponse), NotUsed]

  def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetFlatTransactionResponse]]

  def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetTransactionResponse]]

}
