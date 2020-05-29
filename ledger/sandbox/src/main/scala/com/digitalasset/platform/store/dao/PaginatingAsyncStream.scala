// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.participant.state.v1.Offset

import scala.concurrent.Future

object PaginatingAsyncStream {

  /**
    * Concatenates the results of multiple asynchronous calls into
    * a single [[Source]], injecting the offset of the next page to
    * retrieve for every call.
    *
    * This is designed to work with database limit/offset pagination and
    * in particular to break down large queries intended to serve streams
    * into smaller ones. The reason for this is that we are currently using
    * simple blocking JDBC APIs and a long-running stream would end up
    * occupying a thread in the DB pool, severely limiting the ability
    * of keeping multiple, concurrent, long-running streams while serving
    * lookup calls.
    *
    * This is not designed to page through results using the "seek method":
    * https://use-the-index-luke.com/sql/partial-results/fetch-next-page
    *
    * @param pageSize number of items to retrieve per call
    * @param queryPage takes the offset from which to start the next page and returns that page
    * @tparam T the type of the items returned in each call
    */
  def apply[T](pageSize: Int)(queryPage: Long => Future[Vector[T]]): Source[T, NotUsed] = {
    Source
      .unfoldAsync(Option(0L)) {
        case None => Future.successful(None)
        case Some(queryOffset) =>
          queryPage(queryOffset).map { result =>
            val resultSize = result.size.toLong
            val newQueryOffset = if (resultSize < pageSize) None else Some(queryOffset + pageSize)
            Some(newQueryOffset -> result)
          }(DirectExecutionContext)
      }
      .flatMapConcat(Source(_))
  }

  /**
    * Concatenates the results of multiple asynchronous calls into
    * a single [[Source]], passing the last seen event's ledger [[Offset]] and node index to the
    * next iteration query, so it can continue reading events from this point.
    *
    * This is to implement pagination based on ledger offset and event node index.
    * The main purpose of the pagination is to break down large queries
    * into smaller batches. The reason for this is that we are currently using
    * simple blocking JDBC APIs and a long-running stream would end up
    * occupying a thread in the DB pool, severely limiting the ability
    * of keeping multiple, concurrent, long-running streams while serving
    * lookup calls.
    *
    * @param initialOffset initial ledger [[Offset]]
    * @param extractOffsetAndNodeIndex function that extracts [[Offset]] and node index from the result entry of type [[T]]
    * @param query a function that takes [[Offset]] and optional node index to start pagination from
    * @tparam T the type of the items returned in each call
    */
  def streamFrom[T](initialOffset: Offset, extractOffsetAndNodeIndex: T => (Offset, Int))(
      query: (Offset, Option[Int]) => Future[Vector[T]]
  ): Source[T, NotUsed] = {
    Source
      .unfoldAsync(Option((initialOffset, Option.empty[Int]))) {
        case None =>
          Future.successful(None) // finished reading the whole thing
        case Some((prevOffset, prevNodeIndex)) =>
          query(prevOffset, prevNodeIndex).map { result =>
            val newState = result.lastOption.map { t =>
              val event: (Offset, Int) = extractOffsetAndNodeIndex(t)
              (event._1, Some(event._2))
            }
            Some((newState, result))
          }(DirectExecutionContext) // run in the same thread as the query, avoid context switch for a cheap operation
      }
      .flatMapConcat(Source(_))
  }
}
