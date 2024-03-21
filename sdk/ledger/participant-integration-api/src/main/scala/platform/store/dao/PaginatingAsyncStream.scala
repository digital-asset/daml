// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

private[platform] object PaginatingAsyncStream {

  /** Concatenates the results of multiple asynchronous calls into
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
    * @param pageSize  number of items to retrieve per call
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
          }(ExecutionContext.parasitic)
      }
      .flatMapConcat(Source(_))
  }

  /** Concatenates the results of multiple asynchronous calls into
    * a single [[Source]], passing the last seen event's offset to the
    * next iteration query, so it can continue reading events from this point.
    *
    * This is to implement pagination based on generic offset.
    * The main purpose of the pagination is to break down large queries
    * into smaller batches. The reason for this is that we are currently using
    * simple blocking JDBC APIs and a long-running stream would end up
    * occupying a thread in the DB pool, severely limiting the ability
    * of keeping multiple, concurrent, long-running streams while serving
    * lookup calls.
    *
    * @param startFromOffset initial offset
    * @param getOffset       function that returns a position/offset from the element of type [[T]]
    * @param query           a function that fetches results starting from provided offset
    * @tparam Off the type of the offset
    * @tparam T   the type of the items returned in each call
    */
  def streamFrom[Off, T](startFromOffset: Off, getOffset: T => Off)(
      query: Off => Future[Vector[T]]
  ): Source[T, NotUsed] = {
    Source
      .unfoldAsync(Option(startFromOffset)) {
        case None =>
          Future.successful(None) // finished reading the whole thing
        case Some(offset) =>
          query(offset).map { result =>
            val nextPageOffset: Option[Off] = result.lastOption.map(getOffset)
            Some((nextPageOffset, result))
          }(ExecutionContext.parasitic)
      }
      .flatMapConcat(Source(_))
  }
}
