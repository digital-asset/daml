// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

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
    * @param executionContext where the offset for the next page is computed
    * @param queryPage takes the offset from which to start the next page and returns that page
    * @tparam T the type of the items returned in each call
    */
  def apply[T](pageSize: Int, executionContext: ExecutionContext)(
      queryPage: Long => Future[List[T]]): Source[T, NotUsed] = {
    Source
      .unfoldAsync(Option(0L)) {
        case None => Future.successful(None)
        case Some(queryOffset) =>
          queryPage(queryOffset).map { result =>
            val resultSize = result.size.toLong
            val newQueryOffset = if (resultSize < pageSize) None else Some(queryOffset + pageSize)
            Some(newQueryOffset -> result)
          }(executionContext)
      }
      .flatMapConcat(Source(_))
  }
}
