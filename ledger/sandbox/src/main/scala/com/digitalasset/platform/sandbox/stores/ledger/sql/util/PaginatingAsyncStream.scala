// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.util

import akka.NotUsed
import akka.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}

object PaginatingAsyncStream {
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
