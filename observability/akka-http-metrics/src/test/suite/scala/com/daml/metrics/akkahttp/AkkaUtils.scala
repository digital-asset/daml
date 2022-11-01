// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.akkahttp

import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.http.scaladsl.model.HttpEntity
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import akka.stream.Materializer

object AkkaUtils {

  def duplicate(bs: ByteString): ByteString = ByteString(bs.toArray)
  def duplicate(csp: HttpEntity.ChunkStreamPart): HttpEntity.ChunkStreamPart =
    HttpEntity.ChunkStreamPart(
      duplicate(csp.data)
    )
  def duplicateSource[T](
      source: Source[T, Any],
      duplicateElement: T => T,
  )(implicit ec: ExecutionContext, mat: Materializer): Future[Source[T, Any]] =
    source
      .runFold(Vector[T]())((acc, bs) => acc.appended(bs))
      .map(acc => Source(acc.map(duplicateElement)))

}
