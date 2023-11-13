// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.pekkohttp

import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.apache.pekko.http.scaladsl.model.HttpEntity
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import org.apache.pekko.stream.Materializer

object PekkoUtils {

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
