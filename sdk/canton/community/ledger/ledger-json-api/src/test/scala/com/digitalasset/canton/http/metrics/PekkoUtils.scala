// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.metrics

import org.apache.pekko.http.scaladsl.model.HttpEntity
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

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
