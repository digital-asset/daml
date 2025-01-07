// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.google.protobuf
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}

import scala.jdk.CollectionConverters.*

object ProtobufByteStrings {

  def readFrom(
      source: Source[org.apache.pekko.util.ByteString, NotUsed]
  )(implicit mat: Materializer): protobuf.ByteString = {
    val inputStream = source.runWith(StreamConverters.asInputStream())
    protobuf.ByteString.readFrom(inputStream)
  }

  def toSource(a: protobuf.ByteString): Source[org.apache.pekko.util.ByteString, NotUsed] =
    Source.fromIterator(() =>
      a.asReadOnlyByteBufferList().iterator.asScala.map(x => org.apache.pekko.util.ByteString(x))
    )
}
