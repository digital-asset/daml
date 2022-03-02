// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Source, StreamConverters}
import com.google.protobuf

import scala.jdk.CollectionConverters._

object ProtobufByteStrings {

  def readFrom(
      source: Source[akka.util.ByteString, NotUsed]
  )(implicit mat: Materializer): protobuf.ByteString = {
    val inputStream = source.runWith(StreamConverters.asInputStream())
    protobuf.ByteString.readFrom(inputStream)
  }

  def toSource(a: protobuf.ByteString): Source[akka.util.ByteString, NotUsed] = {
    Source.fromIterator(() =>
      a.asReadOnlyByteBufferList().iterator.asScala.map(x => akka.util.ByteString(x))
    )
  }
}
