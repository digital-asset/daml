// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.serialization

import java.io.{InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

private[platform] object Compression {

  sealed abstract class Algorithm(val id: Option[Int]) {
    def compress(stream: OutputStream): OutputStream

    def decompress(stream: InputStream): InputStream
  }

  object Algorithm {

    private val LookupTable = Map[Option[Int], Algorithm](
      None.id -> None,
      GZIP.id -> GZIP,
    )

    private def unknownAlgorithm(id: Option[Int]): Nothing =
      throw new IllegalArgumentException(s"Unknown compression algorithm identifier: $id")

    @throws[IllegalArgumentException](
      "If the byte does not match a known compression algorithm identifier"
    )
    def assertLookup(id: Option[Int]): Algorithm =
      LookupTable.getOrElse(id, unknownAlgorithm(id))

    case object None extends Algorithm(id = Option.empty) {
      override def compress(stream: OutputStream): OutputStream = stream

      override def decompress(stream: InputStream): InputStream = stream
    }

    case object GZIP extends Algorithm(id = Some(1)) {
      override def compress(stream: OutputStream): OutputStream =
        new GZIPOutputStream(stream)

      override def decompress(stream: InputStream): InputStream =
        new GZIPInputStream(stream)
    }

  }

}
