// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.serialization

import java.io.{ByteArrayOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.daml.platform.store.dao.events.CompressionMetrics

private[platform] object Compression {

  sealed abstract class Algorithm(val id: Option[Int]) {
    // TODO append-only: cleanup. This is not used by new Compression related mechanics in append-only implementation
    final def compress(
        uncompressed: Array[Byte],
        metrics: CompressionMetrics.Field,
    ): Array[Byte] = {
      val output = new ByteArrayOutputStream(uncompressed.length)
      val gzip = compress(output)
      try {
        gzip.write(uncompressed)
      } finally {
        gzip.close()
      }
      val compressed = output.toByteArray
      output.close()
      metrics.compressed.update(compressed.length)
      metrics.uncompressed.update(uncompressed.length)
      compressed
    }

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
