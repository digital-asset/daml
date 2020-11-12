// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.serialization

import java.io.{ByteArrayOutputStream, InputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.daml.lf.archive.{Decode, Reader}
import com.daml.lf.value.Value.{ContractId, VersionedValue}
import com.daml.lf.value.{ValueCoder, ValueOuterClass}
import com.daml.metrics.{Metrics, Timed}

private[platform] object ValueSerializer {

  def serializeValue(value: VersionedValue[ContractId], errorContext: => String)(
      implicit metrics: Metrics): Array[Byte] = {
    val serialized = ValueCoder
      .encodeVersionedValueWithCustomVersion(ValueCoder.CidEncoder, value)
      .fold(error => sys.error(s"$errorContext (${error.errorMessage})"), _.toByteArray)
    val compressed = Timed.value(metrics.daml.indexer.compression, compress(serialized))

    metrics.daml.indexer.serializedSize.update(serialized.length)
    metrics.daml.indexer.compressedSize.update(compressed.length)

    compressed
  }

  private def deserializeValueHelper(
      stream: InputStream,
      errorContext: => Option[String],
  ): VersionedValue[ContractId] = {
    val decompressedStream = decompressStream(stream)
    ValueCoder
      .decodeVersionedValue(
        ValueCoder.CidDecoder,
        ValueOuterClass.VersionedValue.parseFrom(
          Decode.damlLfCodedInputStream(decompressedStream, Reader.PROTOBUF_RECURSION_LIMIT)))
      .fold(
        error =>
          sys.error(errorContext.fold(error.errorMessage)(ctx => s"$ctx (${error.errorMessage})")),
        identity
      )
  }

  def deserializeValue(
      stream: InputStream,
  ): VersionedValue[ContractId] =
    deserializeValueHelper(stream, None)

  def deserializeValue(
      stream: InputStream,
      errorContext: => String,
  ): VersionedValue[ContractId] =
    deserializeValueHelper(stream, Some(errorContext))

  private def compress(input: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream(input.length)
    val gzip = new GZIPOutputStream(bos)
    try {
      gzip.write(input)
    } finally {
      gzip.close()
    }
    val compressed = bos.toByteArray
    bos.close()
    compressed
  }

  private def decompressStream(inputStream: InputStream): GZIPInputStream = {
    new GZIPInputStream(inputStream)
  }
}
