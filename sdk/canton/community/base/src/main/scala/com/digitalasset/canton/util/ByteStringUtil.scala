// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Order
import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  MaxByteToDecompressExceeded,
}
import com.google.protobuf.ByteString

import java.io.{ByteArrayOutputStream, EOFException, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream, ZipException}
import scala.annotation.tailrec

object ByteStringUtil {

  /** Lexicographic ordering on [[com.google.protobuf.ByteString]]s */
  val orderByteString: Order[ByteString] = new Order[ByteString] {
    override def compare(x: ByteString, y: ByteString): Int = {
      val iterX = x.iterator()
      val iterY = y.iterator()

      @tailrec def go(): Int =
        if (iterX.hasNext) {
          if (iterY.hasNext) {
            val cmp = iterX.next().compareTo(iterY.next())
            if (cmp == 0) go() else cmp
          } else 1
        } else if (iterY.hasNext) -1
        else 0

      go()
    }
  }

  val orderingByteString: Ordering[ByteString] = orderByteString.toOrdering

  def compressGzip(bytes: ByteString): ByteString = {
    val rawSize = bytes.size()
    val compressed = new ByteArrayOutputStream(rawSize)
    ResourceUtil.withResource(new GZIPOutputStream(compressed)) { gzipper =>
      bytes.writeTo(gzipper)
    }
    ByteString.copyFrom(compressed.toByteArray)
  }

  /** We decompress maximum maxBytesLimit bytes, and if the input is larger we throw
    * MaxBytesToDecompressExceeded error.
    */
  def decompressGzip(
      bytes: ByteString,
      maxBytesLimit: MaxBytesToDecompress,
  ): Either[DeserializationError, ByteString] =
    ResourceUtil
      .withResourceEither(new GZIPInputStream(bytes.newInput())) { gunzipper =>
        // Write to the output stream of the ByteString to avoid building an additional array copy.
        val out = ByteString.newOutput()
        val buf = new Array[Byte](8 * 1024) // 8k is the default used by BufferedInputStream.
        if (copyNBuffered(maxBytesLimit.limit.value, buf, gunzipper, out)) {
          Right(out.toByteString()) // No need to close as data is in-memory.
        } else {
          Left(
            MaxByteToDecompressExceeded(
              s"Max bytes to decompress is exceeded. The limit is ${maxBytesLimit.limit.value} bytes."
            )
          )
        }
      }
      .leftMap(errorMapping)
      .flatten

  /** Copies `n` bytes from in to out. Returns false if the input contained more than `n` bytes.
    *
    * Up to (n + 1) bytes may in fact be copied.
    */
  @tailrec
  def copyNBuffered(n: Int, buffer: Array[Byte], in: InputStream, out: OutputStream): Boolean =
    if (n < 0) false
    else {
      val readCount = (n + 1).max(1) // +1 to detect input exhaustion, max(1) to avoid int overflow
      in.read(buffer, 0, buffer.length.min(readCount)) match {
        case -1 => true
        case count =>
          out.write(buffer, 0, count)
          copyNBuffered(n - count, buffer, in, out)
      }
    }

  /** Based on the final size we either truncate the bytes to fit in that size or pad with 0s
    */
  def padOrTruncate(bytes: ByteString, finalSize: NonNegativeInt): ByteString =
    if (finalSize == NonNegativeInt.zero)
      ByteString.EMPTY
    else {
      val padSize = finalSize.value - bytes.size()
      if (padSize > 0)
        bytes.concat(ByteString.copyFrom(new Array[Byte](padSize)))
      else if (padSize == 0) bytes
      else bytes.substring(0, bytes.size() + padSize)
    }

  private def errorMapping(err: Throwable): DeserializationError =
    err match {
      // all exceptions that were observed when testing these methods (see also `GzipCompressionTests`)
      case ex: ZipException => DefaultDeserializationError(ex.getMessage)
      case _: EOFException =>
        DefaultDeserializationError("Compressed byte input ended too early")
      case error => DefaultDeserializationError(error.getMessage)
    }
}
