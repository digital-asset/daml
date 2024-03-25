// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import java.io.{ByteArrayOutputStream, EOFException}
import java.util.zip.{GZIPInputStream, GZIPOutputStream, ZipException}
import scala.annotation.tailrec

object ByteStringUtil {

  /** Lexicographic ordering on [[com.google.protobuf.ByteString]]s */
  val orderByteString: Order[ByteString] = new Order[ByteString] {
    override def compare(x: ByteString, y: ByteString): Int = {
      val iterX = x.iterator()
      val iterY = y.iterator()

      @tailrec def go(): Int = {
        if (iterX.hasNext) {
          if (iterY.hasNext) {
            val cmp = iterX.next().compareTo(iterY.next())
            if (cmp == 0) go() else cmp
          } else 1
        } else if (iterY.hasNext) -1
        else 0
      }

      go()
    }
  }

  def compressGzip(bytes: ByteString): ByteString = {
    val rawSize = bytes.size()
    val compressed = new ByteArrayOutputStream(rawSize)
    ResourceUtil.withResource(new GZIPOutputStream(compressed)) { gzipper =>
      bytes.writeTo(gzipper)
    }
    ByteString.copyFrom(compressed.toByteArray)
  }

  /** If maxBytesToRead is not specified, we decompress all the gunzipper input stream.
    * If maxBytesToRead is specified, we decompress maximum maxBytesToRead bytes, and if the input is larger
    * we throw MaxBytesToDecompressExceeded error.
    */
  def decompressGzip(
      bytes: ByteString,
      maxBytesLimit: Option[Int],
  ): Either[DeserializationError, ByteString] = {
    ResourceUtil
      .withResourceEither(new GZIPInputStream(bytes.newInput())) { gunzipper =>
        maxBytesLimit match {
          case None =>
            Right(ByteString.readFrom(gunzipper))
          case Some(max) =>
            val read = gunzipper.readNBytes(max + 1)
            if (read.length > max) {
              Left(
                MaxByteToDecompressExceeded(
                  s"Max bytes to decompress is exceeded. The limit is $max bytes."
                )
              )
            } else {
              Right(ByteString.copyFrom(read))
            }
        }
      }
      .leftMap(errorMapping)
      .flatten
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

  private def errorMapping(err: Throwable): DeserializationError = {
    err match {
      // all exceptions that were observed when testing these methods (see also `GzipCompressionTests`)
      case ex: ZipException => DefaultDeserializationError(ex.getMessage)
      case _: EOFException =>
        DefaultDeserializationError("Compressed byte input ended too early")
      case error => DefaultDeserializationError(error.getMessage)
    }
  }
}
