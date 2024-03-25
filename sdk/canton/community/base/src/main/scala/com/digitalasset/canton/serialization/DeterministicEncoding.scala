// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.serialization

import cats.syntax.either.*
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError}
import com.google.protobuf.ByteString

import java.nio.{ByteBuffer, ByteOrder}
import java.time.Instant
import scala.annotation.tailrec

sealed trait DeserializationError extends PrettyPrinting {
  val message: String

  override def pretty: Pretty[DeserializationError] =
    prettyOfClass(
      param("message", _.message.unquoted)
    )
  def toProtoDeserializationError: ProtoDeserializationError =
    this match {
      case DefaultDeserializationError(message) =>
        ProtoDeserializationError.OtherError(message)
      case MaxByteToDecompressExceeded(message) =>
        ProtoDeserializationError.MaxBytesToDecompressExceeded(message)
    }
}
final case class DefaultDeserializationError(message: String) extends DeserializationError
final case class MaxByteToDecompressExceeded(message: String) extends DeserializationError

/** The methods in this object should be used when a <strong>deterministic</strong> encoding is
  * needed. They are not meant for computing serializations for a wire format. Protobuf is a better choice there.
  */
object DeterministicEncoding {

  /** Tests that the given [[com.google.protobuf.ByteString]] has at least `len` bytes and splits the [[com.google.protobuf.ByteString]] at `len`. */
  def splitAt(
      len: Int,
      bytes: ByteString,
  ): Either[DeserializationError, (ByteString, ByteString)] =
    if (bytes.size < len)
      Left(DefaultDeserializationError(s"Expected $len bytes"))
    else
      Right((bytes.substring(0, len), bytes.substring(len)))

  /** Encode a [[scala.Byte]] into a [[com.google.protobuf.ByteString]]. */
  def encodeByte(b: Byte): ByteString = ByteString.copyFrom(Array[Byte](b))

  /** Encode a ByteString (of given length) into another ByteString */
  def encodeBytes(b: ByteString): ByteString =
    encodeInt(b.size).concat(b)

  /** Extract a byte-string (length stored) from another ByteString */
  def decodeBytes(
      bytes: ByteString
  ): Either[DeserializationError, (ByteString, ByteString)] =
    for {
      lenAndContent <- decodeLength(bytes)
      (len, content) = lenAndContent
      bytesAndRest <- splitAt(len, content)
    } yield bytesAndRest

  /** Encode an [[scala.Int]] into a fixed-length [[com.google.protobuf.ByteString]] in big-endian order. */
  def encodeInt(i: Int): ByteString =
    ByteString.copyFrom(
      ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(i).array()
    )

  /** Encodes the [[scala.Long]] into a unsigned variable integer according to https://github.com/multiformats/unsigned-varint */
  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
  def encodeUVarInt(i: Long): ByteString = {
    require(i >= 0, "Only unsigned integers can be encoded to var-int")

    var bs = ByteString.EMPTY
    var x = i

    // If value is larger than 7 bit
    while (x > 0x7f) {
      // Write 7 bits of input value with continuation MSB set
      val byte: Byte = ((x & 0x7f) | 0x80).toByte
      bs = bs.concat(encodeByte(byte))

      // Shift out the 7 bits that have been written
      x >>>= 7
    }

    // Write the final 7 bits
    bs.concat(encodeByte((x & 0x7f).toByte))
  }

  /** Decodes a unsigned variable integer according to https://github.com/multiformats/unsigned-varint */
  def decodeUVarInt(bytes: ByteString): Either[DeserializationError, (Long, ByteString)] = {

    // Returns a tuple of output varint and index to last consumed byte
    @tailrec
    def decodeUVarIntBytes(output: Long, index: Int, shift: Int): Either[String, (Long, Int)] = {
      if (index >= bytes.size)
        Left("Input bytes already consumed")
      // Only consume maximum of 9 bytes according to spec
      else if (index > 8)
        Left("Varint too long")
      else {
        val nextByte = bytes.byteAt(index)
        val out = output | (nextByte & 0x7f) << shift
        // the continuation MSB is set
        if ((nextByte & 0x80) != 0) {
          decodeUVarIntBytes(out, index + 1, shift + 7)
        } else {
          Right((out, index))
        }
      }
    }

    decodeUVarIntBytes(0, 0, 0).bimap(
      err => DefaultDeserializationError(s"Failed to decode unsigned var-int: $err"),
      { case (output, index) =>
        (output, bytes.substring(index + 1))
      },
    )

  }

  /** Decode a length parameter and do some sanity checks */
  private def decodeLength(
      bytes: ByteString
  ): Either[DeserializationError, (Int, ByteString)] =
    for {
      intAndB <- decodeInt(bytes)
      (len, rest) = intAndB
      _ <- Either.cond(
        len >= 0,
        (),
        DefaultDeserializationError(s"Negative length of $len in encoded data"),
      )
      _ <- Either.cond(
        len <= rest.size,
        (),
        DefaultDeserializationError(s"Length $len is larger than received bytes"),
      )
    } yield intAndB

  /** Consume and decode a fixed-length big-endian [[scala.Int]] and return the remainder of the [[com.google.protobuf.ByteString]].
    *
    * Inverse to [[DeterministicEncoding.encodeInt]]
    */
  def decodeInt(bytes: ByteString): Either[DeserializationError, (Int, ByteString)] =
    for {
      intBytesAndRest <- splitAt(Integer.BYTES, bytes)
      (intBytes, rest) = intBytesAndRest
    } yield (
      ByteBuffer
        .allocate(Integer.BYTES)
        .order(ByteOrder.BIG_ENDIAN)
        .put(intBytes.toByteArray)
        .getInt(0),
      rest,
    )

  /** Encode a [[scala.Long]] into a fixed-length [[com.google.protobuf.ByteString]] in big-endian order. */
  def encodeLong(l: Long): ByteString =
    ByteString.copyFrom(
      ByteBuffer.allocate(java.lang.Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(l).array()
    )

  /** Decode a [[scala.Long]] from a [[com.google.protobuf.ByteString]] and return the remainder of the [[com.google.protobuf.ByteString]].
    *
    * Inverse to [[DeterministicEncoding.encodeLong]]
    */
  def decodeLong(bytes: ByteString): Either[DeserializationError, (Long, ByteString)] =
    for {
      longBytesAndRest <- splitAt(java.lang.Long.BYTES, bytes)
      (longBytes, rest) = longBytesAndRest
    } yield (
      ByteBuffer
        .allocate(java.lang.Long.BYTES)
        .order(ByteOrder.BIG_ENDIAN)
        .put(longBytes.toByteArray)
        .getLong(0),
      rest,
    )

  /** Encode a [[java.lang.String]] into a [[com.google.protobuf.ByteString]], prefixing the string content with its length.
    */
  def encodeString(s: String): ByteString =
    encodeBytes(ByteString.copyFromUtf8(s))

  /** Decode a [[java.lang.String]] from a length-prefixed [[com.google.protobuf.ByteString]] and return the remainder of the [[com.google.protobuf.ByteString]].
    *
    * Inverse to [[DeterministicEncoding.encodeString]]
    */
  def decodeString(bytes: ByteString): Either[DeserializationError, (String, ByteString)] =
    for {
      stringBytesAndBytes <- decodeBytes(bytes)
      (stringBytes, rest) = stringBytesAndBytes
    } yield (stringBytes.toStringUtf8, rest)

  /** Encode an [[java.time.Instant]] into a [[com.google.protobuf.ByteString]] */
  def encodeInstant(instant: Instant): ByteString =
    encodeLong(instant.getEpochSecond).concat(encodeInt(instant.getNano))

  /** Decode a [[java.time.Instant]] from a [[com.google.protobuf.ByteString]] and return the remainder of the [[com.google.protobuf.ByteString]].
    *
    * Inverse to [[DeterministicEncoding.encodeInstant]]
    */
  def decodeInstant(
      bytes: ByteString
  ): Either[DeserializationError, (Instant, ByteString)] = {
    for {
      longAndBytes <- decodeLong(bytes)
      (long, bytes) = longAndBytes
      intAndBytes <- decodeInt(bytes)
      (int, bytes) = intAndBytes
    } yield (Instant.ofEpochSecond(long, int.toLong), bytes)
  }

  /** Encode an [[LfPartyId]] into a [[com.google.protobuf.ByteString]], using the underlying string */
  def encodeParty(party: LfPartyId): ByteString =
    encodeString(party)

  /** Encode an [[scala.Option]] into a tagged [[com.google.protobuf.ByteString]], using the given `encode` function. */
  def encodeOptionWith[A](option: Option[A])(encode: A => ByteString): ByteString = {
    option match {
      case None => encodeByte(0)
      case Some(x) => encodeByte(1).concat(encode(x))
    }
  }

  /** Encode a [[scala.Seq]] into a [[com.google.protobuf.ByteString]] using the given encoding function,
    *  prefixing it with the length of the [[scala.Seq]]
    */
  def encodeSeqWith[A](seq: Seq[A])(encode: A => ByteString): ByteString = {
    import scala.jdk.CollectionConverters.*
    DeterministicEncoding
      .encodeInt(seq.length)
      .concat(ByteString.copyFrom(seq.map(encode).asJava))
  }

  def decodeSeqWith[A](bytes: ByteString)(
      decode: ByteString => Either[DeserializationError, (A, ByteString)]
  ): Either[DeserializationError, (Seq[A], ByteString)] = {
    def iterate(
        col: Seq[A],
        num: Int,
        bytes: ByteString,
    ): Either[DeserializationError, (Seq[A], ByteString)] = {
      if (num == 0) {
        Right((col, bytes))
      } else {
        decode(bytes).flatMap { case (elem, rest) =>
          iterate(col :+ elem, num - 1, rest)
        }
      }
    }
    for {
      lengthAndRest <- DeterministicEncoding.decodeInt(bytes)
      (len, rest) = lengthAndRest
      dc <- iterate(Seq(), len, rest)
    } yield dc
  }

  /** Encode an [[scala.Either]] of [[com.google.protobuf.ByteString]]s into a tagged [[com.google.protobuf.ByteString]]. */
  def encodeEitherWith[L, R](
      either: Either[L, R]
  )(encodeL: L => ByteString, encodeR: R => ByteString): ByteString =
    either match {
      case Left(l) => encodeByte(0).concat(encodeL(l))
      case Right(r) => encodeByte(2).concat(encodeR(r))
    }

  /** Encode a pair of [[com.google.protobuf.ByteString]]s as an untagged [[com.google.protobuf.ByteString]] */
  def encodeTuple2With[A, B](
      pair: (A, B)
  )(encodeA: A => ByteString, encodeB: B => ByteString): ByteString =
    encodeA(pair._1).concat(encodeB(pair._2))
}
