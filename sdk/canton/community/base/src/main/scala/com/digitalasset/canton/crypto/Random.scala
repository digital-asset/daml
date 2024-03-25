// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  HasCryptographicEvidence,
}
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.google.protobuf.ByteString

import scala.util.Random

trait RandomOps {

  protected def generateRandomBytes(length: Int): Array[Byte]

  def generateRandomByteString(length: Int): ByteString =
    ByteString.copyFrom(generateRandomBytes(length))

  def generateSecureRandomness(length: Int): SecureRandomness = SecureRandomness(
    generateRandomByteString(length)
  )
}

/** The class is a tag that denotes a byte string as a securely generated random value.
  *
  * Not an AnyVal as we also want it to be a serializable value such that we can encrypt it.
  */
final case class SecureRandomness private[crypto] (unwrap: ByteString)
    extends HasCryptographicEvidence
    with HasVersionedToByteString {
  override def toByteString(version: ProtocolVersion): ByteString = getCryptographicEvidence

  override def getCryptographicEvidence: ByteString = unwrap
}

/** Cryptographically-secure randomness */
object SecureRandomness {

  /** Recover secure randomness from a byte string. Use for deserialization only. Fails if the provided byte string
    * is not of the expected length.
    */
  def fromByteString(
      expectedLength: Int
  )(bytes: ByteString): Either[DeserializationError, SecureRandomness] = {
    if (bytes.size != expectedLength)
      Left(
        DefaultDeserializationError(
          s"Expected $expectedLength bytes of serialized randomness, got ${bytes.size}"
        )
      )
    else Right(SecureRandomness(bytes))
  }
}

/** Pseudo randomness, MUST NOT be used for security-relevant operations. */
object PseudoRandom {

  private val rand = new Random(new java.security.SecureRandom())

  def randomAlphaNumericString(length: Int): String = rand.alphanumeric.take(length).mkString

  def randomUnsigned(maxValue: Int): Int = rand.between(0, maxValue)

}
