// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.ProtoDeserializationError.CryptoDeserializationError
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DefaultDeserializationError, HasCryptographicEvidence}
import com.digitalasset.canton.util.HexString
import com.google.protobuf.ByteString

final case class Nonce private (private val bytes: ByteString) extends HasCryptographicEvidence {
  def toProtoPrimitive: ByteString = bytes
  def toLengthLimitedHexString: String300 =
    String300.tryCreate(HexString.toHexString(this.toProtoPrimitive))

  override def getCryptographicEvidence: ByteString = bytes
}

object Nonce {

  /** [[Nonce]] length is 20 bytes (160 bits), providing enough randomness to make collisions and
    * predictions practically impossible. This ensures nonces stay unique and fresh, preventing
    * replay attacks.
    */
  val length: Int = 20

  def generate(randomOps: RandomOps): Nonce = new Nonce(randomOps.generateRandomByteString(length))

  def fromProtoPrimitive(bytes: ByteString): ParsingResult[Nonce] =
    Either.cond(
      bytes.size() == length,
      new Nonce(bytes),
      CryptoDeserializationError(
        DefaultDeserializationError(s"Nonce of invalid length: ${bytes.size()}")
      ),
    )

}
