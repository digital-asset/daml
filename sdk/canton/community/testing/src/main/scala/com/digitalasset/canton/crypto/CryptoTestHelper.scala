// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.version.HasToByteString
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait CryptoTestHelper extends BaseTest with HasExecutionContext {
  this: AsyncWordSpec =>

  /** Gets a new encryption key by generating a new key.
    *
    * @param crypto determines the algorithms used for signing, hashing, and encryption, used
    *               on the client side for serialization.
    * @param encryptionKeySpec the encryption key specification for the new key.
    * @return an encryption public key
    */
  protected def getEncryptionPublicKey(
      crypto: Crypto,
      encryptionKeySpec: EncryptionKeySpec,
  ): FutureUnlessShutdown[EncryptionPublicKey] =
    crypto
      .generateEncryptionKey(encryptionKeySpec)
      .valueOrFail("generate encryption key")

  /** Helper method to get two different encryption public keys.
    */
  protected def getTwoEncryptionPublicKeys(
      crypto: Crypto,
      encryptionKeySpec: EncryptionKeySpec,
  ): FutureUnlessShutdown[(EncryptionPublicKey, EncryptionPublicKey)] =
    for {
      pubKey1 <- getEncryptionPublicKey(crypto, encryptionKeySpec)
      pubKey2 <- getEncryptionPublicKey(crypto, encryptionKeySpec)
    } yield (pubKey1, pubKey2)

  /** Gets a new signing key by generating a new key.
    *
    * @param crypto    determines the algorithms used for signing, hashing, and encryption, used
    *                  on the client side for serialization.
    * @param usage     what the key must be used for
    * @param keySpec   the signing key specification for the new key.
    * @return a signing public key
    */
  protected def getSigningPublicKey(
      crypto: Crypto,
      usage: NonEmpty[Set[SigningKeyUsage]],
      keySpec: SigningKeySpec,
  ): FutureUnlessShutdown[SigningPublicKey] =
    crypto
      .generateSigningKey(keySpec, usage)
      .valueOrFail("generate signing key")

  /** Helper method to get two different signing public keys.
    */
  protected def getTwoSigningPublicKeys(
      crypto: Crypto,
      usage: NonEmpty[Set[SigningKeyUsage]],
      keySpec: SigningKeySpec,
  ): FutureUnlessShutdown[(SigningPublicKey, SigningPublicKey)] =
    for {
      pubKey1 <- getSigningPublicKey(crypto, usage, keySpec)
      pubKey2 <- getSigningPublicKey(crypto, usage, keySpec)
    } yield (pubKey1, pubKey2)

}

object CryptoTestHelper {
  final case class TestMessage(bytes: ByteString) extends HasToByteString {
    override def toByteString: ByteString = bytes
  }

  object TestMessage {
    def fromByteString(bytes: ByteString): Either[DefaultDeserializationError, TestMessage] = Right(
      TestMessage(bytes)
    )
  }
}
