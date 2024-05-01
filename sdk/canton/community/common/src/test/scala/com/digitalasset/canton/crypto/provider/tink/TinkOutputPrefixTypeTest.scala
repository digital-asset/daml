// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.tink

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.CryptoTestHelper.TestMessage
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.google.crypto.tink.proto.OutputPrefixType
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

/** Tests that Canton can handle ciphertexts/signatures with a TINK output prefix
  * by correctly converting the associated keys to the correct format.
  */
trait TinkOutputPrefixTypeTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>

  def generateEncryptionKeyWithTinkOutputPrefix(
      crypto: Crypto,
      scheme: EncryptionKeyScheme,
  ): Future[EncryptionKeyPair] =
    for {
      publicKey <- crypto.privateCrypto
        .asInstanceOf[TinkPrivateCrypto]
        .generateEncryptionKeypairInternal(scheme, OutputPrefixType.TINK)
        .valueOrFail("generate encryption key")
      _ <- crypto.cryptoPublicStore
        .storeEncryptionKey(publicKey.publicKey)
        .leftMap[EncryptionKeyGenerationError](
          EncryptionKeyGenerationError.EncryptionPublicStoreError
        )
        .valueOrFail("store public encryption key")
      _ <- crypto.cryptoPrivateStore
        .asInstanceOf[CryptoPrivateStoreExtended]
        .storeDecryptionKey(publicKey.privateKey, None)
        .leftMap[EncryptionKeyGenerationError](
          EncryptionKeyGenerationError.EncryptionPrivateStoreError
        )
        .valueOrFail("store private encryption key")
    } yield publicKey

  def generateSymmetricKeyWithTinkOutputPrefix(
      crypto: Crypto,
      scheme: SymmetricKeyScheme,
  ): SymmetricKey =
    crypto.pureCrypto
      .asInstanceOf[TinkPureCrypto]
      .generateSymmetricKeyInternal(scheme, OutputPrefixType.TINK)
      .valueOrFail("generate symmetric key")

  def generateSigningKeyWithTinkOutputPrefix(
      crypto: Crypto,
      scheme: SigningKeyScheme,
  ): Future[SigningKeyPair] =
    for {
      publicKey <- crypto.privateCrypto
        .asInstanceOf[TinkPrivateCrypto]
        .generateSigningKeypairInternal(scheme, OutputPrefixType.TINK)
        .valueOrFail("generate encryption key")
      _ <- crypto.cryptoPublicStore
        .storeSigningKey(publicKey.publicKey)
        .leftMap[SigningKeyGenerationError](
          SigningKeyGenerationError.SigningPublicStoreError
        )
        .valueOrFail("store public encryption key")
      _ <- crypto.cryptoPrivateStore
        .asInstanceOf[CryptoPrivateStoreExtended]
        .storeSigningKey(publicKey.privateKey, None)
        .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.SigningPrivateStoreError)
        .valueOrFail("store private encryption key")
    } yield publicKey

  // generate a random prefix starting with 0x01 to simulate a TINK output prefix
  private lazy val randomTinkPrefix: Array[Byte] = {
    val tinkPrefix = new Array[Byte](TinkKeyFormat.tinkOuputPrefixSizeInBytes - 1)
    scala.util.Random.nextBytes(tinkPrefix)
    Array[Byte](0x01) ++ tinkPrefix
  }

  private def canDecryptCiphertextWithTinkOutputPrefix(
      encryptionKeyScheme: EncryptionKeyScheme,
      newCrypto: => Future[Crypto],
  ): Unit =
    s"Decrypt a ${encryptionKeyScheme.name} ciphertext with TINK output prefix" in {
      for {
        crypto <- newCrypto
        publicKey <- getEncryptionPublicKey(crypto, encryptionKeyScheme)
        message = CryptoTestHelper.TestMessage(ByteString.copyFromUtf8("test"))
        ciphertext = crypto.pureCrypto
          .encryptWith(message, publicKey, testedProtocolVersion)
          .valueOrFail("encrypt with converted key")
        ciphertextWithPrefix = AsymmetricEncrypted(
          ByteString.copyFrom(randomTinkPrefix ++ ciphertext.ciphertext.toByteArray),
          publicKey.fingerprint,
        )
        message2 <- crypto.privateCrypto
          .decrypt(ciphertextWithPrefix)(CryptoTestHelper.TestMessage.fromByteString)
          .valueOrFail("decrypt")
      } yield message shouldEqual message2
    }

  private def canSymmetricDecryptCiphertextWithTinkOutputPrefix(
      symmetricKeyScheme: SymmetricKeyScheme,
      newCrypto: => Future[Crypto],
  ): Unit =
    s"Symmetric decrypt a $symmetricKeyScheme ciphertext with TINK output prefix" in {
      for {
        crypto <- newCrypto
        symmetricKey = crypto.pureCrypto
          .generateSymmetricKey(scheme = symmetricKeyScheme)
          .valueOrFail("generate symmetric key")
        message = CryptoTestHelper.TestMessage(ByteString.copyFromUtf8("test"))
        ciphertext = crypto.pureCrypto
          .encryptWith(message, symmetricKey, testedProtocolVersion)
          .valueOrFail("encrypt message")
        ciphertextWithPrefix = new Encrypted[TestMessage](
          ByteString.copyFrom(randomTinkPrefix ++ ciphertext.ciphertext.toByteArray)
        )
        message2 = crypto.pureCrypto
          .decryptWith(
            ciphertextWithPrefix,
            symmetricKey,
          )(TestMessage.fromByteString)
          .valueOrFail("decrypt")
      } yield message shouldEqual message2
    }

  private def canVerifySignatureWithTinkOutputPrefix(
      signingKeyScheme: SigningKeyScheme,
      newCrypto: => Future[Crypto],
  ): Unit =
    s"Verify a ${signingKeyScheme.name} signature with TINK output prefix" in {
      for {
        crypto <- newCrypto
        publicKey <- getSigningPublicKey(crypto, signingKeyScheme)
        hash = TestHash.digest("foobar")
        sig <- crypto.privateCrypto
          .sign(hash, publicKey.fingerprint)
          .valueOrFail("signing")
        signature = new Signature(
          sig.format,
          ByteString.copyFrom(randomTinkPrefix ++ sig.unwrap.toByteArray),
          sig.signedBy,
        )
        res = crypto.pureCrypto.verifySignature(hash, publicKey, signature)
      } yield res shouldEqual Right(())
    }

  def handleTinkOutputPrefixType(
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      supportedSymmetricKeySchemes: Set[SymmetricKeyScheme],
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      newCrypto: => Future[Crypto],
  ): Unit = {

    "handle the TINK output prefix" should {
      forAll(supportedSigningKeySchemes) { signingKeyScheme =>
        canVerifySignatureWithTinkOutputPrefix(signingKeyScheme, newCrypto)
      }

      forAll(supportedSymmetricKeySchemes) { symmetricKeyScheme =>
        canSymmetricDecryptCiphertextWithTinkOutputPrefix(symmetricKeyScheme, newCrypto)
      }

      forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
        canDecryptCiphertextWithTinkOutputPrefix(encryptionKeyScheme, newCrypto)
      }

    }
  }
}
