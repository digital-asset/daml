// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.CryptoTestHelper.TestMessage
import com.digitalasset.canton.crypto.DecryptionError.FailedToDecrypt
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait EncryptionTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>

  def encryptionProvider(
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      supportedSymmetricKeySchemes: Set[SymmetricKeyScheme],
      newCrypto: => Future[Crypto],
  ): Unit = {

    forAll(supportedSymmetricKeySchemes) { symmetricKeyScheme =>
      s"Symmetric encrypt with $symmetricKeyScheme" should {

        def newSymmetricKey(crypto: Crypto): SymmetricKey =
          crypto.pureCrypto
            .generateSymmetricKey(scheme = symmetricKeyScheme)
            .valueOrFail("generate symmetric key")

        def newSecureRandomKey(crypto: Crypto): SymmetricKey = {
          val randomness =
            crypto.pureCrypto.generateSecureRandomness(symmetricKeyScheme.keySizeInBytes)
          crypto.pureCrypto
            .createSymmetricKey(randomness, symmetricKeyScheme)
            .valueOrFail("create key from randomness")
        }

        "serialize and deserialize symmetric encryption key via protobuf" in {
          for {
            crypto <- newCrypto
            key = newSymmetricKey(crypto)
            keyBytes = key.toByteString(testedProtocolVersion)
            key2 = SymmetricKey.fromByteString(keyBytes).valueOrFail("serialize key")
          } yield key shouldEqual key2
        }

        "encrypt and decrypt with a symmetric key" in {
          for {
            crypto <- newCrypto
            message = TestMessage(ByteString.copyFromUtf8("foobar"))
            key = newSymmetricKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, key, testedProtocolVersion)
              .valueOrFail("encrypt")
            message2 = crypto.pureCrypto
              .decryptWith(encrypted, key)(TestMessage.fromByteString)
              .valueOrFail("decrypt")
          } yield {
            message.bytes !== encrypted.ciphertext
            message shouldEqual message2
          }
        }

        "fail decrypt with a different symmetric key" in {
          for {
            crypto <- newCrypto
            message = TestMessage(ByteString.copyFromUtf8("foobar"))
            key = newSymmetricKey(crypto)
            key2 = newSymmetricKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, key, testedProtocolVersion)
              .valueOrFail("encrypt")
            message2 = crypto.pureCrypto.decryptWith(encrypted, key2)(TestMessage.fromByteString)
          } yield message2.left.value shouldBe a[FailedToDecrypt]
        }

        "encrypt and decrypt with secure randomness" in {
          for {
            crypto <- newCrypto
            message = TestMessage(ByteString.copyFromUtf8("foobar"))
            key = newSecureRandomKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, key, testedProtocolVersion)
              .valueOrFail("encrypt")
            message2 = crypto.pureCrypto
              .decryptWith(encrypted, key)(TestMessage.fromByteString)
              .valueOrFail("decrypt")
          } yield {
            message.bytes !== encrypted.ciphertext
            message shouldEqual message2
          }
        }

        "fail decrypt with a different secure randomness" in {
          for {
            crypto <- newCrypto
            message = TestMessage(ByteString.copyFromUtf8("foobar"))
            key = newSecureRandomKey(crypto)
            key2 = newSecureRandomKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptWith(message, key, testedProtocolVersion)
              .valueOrFail("encrypt")
            message2 = crypto.pureCrypto.decryptWith(encrypted, key2)(TestMessage.fromByteString)
          } yield message2.left.value shouldBe a[FailedToDecrypt]
        }

      }
    }

    forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
      s"Random hybrid encrypt with $encryptionKeyScheme" should {

        behave like hybridEncrypt(
          encryptionKeyScheme,
          (message, publicKey, version) =>
            newCrypto.map(crypto => crypto.pureCrypto.encryptWith(message, publicKey, version)),
          newCrypto,
        )

        "yield a different ciphertext for the same encryption" in {
          val message = TestMessage(ByteString.copyFromUtf8("foobar"))
          for {
            crypto <- newCrypto
            publicKey <- getEncryptionPublicKey(crypto, encryptionKeyScheme)
            encrypted1 = crypto.pureCrypto
              .encryptWith(message, publicKey, testedProtocolVersion)
              .valueOrFail("encrypt")
            _ = assert(message.bytes != encrypted1.ciphertext)
            encrypted2 = crypto.pureCrypto
              .encryptWith(message, publicKey, testedProtocolVersion)
              .valueOrFail("encrypt")
            _ = assert(message.bytes != encrypted2.ciphertext)
          } yield encrypted1.ciphertext should not equal encrypted2.ciphertext
        }

      }
    }

  }

  def hybridEncrypt(
      encryptionKeyScheme: EncryptionKeyScheme,
      encryptWith: (
          TestMessage,
          EncryptionPublicKey,
          ProtocolVersion,
      ) => Future[Either[EncryptionError, AsymmetricEncrypted[TestMessage]]],
      newCrypto: => Future[Crypto],
  ): Unit = {

    "serialize and deserialize encryption public key via protobuf" in {
      for {
        crypto <- newCrypto
        key <- getEncryptionPublicKey(crypto, encryptionKeyScheme)
        keyP = key.toProtoVersioned(testedProtocolVersion)
        key2 = EncryptionPublicKey.fromProtoVersioned(keyP).valueOrFail("serialize key")
      } yield key shouldEqual key2
    }

    "encrypt and decrypt with an encryption keypair" in {
      val message = TestMessage(ByteString.copyFromUtf8("foobar"))
      for {
        crypto <- newCrypto
        publicKey <- getEncryptionPublicKey(crypto, encryptionKeyScheme)

        encryptedE <- encryptWith(message, publicKey, testedProtocolVersion)
        encrypted = encryptedE.valueOrFail("encrypt")
        message2 <- crypto.privateCrypto
          .decrypt(encrypted)(TestMessage.fromByteString)
          .valueOrFail("decrypt")
      } yield message shouldEqual message2
    }

    "fail decrypt with a different encryption private key" in {
      val message = TestMessage(ByteString.copyFromUtf8("foobar"))
      val res = for {
        crypto <- newCrypto
        (publicKey, publicKey2) <- getTwoEncryptionPublicKeys(crypto, encryptionKeyScheme)
        _ = assert(publicKey != publicKey2)
        encryptedE <- encryptWith(message, publicKey, testedProtocolVersion)
        encrypted = encryptedE.valueOrFail("encrypt")
        _ = assert(message.bytes != encrypted.ciphertext)
        encrypted2 = AsymmetricEncrypted(
          encrypted.ciphertext,
          publicKey2.id,
        )
        message2 <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
          crypto.privateCrypto
            .decrypt(encrypted2)(TestMessage.fromByteString)
            .value,
          LogEntry.assertLogSeq(
            Seq.empty,
            Seq(
              // Aws logs a failure here
              _.warningMessage should (include("Request") and include("failed"))
            ),
          ),
        )
      } yield message2

      res.map(res => res.left.value shouldBe a[FailedToDecrypt])
    }
  }

}
