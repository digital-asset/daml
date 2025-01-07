// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.crypto.CryptoTestHelper.TestMessage
import com.digitalasset.canton.crypto.DecryptionError.FailedToDecrypt
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.version.HasToByteString
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait EncryptionTest extends AsyncWordSpec with BaseTest with CryptoTestHelper with FailOnShutdown {

  def encryptionProvider(
      supportedEncryptionAlgorithmSpecs: Set[EncryptionAlgorithmSpec],
      supportedSymmetricKeySchemes: Set[SymmetricKeyScheme],
      newCrypto: => FutureUnlessShutdown[Crypto],
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
            key2 = SymmetricKey.fromTrustedByteString(keyBytes).valueOrFail("serialize key")
          } yield key shouldEqual key2
        }

        "encrypt and decrypt with a symmetric key" in {
          for {
            crypto <- newCrypto
            message = TestMessage(ByteString.copyFromUtf8("foobar"))
            key = newSymmetricKey(crypto)
            encrypted = crypto.pureCrypto
              .encryptSymmetricWith(message, key)
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
              .encryptSymmetricWith(message, key)
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
              .encryptSymmetricWith(message, key)
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
              .encryptSymmetricWith(message, key)
              .valueOrFail("encrypt")
            message2 = crypto.pureCrypto.decryptWith(encrypted, key2)(TestMessage.fromByteString)
          } yield message2.left.value shouldBe a[FailedToDecrypt]
        }

      }
    }

    forAll(supportedEncryptionAlgorithmSpecs) { encryptionAlgorithmSpec =>
      forAll(encryptionAlgorithmSpec.supportedEncryptionKeySpecs.forgetNE) { encryptionKeySpec =>
        s"Random hybrid encrypt with $encryptionAlgorithmSpec and a $encryptionKeySpec key" should {

          behave like hybridEncrypt(
            encryptionKeySpec,
            (message, publicKey) =>
              newCrypto.map(crypto =>
                crypto.pureCrypto
                  .encryptWith(message, publicKey)
              ),
            newCrypto,
          )

          "yield a different ciphertext for the same encryption" in {

            case class TestMessageV2(bytes: ByteString) extends HasToByteString {
              override def toByteString: ByteString = bytes
            }

            val message = TestMessage(ByteString.copyFromUtf8("foobar"))
            val message2 = TestMessageV2(ByteString.copyFromUtf8("foobar"))
            for {
              crypto <- newCrypto
              publicKey <- getEncryptionPublicKey(crypto, encryptionKeySpec)
              encrypted1 = crypto.pureCrypto
                .encryptWith(message, publicKey)
                .valueOrFail("encrypt")
              _ = assert(message.bytes != encrypted1.ciphertext)
              encrypted2 = crypto.pureCrypto
                .encryptWith(message, publicKey)
                .valueOrFail("encrypt")
              // test the other encryption method
              encrypted3 = crypto.pureCrypto
                .encryptWith(message2, publicKey)
                .valueOrFail("encrypt")
              _ = assert(message.bytes != encrypted2.ciphertext)
              _ = assert(message.bytes != encrypted3.ciphertext)
            } yield encrypted1.ciphertext should (not equal encrypted2.ciphertext and not equal encrypted3.ciphertext)
          }

        }
      }
    }
  }

  def hybridEncrypt(
      encryptionKeySpec: EncryptionKeySpec,
      encryptWith: (
          TestMessage,
          EncryptionPublicKey,
      ) => FutureUnlessShutdown[Either[EncryptionError, AsymmetricEncrypted[TestMessage]]],
      newCrypto: => FutureUnlessShutdown[Crypto],
  ): Unit = {

    "serialize and deserialize encryption public key via protobuf" in {
      for {
        crypto <- newCrypto
        key <- getEncryptionPublicKey(crypto, encryptionKeySpec)
        keyP = key.toProtoVersioned(testedProtocolVersion)
        key2 = EncryptionPublicKey.fromProtoVersioned(keyP).valueOrFail("serialize key")
      } yield key shouldEqual key2
    }

    "encrypt and decrypt with an encryption keypair" in {
      val message = TestMessage(ByteString.copyFromUtf8("foobar"))
      for {
        crypto <- newCrypto
        publicKey <- getEncryptionPublicKey(crypto, encryptionKeySpec)

        encryptedE <- encryptWith(message, publicKey)
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
        publicKeys <- getTwoEncryptionPublicKeys(crypto, encryptionKeySpec)
        (publicKey, publicKey2) = publicKeys
        _ = assert(publicKey != publicKey2)
        encryptedE <- encryptWith(message, publicKey)
        encrypted = encryptedE.valueOrFail("encrypt")
        _ = assert(message.bytes != encrypted.ciphertext)
        encrypted2 = AsymmetricEncrypted(
          encrypted.ciphertext,
          encrypted.encryptionAlgorithmSpec,
          publicKey2.id,
        )
        message2 <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
          crypto.privateCrypto
            .decrypt(encrypted2)(TestMessage.fromByteString)
            .value,
          LogEntry.assertLogSeq(
            Seq.empty,
            Seq(
              _.warningMessage should include(
                "KMS operation `asymmetric decrypting with key KmsKeyId(canton-kms-test-another-asymmetric-key)` failed: KmsDecryptError"
              ),
              // Aws logs a failure here
              _.warningMessage should (include("Request") and include("failed")),
            ),
          ),
        )
      } yield message2

      res.map(res => res.left.value shouldBe a[FailedToDecrypt])
    }
  }

}
