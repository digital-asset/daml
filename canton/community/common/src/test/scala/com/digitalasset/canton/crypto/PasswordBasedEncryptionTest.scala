// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.CryptoTestHelper.TestMessage
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait PasswordBasedEncryptionTest {
  this: AsyncWordSpec with BaseTest =>

  def pbeProvider(
      supportedPbkdfSchemes: Set[PbkdfScheme],
      supportedSymmetricKeySchemes: Set[SymmetricKeyScheme],
      newCrypto: => Future[PasswordBasedEncryptionOps with EncryptionOps],
  ): Unit = {

    s"encrypt with passwords" should {

      forAll(supportedPbkdfSchemes) { pbkdfScheme =>
        forAll(supportedSymmetricKeySchemes) { symmetricKeyScheme =>
          s"generate symmetric key for $symmetricKeyScheme from password using $pbkdfScheme" in {
            newCrypto.map { crypto =>
              val pbkey = crypto
                .deriveSymmetricKey("hello world", symmetricKeyScheme, pbkdfScheme, saltO = None)
                .valueOrFail("Failed to derive key from password")

              pbkey.salt.unwrap.size() shouldEqual pbkdfScheme.defaultSaltLengthInBytes
              pbkey.key.key.size() shouldEqual symmetricKeyScheme.keySizeInBytes
            }
          }

          s"generate the same symmetric key in $symmetricKeyScheme for the same password when given the same salt using $pbkdfScheme" in {
            newCrypto.map { crypto =>
              val pbkey1 = crypto
                .deriveSymmetricKey("hello world", symmetricKeyScheme, pbkdfScheme, saltO = None)
                .valueOrFail("Failed to derive key from password")

              val pbkey2 = crypto
                .deriveSymmetricKey(
                  "hello world",
                  symmetricKeyScheme,
                  pbkdfScheme,
                  saltO = Some(pbkey1.salt),
                )
                .valueOrFail("Failed to derive key from password")

              pbkey1.salt.unwrap shouldEqual pbkey2.salt.unwrap
              pbkey1.key shouldEqual pbkey2.key
            }
          }

          s"encrypt and decrypt using a password with $symmetricKeyScheme and $pbkdfScheme" in {
            newCrypto.map { crypto =>
              val message = TestMessage(ByteString.copyFromUtf8("foobar"))
              val password = "hello world"
              val encrypted = crypto
                .encryptWithPassword(
                  message,
                  password,
                  testedProtocolVersion,
                  symmetricKeyScheme,
                  pbkdfScheme,
                )
                .valueOrFail("Failed to encrypt with password")

              val decrypted = crypto
                .decryptWithPassword(encrypted, password)(TestMessage.fromByteString)
                .valueOrFail("Failed to decrypt")

              decrypted shouldEqual message
            }
          }

          s"encrypt with one password and fail to decrypt with another password using $symmetricKeyScheme and $pbkdfScheme" in {
            newCrypto.map { crypto =>
              val message = TestMessage(ByteString.copyFromUtf8("foobar"))
              val encrypted = crypto
                .encryptWithPassword(
                  message,
                  "hello world",
                  testedProtocolVersion,
                  symmetricKeyScheme,
                  pbkdfScheme,
                )
                .valueOrFail("Failed to encrypt with password")

              val decryptedE = crypto
                .decryptWithPassword(encrypted, "hallo welt")(TestMessage.fromByteString)

              decryptedE.left.value shouldBe a[PasswordBasedEncryptionError.DecryptError]
            }
          }
        }
      }
    }
  }
}
