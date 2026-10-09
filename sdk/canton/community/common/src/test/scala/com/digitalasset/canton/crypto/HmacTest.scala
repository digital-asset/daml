// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.security.SecureRandom

trait HmacTest extends AsyncWordSpec with BaseTest with CryptoTestHelper with FailOnShutdown {

  def hmacProvider(
      supportedHmacAlgorithms: Set[HmacAlgorithm],
      newCrypto: => FutureUnlessShutdown[HmacOps],
  ): Unit =
    forAll(supportedHmacAlgorithms) { algorithm =>
      s"HMAC ${algorithm.name}" should {

        "compute HMAC from a message with an explicit secret" in {
          for {
            crypto <- newCrypto
            // fixed 16-byte keys
            secret = ByteString.copyFromUtf8("0123456789ABCDEF")
            secretOther = ByteString.copyFromUtf8("1123456789ABCDEF")
            message = "test message"
            hmac_1 =
              crypto
                .computeHmacWithSecret(secret, ByteString.copyFromUtf8(message), algorithm)
                .valueOrFail("failed to compute hmac")
            hmac_2 =
              crypto
                .computeHmacWithSecret(secret, ByteString.copyFromUtf8(message), algorithm)
                .valueOrFail("failed to compute hmac")
            hmac_Other =
              crypto
                .computeHmacWithSecret(secretOther, ByteString.copyFromUtf8(message), algorithm)
                .valueOrFail("failed to compute hmac")
          } yield {
            hmac_1.unwrap.size() shouldBe algorithm.hashAlgorithm.length.toInt
            hmac_1 shouldBe hmac_2
            hmac_1 should not be hmac_Other
          }
        }

        "fail HMAC computation if secret key length is invalid" in {
          for {
            crypto <- newCrypto
            random = new SecureRandom()

            smallSecretBytes = new Array[Byte](crypto.minimumSecretKeyLengthInBytes - 1)
            bigSecretBytes = new Array[Byte](algorithm.hashAlgorithm.internalBlockSizeInBytes + 1)
            _ = random.nextBytes(smallSecretBytes)
            _ = random.nextBytes(bigSecretBytes)
            message = "test message"
            hmacSmall =
              crypto.computeHmacWithSecret(
                ByteString.copyFrom(smallSecretBytes),
                ByteString.copyFromUtf8(message),
                algorithm,
              )
            hmacBig =
              crypto.computeHmacWithSecret(
                ByteString.copyFrom(bigSecretBytes),
                ByteString.copyFromUtf8(message),
                algorithm,
              )
          } yield {
            hmacSmall.left.value shouldBe a[HmacError.InvalidHmacKeyLength]
            hmacBig.left.value shouldBe a[HmacError.InvalidHmacKeyLength]
          }

        }

      }
    }
}
