// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait PrivateKeySerializationTest extends BaseTest { this: AsyncWordSpec =>

  def privateKeySerializerProvider(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      newCrypto: => Future[Crypto],
  ): Unit = {

    s"Serialize and deserialize a private key via protobuf" should {

      forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
        s"for a $encryptionKeyScheme encryption private key" in {
          for {
            crypto <- newCrypto
            cryptoPrivateStore = crypto.cryptoPrivateStore.toExtended
              .valueOrFail("crypto private store does not implement all necessary methods")
            publicKey <- crypto.privateCrypto
              .generateEncryptionKey(encryptionKeyScheme)
              .valueOrFail("generate enc key")
              .failOnShutdown
            privateKey <- cryptoPrivateStore
              .decryptionKey(publicKey.id)
              .leftMap(_.toString)
              .subflatMap(_.toRight("Private key not found"))
              .valueOrFail("get key")
              .failOnShutdown
            keyP = privateKey.toProtoVersioned(testedProtocolVersion)
            key2 = EncryptionPrivateKey.fromProtoVersioned(keyP).valueOrFail("serialize key")
          } yield privateKey shouldEqual key2
        }
      }

      forAll(supportedSigningKeySchemes) { signingKeyScheme =>
        s"for a $signingKeyScheme signing private key" in {
          for {
            crypto <- newCrypto
            cryptoPrivateStore = crypto.cryptoPrivateStore.toExtended
              .valueOrFail("crypto private store does not implement all necessary methods")
            publicKey <- crypto.privateCrypto
              .generateSigningKey(signingKeyScheme)
              .valueOrFail("generate signing key")
              .failOnShutdown
            privateKey <- cryptoPrivateStore
              .signingKey(publicKey.id)
              .leftMap(_.toString)
              .subflatMap(_.toRight("Private key not found"))
              .valueOrFail("get key")
              .failOnShutdown
            privateKeyP = privateKey.toProtoVersioned(testedProtocolVersion)
            privateKey2 = SigningPrivateKey
              .fromProtoVersioned(privateKeyP)
              .valueOrFail("serialize key")
          } yield privateKey shouldEqual privateKey2
        }
      }
    }
  }
}
