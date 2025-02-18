// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

trait PrivateKeySerializationTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  def privateKeySerializerProvider(
      supportedSigningKeySpecs: Set[SigningKeySpec],
      supportedEncryptionKeySpecs: Set[EncryptionKeySpec],
      newCrypto: => FutureUnlessShutdown[Crypto],
  ): Unit =
    s"Serialize and deserialize a private key via protobuf" should {

      forAll(supportedEncryptionKeySpecs) { encryptionKeySpec =>
        s"for a $encryptionKeySpec encryption private key" in {
          for {
            crypto <- newCrypto
            cryptoPrivateStore = crypto.cryptoPrivateStore.toExtended
              .valueOrFail("crypto private store does not implement all necessary methods")
            publicKey <- crypto.privateCrypto
              .generateEncryptionKey(encryptionKeySpec)
              .valueOrFail("generate enc key")
            privateKey <- cryptoPrivateStore
              .decryptionKey(publicKey.id)
              .leftMap(_.toString)
              .subflatMap(_.toRight("Private key not found"))
              .valueOrFail("get key")
            keyP = privateKey.toProtoVersioned(testedProtocolVersion)
            key2 = EncryptionPrivateKey.fromProtoVersioned(keyP).valueOrFail("serialize key")
          } yield privateKey shouldEqual key2
        }.failOnShutdown
      }

      forAll(supportedSigningKeySpecs) { signingKeySpec =>
        s"for a $signingKeySpec signing private key" in {
          for {
            crypto <- newCrypto
            cryptoPrivateStore = crypto.cryptoPrivateStore.toExtended
              .valueOrFail("crypto private store does not implement all necessary methods")
            publicKey <- crypto.privateCrypto
              .generateSigningKey(signingKeySpec, SigningKeyUsage.ProtocolOnly)
              .valueOrFail("generate signing key")
            privateKey <- cryptoPrivateStore
              .signingKey(publicKey.id)
              .leftMap(_.toString)
              .subflatMap(_.toRight("Private key not found"))
              .valueOrFail("get key")
            privateKeyP = privateKey.toProtoVersioned(testedProtocolVersion)
            privateKey2 = SigningPrivateKey
              .fromProtoVersioned(privateKeyP)
              .valueOrFail("serialize key")
          } yield privateKey shouldEqual privateKey2
        }.failOnShutdown
      }
    }
}
