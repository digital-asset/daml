// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.nowarn

trait CryptoKeyFormatMigrationTest
    extends AsyncWordSpec
    with BaseTest
    with CryptoTestHelper
    with FailOnShutdown {

  def migrationTest(
      supportedSigningKeySpecs: Set[SigningKeySpec],
      supportedEncryptionKeySpecs: Set[EncryptionKeySpec],
      newCrypto: => FutureUnlessShutdown[Crypto],
  ): Unit =
    "cryptographic keys" should {
      forAll(supportedSigningKeySpecs) { keySpec =>
        s"be migrated upon deserialization (signing key $keySpec)" in {
          for {
            crypto <- newCrypto

            publicKey <- getSigningPublicKey(crypto, SigningKeyUsage.All, keySpec)
            privateStore = crypto.cryptoPrivateStore.toExtended.valueOrFail("extend store")
            privateKeyO <- privateStore.signingKey(publicKey.id).valueOrFail("read private key")
            privateKey = privateKeyO.valueOrFail("find private key")
          } yield {
            val (legacyPublicKey, legacyPrivateKey) = makeLegacyKeys(publicKey, privateKey)
            @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
            val expectedLegacyFormat = keySpec match {
              case SigningKeySpec.EcCurve25519 => CryptoKeyFormat.Raw
              case SigningKeySpec.EcP256 | SigningKeySpec.EcP384 => CryptoKeyFormat.Der
            }
            legacyPublicKey.format shouldBe expectedLegacyFormat
            legacyPrivateKey.format shouldBe expectedLegacyFormat

            val newPublicKey = SigningPublicKey
              .fromProtoV30(legacyPublicKey.toProtoV30)
              .valueOrFail("deserialize public")

            newPublicKey.format shouldBe CryptoKeyFormat.DerX509Spki
            newPublicKey.migrated shouldBe true
            newPublicKey shouldBe publicKey

            val newPrivateKey = SigningPrivateKey
              .fromProtoV30(legacyPrivateKey.toProtoV30)
              .valueOrFail("deserialize private")

            newPrivateKey.format shouldBe CryptoKeyFormat.DerPkcs8Pki
            newPrivateKey.migrated shouldBe true
            newPrivateKey shouldBe privateKey
          }
        }.failOnShutdown
      }

      forAll(supportedEncryptionKeySpecs) { keySpec =>
        s"be migrated upon deserialization (encryption key $keySpec)" in {
          for {
            crypto <- newCrypto

            publicKey <- getEncryptionPublicKey(crypto, keySpec)
            privateStore = crypto.cryptoPrivateStore.toExtended.valueOrFail("extend store")
            privateKeyO <- privateStore.decryptionKey(publicKey.id).valueOrFail("read private key")
            privateKey = privateKeyO.valueOrFail("find private key")
          } yield {
            val (legacyPublicKey, legacyPrivateKey) = makeLegacyKeys(publicKey, privateKey)
            @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
            val expectedLegacyFormat = CryptoKeyFormat.Der
            legacyPublicKey.format shouldBe expectedLegacyFormat
            legacyPrivateKey.format shouldBe expectedLegacyFormat

            val newPublicKey = EncryptionPublicKey
              .fromProtoV30(legacyPublicKey.toProtoV30)
              .valueOrFail("deserialize public")

            newPublicKey.format shouldBe CryptoKeyFormat.DerX509Spki
            newPublicKey.migrated shouldBe true
            newPublicKey shouldBe publicKey

            val newPrivateKey = EncryptionPrivateKey
              .fromProtoV30(legacyPrivateKey.toProtoV30)
              .valueOrFail("deserialize private")

            newPrivateKey.format shouldBe CryptoKeyFormat.DerPkcs8Pki
            newPrivateKey.migrated shouldBe true
            newPrivateKey shouldBe privateKey
          }
        }.failOnShutdown
      }

      def makeLegacyKeys[PK <: PublicKey, SK <: PrivateKey](
          publicKey: PK,
          privateKey: SK,
      ): (PK#K, SK#K) = {
        val legacyPublicKey = publicKey.reverseMigrate().valueOrFail("public key reverse migration")
        legacyPublicKey.id shouldBe publicKey.id

        val legacyPrivateKey =
          privateKey.reverseMigrate().valueOrFail("private key reverse migration")

        (legacyPublicKey, legacyPrivateKey)
      }
    }

}
