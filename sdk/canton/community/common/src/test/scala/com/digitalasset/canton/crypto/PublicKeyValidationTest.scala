// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.crypto.provider.jce.JcePureCrypto
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait PublicKeyValidationTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>

  private def modifyPublicKeyFormat(
      publicKey: PublicKey,
      newFormat: CryptoKeyFormat,
  ): PublicKey =
    publicKey match {
      case epk: EncryptionPublicKey =>
        epk.replaceFormat(newFormat)
      case spk: SigningPublicKey =>
        spk.replaceFormat(newFormat)
      case _ => fail(s"unsupported key type")
    }

  private def keyValidationTest[K <: PublicKey](
      supportedCryptoKeyFormats: Set[CryptoKeyFormat],
      name: String,
      newCrypto: => Future[Crypto],
      newPublicKey: Crypto => Future[PublicKey],
  ): Unit =
    // change format
    forAll(supportedCryptoKeyFormats) { format =>
      s"Validate $name public key with format \"$format\"" in {
        for {
          crypto <- newCrypto
          publicKey <- newPublicKey(crypto)
          newPublicKeyWithTargetFormat = modifyPublicKeyFormat(publicKey, format)
          validationRes = CryptoKeyValidation.parseAndValidatePublicKey(
            newPublicKeyWithTargetFormat,
            errString => errString,
          )
        } yield
          if (format == publicKey.format || format == CryptoKeyFormat.Symbolic)
            validationRes shouldEqual Either.unit
          else
            validationRes.left.value should include(
              s"Failed to deserialize $format public key: KeyParseAndValidateError"
            )
      }
    }

  /** Test key validation
    */
  def keyValidationProvider(
      supportedSigningKeySpecs: Set[SigningKeySpec],
      supportedEncryptionKeySpecs: Set[EncryptionKeySpec],
      supportedCryptoKeyFormats: Set[CryptoKeyFormat],
      newCrypto: => Future[Crypto],
      javaPrivateKeyRetentionTime: PositiveFiniteDuration,
  ): Unit =
    "Validate keys" should {
      forAll(supportedSigningKeySpecs) { signingKeySpec =>
        keyValidationTest[SigningPublicKey](
          supportedCryptoKeyFormats,
          if (signingKeySpec.toString == "EC-P256") "EC-P256-Signing" else signingKeySpec.toString,
          newCrypto,
          crypto =>
            getSigningPublicKey(
              crypto,
              SigningKeyUsage.ProtocolOnly,
              signingKeySpec,
            ).failOnShutdown,
        )
      }

      forAll(supportedEncryptionKeySpecs) { encryptionKeySpec =>
        keyValidationTest[EncryptionPublicKey](
          supportedCryptoKeyFormats,
          if (encryptionKeySpec.toString == "EC-P256") "EC-P256-Encryption"
          else encryptionKeySpec.toString,
          newCrypto,
          crypto => getEncryptionPublicKey(crypto, encryptionKeySpec).failOnShutdown,
        )
      }

      "fail if public key not on the curve" in {
        for {
          crypto <- newCrypto
          publicKeyEcP256 <- getSigningPublicKey(
            crypto,
            SigningKeyUsage.ProtocolOnly,
            SigningKeySpec.EcP256,
          ).failOnShutdown
          // use a different curve (P-384) compared to the one on which the public key was generated (P-256)
          validationRes =
            SigningPublicKey.create(
              publicKeyEcP256.format,
              publicKeyEcP256.key,
              SigningKeySpec.EcP384,
              publicKeyEcP256.usage,
            )
        } yield validationRes.left.value.message should include(
          s"EC key not in curve"
        )
      }

      "retain parsed Java keys in cache for the correct amount of time" in {
        for {
          crypto <- newCrypto
          _ = assume(
            crypto.pureCrypto.isInstanceOf[JcePureCrypto],
            "Test only runs with JcePureCrypto",
          )
          jcePureCrypto = crypto.pureCrypto.asInstanceOf[JcePureCrypto]
          privateStore = crypto.cryptoPrivateStore match {
            case extended: CryptoPrivateStoreExtended => extended
            case _ => fail("incorrect crypto private store type")
          }
          publicKey <- getSigningPublicKey(
            crypto,
            SigningKeyUsage.ProtocolOnly,
            supportedSigningKeySpecs.head,
          ).failOnShutdown
          privateKey <- privateStore
            .exportPrivateKey(publicKey.id)
            .valueOrFail("export private key")
            .failOnShutdown
          signingPrivateKey = privateKey
            .valueOrFail("no private key")
            .asInstanceOf[SigningPrivateKey]

          // indirectly converts the private key to a Java key and stores it in the cache
          signature = jcePureCrypto
            .signBytes(
              ByteString.copyFromUtf8("test"),
              signingPrivateKey,
              SigningKeyUsage.ProtocolOnly,
            )
            .valueOrFail("sign message")

          // indirectly converts the public key to a Java key and stores it in the cache
          _ = crypto.pureCrypto
            .verifySignature(
              ByteString.copyFromUtf8("test"),
              publicKey,
              signature,
              SigningKeyUsage.ProtocolOnly,
            )
            .valueOrFail("verify signature")

          inCache = jcePureCrypto.isJavaPublicKeyInCache(publicKey.id) &&
            jcePureCrypto.isJavaPrivateKeyInCache(publicKey.id)

          // ensure enough time has passed for the Java keys to be removed from their respective caches
          _ = Threading.sleep(javaPrivateKeyRetentionTime.underlying.toMillis + 1000)

          inPublicCache = jcePureCrypto.isJavaPublicKeyInCache(publicKey.id)
          inPrivateCache = jcePureCrypto.isJavaPrivateKeyInCache(publicKey.id)

        } yield {
          inCache shouldBe true
          inPublicCache shouldBe false
          inPrivateCache shouldBe false
        }
      }
    }

}
