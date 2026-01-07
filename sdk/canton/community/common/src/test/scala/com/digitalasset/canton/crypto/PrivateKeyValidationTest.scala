// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.CryptoKeyFormat.DerPkcs8Pki
import com.digitalasset.canton.crypto.provider.jce.JceSecurityProvider
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import java.security.KeyPairGenerator
import java.security.spec.RSAKeyGenParameterSpec
import scala.concurrent.Future

trait PrivateKeyValidationTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>

  private def modifyPrivateKeyFormat(
      privateKey: PrivateKey,
      newFormat: CryptoKeyFormat,
  ): PrivateKey =
    privateKey match {
      case epk: EncryptionPrivateKey =>
        epk.replaceFormat(newFormat)
      case spk: SigningPrivateKey =>
        spk.replaceFormat(newFormat)
      case _ => fail(s"unsupported key type")
    }

  private def privateKeyValidationTest(
      supportedCryptoKeyFormats: Set[CryptoKeyFormat],
      name: String,
      newCrypto: => Future[Crypto],
      newPrivateKey: Crypto => Future[PrivateKey],
  ): Unit =
    // Modify the private key to have a different encoding format than expected,
    // to verify that parseAndValidatePrivateKey correctly rejects mismatched formats.
    forAll(supportedCryptoKeyFormats) { format =>
      s"Validate $name private key with format \"$format\"" in {
        for {
          crypto <- newCrypto
          privateKey <- newPrivateKey(crypto)
          newPrivateKeyWithTargetFormat = modifyPrivateKeyFormat(privateKey, format)
          validationRes = CryptoKeyValidation.parseAndValidatePrivateKey(
            newPrivateKeyWithTargetFormat,
            errString => errString,
            cacheValidation = false,
          )
        } yield
          if (format == privateKey.format || format == CryptoKeyFormat.Symbolic)
            validationRes shouldEqual Either.unit
          else
            validationRes.left.value should include(
              s"Failed to deserialize or validate $format private key: KeyParseAndValidateError"
            )
      }
    }

  def privateKeyValidationProvider(
      supportedSigningKeySpecs: Set[SigningKeySpec],
      supportedEncryptionKeySpecs: Set[EncryptionKeySpec],
      supportedCryptoKeyFormats: Set[CryptoKeyFormat],
      newCrypto: => Future[Crypto],
  ): Unit =
    "Validate private keys" should {
      forAll(supportedSigningKeySpecs) { signingKeySpec =>
        privateKeyValidationTest(
          supportedCryptoKeyFormats,
          if (signingKeySpec.toString == "EC-P256") "EC-P256-Signing" else signingKeySpec.toString,
          newCrypto,
          crypto =>
            getSigningPrivateKey(
              crypto,
              SigningKeyUsage.ProtocolOnly,
              signingKeySpec,
            ).failOnShutdown,
        )
      }

      forAll(supportedEncryptionKeySpecs) { encryptionKeySpec =>
        privateKeyValidationTest(
          supportedCryptoKeyFormats,
          if (encryptionKeySpec.toString == "EC-P256") "EC-P256-Encryption"
          else encryptionKeySpec.toString,
          newCrypto,
          crypto => getEncryptionPrivateKey(crypto, encryptionKeySpec).failOnShutdown,
        )
      }

      "fail if EC private key parameters do not match expected curve" in {
        for {
          crypto <- newCrypto
          privateKeyEcP256 <- getSigningPrivateKey(
            crypto,
            SigningKeyUsage.ProtocolOnly,
            SigningKeySpec.EcP256,
          ).failOnShutdown
          // use a different curve (P-384) compared to the one on which the private key was generated (P-256)
          validationRes =
            SigningPrivateKey.create(
              Fingerprint.tryFromString("test_ec384"),
              privateKeyEcP256.format,
              privateKeyEcP256.key,
              SigningKeySpec.EcP384,
              privateKeyEcP256.usage,
            )
        } yield validationRes.left.value.toString should include(
          s"EC private key parameters do not match expected curve"
        )
      }

      "fail if RSA private key has the wrong modulus" in {
        val kpGen = KeyPairGenerator.getInstance("RSA", JceSecurityProvider.bouncyCastleProvider)
        kpGen.initialize(new RSAKeyGenParameterSpec(4096, RSAKeyGenParameterSpec.F4))
        val invalidPrivateKey = ByteString.copyFrom(kpGen.generateKeyPair().getPrivate.getEncoded)
        // use a different modulus length
        val validationRes =
          EncryptionPrivateKey.create(
            Fingerprint.tryFromString("test_rsa"),
            DerPkcs8Pki,
            invalidPrivateKey,
            EncryptionKeySpec.Rsa2048,
          )
        validationRes.left.value.toString should include(
          s"RSA key modulus size ${4096} does not match expected " +
            s"size ${EncryptionKeySpec.Rsa2048.keySizeInBits}"
        )
      }
    }

}
