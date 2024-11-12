// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait PublicKeyValidationTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>

  private def modifyPublicKey(
      publicKey: PublicKey,
      newFormat: CryptoKeyFormat,
  ): PublicKey =
    publicKey match {
      case EncryptionPublicKey(_format, key, scheme) =>
        new EncryptionPublicKey(newFormat, key, scheme)()
      case SigningPublicKey(_format, key, scheme, usage, dataForFingerprint) =>
        new SigningPublicKey(newFormat, key, scheme, usage, dataForFingerprint)()
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
          newPublicKeyWithTargetFormat = modifyPublicKey(publicKey, format)
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

  /** Test public key validation
    */
  def publicKeyValidationProvider(
      supportedSigningKeySpecs: Set[SigningKeySpec],
      supportedEncryptionKeySpecs: Set[EncryptionKeySpec],
      supportedCryptoKeyFormats: Set[CryptoKeyFormat],
      newCrypto: => Future[Crypto],
  ): Unit =
    "Validate public keys" should {
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
    }

}
