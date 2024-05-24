// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait PublicKeyValidationTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>

  private def modifyPublicKey(
      publicKey: PublicKey,
      newFormat: CryptoKeyFormat,
  ): PublicKey = {
    publicKey match {
      case EncryptionPublicKey(_format, key, scheme) =>
        new EncryptionPublicKey(newFormat, key, scheme)
      case SigningPublicKey(_format, key, scheme) =>
        new SigningPublicKey(newFormat, key, scheme)
      case _ => fail(s"unsupported key type")
    }
  }

  private def keyValidationTest[K <: PublicKey](
      supportedCryptoKeyFormats: Set[CryptoKeyFormat],
      name: String,
      newCrypto: => Future[Crypto],
      newPublicKey: Crypto => Future[PublicKey],
  ): Unit = {

    // change format
    forAll(supportedCryptoKeyFormats) { format =>
      s"Validate $name public key with $format" in {
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
            validationRes shouldEqual Right(())
          else
            validationRes.left.value should include(
              s"Failed to deserialize $format public key: KeyParseAndValidateError"
            )
      }
    }

  }

  /** Test public key validation
    */
  def publicKeyValidationProvider(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      supportedCryptoKeyFormats: Set[CryptoKeyFormat],
      newCrypto: => Future[Crypto],
  ): Unit = {

    "Validate public keys" should {
      forAll(supportedSigningKeySchemes) { signingKeyScheme =>
        keyValidationTest[SigningPublicKey](
          supportedCryptoKeyFormats,
          signingKeyScheme.toString,
          newCrypto,
          crypto => getSigningPublicKey(crypto, signingKeyScheme).failOnShutdown,
        )
      }

      forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
        keyValidationTest[EncryptionPublicKey](
          supportedCryptoKeyFormats,
          encryptionKeyScheme.toString,
          newCrypto,
          crypto => getEncryptionPublicKey(crypto, encryptionKeyScheme).failOnShutdown,
        )
      }
    }

  }

}
