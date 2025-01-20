// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait PublicKeyValidationTest extends BaseTest with CryptoTestHelper { this: AsyncWordSpec =>

  private def modifyPublicKey(
      publicKey: PublicKey,
      newFormat: Option[CryptoKeyFormat],
      newId: Option[Fingerprint],
  ): PublicKey =
    publicKey match {
      case EncryptionPublicKey(id, format, key, scheme) =>
        new EncryptionPublicKey(newId.getOrElse(id), newFormat.getOrElse(format), key, scheme)
      case SigningPublicKey(id, format, key, scheme) =>
        new SigningPublicKey(newId.getOrElse(id), newFormat.getOrElse(format), key, scheme)
      case _ => fail(s"unsupported key type")
    }

  private def keyValidationTest[K <: PublicKey](
      name: String,
      newCrypto: => Future[Crypto],
      newPublicKey: Crypto => Future[PublicKey],
  ): Unit = {

    // change format
    forAll(CryptoKeyFormat.allFormats) { format =>
      s"Validate $name public key with $format" in {
        for {
          crypto <- newCrypto
          publicKey <- newPublicKey(crypto)
          newPublicKeyWithTargetFormat = modifyPublicKey(publicKey, Some(format), None)
          validationRes = CryptoPureApiHelper.parseAndValidatePublicKey(
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

    // with wrong fingerprint
    s"Validate $name public key with wrong fingerprint" in {
      for {
        crypto <- newCrypto
        publicKey <- newPublicKey(crypto)
        newPublicKeyWithWrongFingerprint = modifyPublicKey(
          publicKey,
          None,
          Some(Fingerprint.tryCreate("mock")),
        )
        validationRes = CryptoPureApiHelper.parseAndValidatePublicKey(
          newPublicKeyWithWrongFingerprint,
          errString => errString,
        )
      } yield validationRes.left.value should fullyMatch regex
        raw"Failed to deserialize ${publicKey.format} public key: " +
        raw"KeyParseAndValidateError\(The regenerated fingerprint \S+ does not match the fingerprint of the object: mock\)"
    }

  }

  /** Test public key validation
    */
  def publicKeyValidationProvider(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      supportedEncryptionKeySchemes: Set[EncryptionKeyScheme],
      newCrypto: => Future[Crypto],
  ): Unit = {

    "Validate public keys" should {
      forAll(supportedSigningKeySchemes) { signingKeyScheme =>
        keyValidationTest[SigningPublicKey](
          signingKeyScheme.toString,
          newCrypto,
          crypto => getSigningPublicKey(crypto, signingKeyScheme),
        )
      }

      forAll(supportedEncryptionKeySchemes) { encryptionKeyScheme =>
        keyValidationTest[EncryptionPublicKey](
          encryptionKeyScheme.toString,
          newCrypto,
          crypto => getEncryptionPublicKey(crypto, encryptionKeyScheme),
        )
      }
    }

  }

}
