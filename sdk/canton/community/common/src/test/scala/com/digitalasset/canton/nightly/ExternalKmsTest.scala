// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.nightly

import cats.data.EitherT
import com.digitalasset.canton.crypto.kms.KmsError.KmsEncryptError
import com.digitalasset.canton.crypto.kms.{KmsError, KmsKeyId, KmsTest}
import com.digitalasset.canton.crypto.provider.kms.HasPredefinedKmsKeys
import com.digitalasset.canton.crypto.{
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  SignatureFormat,
  SigningAlgorithmSpec,
  SigningKeySpec,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{ByteString4096, ResourceUtil}
import org.scalatest.Assertion
import org.scalatest.wordspec.FixtureAsyncWordSpec

/** Implements all tests that are shared among 'external' KMSs that require an access point and an
  * SDK client (i.e. AWS and GCP vs Symbolic).
  */
trait ExternalKmsTest extends KmsTest {
  self: FixtureAsyncWordSpec & HasPredefinedKmsKeys =>

  def kmsSymmetricEncryptionKeyId: FutureUnlessShutdown[KmsKeyId] =
    FutureUnlessShutdown.pure(predefinedSymmetricEncryptionKey)

  def kmsAsymmetricEncryptionKey: FutureUnlessShutdown[KmsAsymmetricEncryptionKey] =
    FutureUnlessShutdown.pure(
      KmsAsymmetricEncryptionKey.create(
        predefinedAsymmetricEncryptionKeys
          .get(EncryptionKeySpec.Rsa2048)
          .valueOrFail("could not find predefined Rsa2048 encryption key")
          ._1,
        EncryptionKeySpec.Rsa2048,
        EncryptionAlgorithmSpec.RsaOaepSha256,
      )
    )

  def kmsSigningKey: FutureUnlessShutdown[KmsSigningKey] =
    FutureUnlessShutdown.pure(
      KmsSigningKey(
        predefinedSigningKeys
          .get(SigningKeySpec.EcP256)
          .valueOrFail("could not find predefined P256 signing key")
          ._1,
        SigningKeySpec.EcP256,
        SigningAlgorithmSpec.EcDsaSha256,
        SignatureFormat.Der,
      )
    )

  // signing key used to verify a signature generated from another key
  def kmsAnotherSigningKey: FutureUnlessShutdown[KmsSigningKey] =
    FutureUnlessShutdown.pure(
      KmsSigningKey(
        predefinedSigningKeys
          .get(SigningKeySpec.EcP256)
          .valueOrFail("could not find predefined P256 signing key")
          ._2,
        SigningKeySpec.EcP256,
        SigningAlgorithmSpec.EcDsaSha256,
        SignatureFormat.Der,
      )
    )

  protected def keyActivenessCheckAndDelete(
      kms: KmsType,
      keyId: KmsKeyId,
  ): FutureUnlessShutdown[Either[KmsError, Unit]] = {

    def checkKeyExistsAndRunEncryptionTest(): FutureUnlessShutdown[Unit] =
      for {
        _ <- eventually()(
          kms.keyExistsAndIsActive(keyId).valueOrFail("check key exists")
        )
        _ <- EitherT
          .liftF[FutureUnlessShutdown, KmsError, Assertion](
            encryptDecryptSymmetricTest(kms, keyId)
          )
          .valueOrFail("encrypt and decrypt")
      } yield ()

    for {
      _ <- checkKeyExistsAndRunEncryptionTest()
        .thereafterF(_ => kms.deleteKey(keyId).valueOrFailShutdown("delete key"))
      // key should no longer be available
      keyExists <- kms.keyExistsAndIsActive(keyId).value
    } yield keyExists
  }

  def externalKms(wrongKmsConfig: KmsType#Config): Unit =
    "fail encryption if wrong region selected" in { _ =>
      ResourceUtil.withResourceM(newKms(wrongKmsConfig)) { kms =>
        for {
          encryptedDataFailed <- kms
            .encryptSymmetric(
              predefinedSymmetricEncryptionKey,
              ByteString4096.tryCreate(dataToHandle),
            )
            .value
            .failOnShutdown
        } yield encryptedDataFailed.left.value shouldBe a[KmsEncryptError]
      }
    }

}
