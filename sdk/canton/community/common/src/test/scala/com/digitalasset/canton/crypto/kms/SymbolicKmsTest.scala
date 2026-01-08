// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import com.digitalasset.canton.crypto.{
  CryptoPureApi,
  EncryptionAlgorithmSpec,
  EncryptionKeySpec,
  EncryptionPublicKey,
  SignatureFormat,
  SigningAlgorithmSpec,
  SigningKeySpec,
  SigningKeyUsage,
  SigningPublicKey,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.util.ByteString190
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.FixtureAsyncWordSpec

class SymbolicKmsTest extends FixtureAsyncWordSpec with KmsTest {
  override type KmsType = SymbolicKms

  // Use null for the config as there's no KmsConfig subclass for SymbolicKms which is a test only KMS implementation
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override protected val defaultKmsConfig = null

  override protected def newKms(config: KmsType#Config) =
    new SymbolicKms(
      SymbolicCrypto.create(
        testedReleaseProtocolVersion,
        timeouts,
        loggerFactory,
      ),
      config,
      timeouts,
      loggerFactory,
    )

  override lazy val kmsSymmetricEncryptionKeyId: FutureUnlessShutdown[KmsKeyId] =
    for {
      kmsKeyId <- defaultKms
        .generateSymmetricEncryptionKey()
        .valueOrFail("create KMS symmetric encryption key")
    } yield kmsKeyId

  override lazy val kmsAsymmetricEncryptionKey: FutureUnlessShutdown[KmsAsymmetricEncryptionKey] =
    for {
      kmsKeyId <- defaultKms
        .generateAsymmetricEncryptionKeyPair(EncryptionKeySpec.Rsa2048)
        .valueOrFail("create KMS asymmetric encryption key")
    } yield KmsAsymmetricEncryptionKey.create(
      kmsKeyId,
      EncryptionKeySpec.Rsa2048,
      EncryptionAlgorithmSpec.RsaOaepSha256,
    )

  override lazy val kmsSigningKey: FutureUnlessShutdown[KmsSigningKey] =
    for {
      kmsKeyId <- defaultKms
        .generateSigningKeyPair(SigningKeySpec.EcP256)
        .valueOrFail("create KMS signing key")
    } yield KmsSigningKey(
      kmsKeyId,
      SigningKeySpec.EcP256,
      SigningAlgorithmSpec.EcDsaSha256,
      SignatureFormat.Symbolic,
    )

  override lazy val kmsAnotherSigningKey: FutureUnlessShutdown[KmsSigningKey] =
    for {
      kmsKeyId <- defaultKms
        .generateSigningKeyPair(SigningKeySpec.EcP256)
        .valueOrFail("create KMS signing key")
    } yield KmsSigningKey(
      kmsKeyId,
      SigningKeySpec.EcP256,
      SigningAlgorithmSpec.EcDsaSha256,
      SignatureFormat.Symbolic,
    )

  override lazy val pureCrypto: CryptoPureApi =
    new SymbolicPureCrypto

  /* For the asymmetric encryption test the bound is slightly different for the symbolic KMS
   * because the encryption is done as a simple aggregation of (RandomIv, KeyId, Message).
   * Although the bound is 190bytes, we set the byte string ('dataToHandle190') to have 164bytes because
   * it's the maximum size that a symbolic encryption can handle so that it can produce a
   * <256 length ciphertext. This is a requirement because the decryption function is bounded
   * by a ByteString256, which corresponds to the ciphertext length for RSA2048-OAEP-SHA256.
   */
  override lazy val dataToHandle190: ByteString190 =
    ByteString190.tryCreate(ByteString.copyFrom(("t" * 164).getBytes()))

  override def convertToSigningPublicKey(
      kmsSpk: KmsSigningPublicKey,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): SigningPublicKey = kmsSpk
    .convertToSymbolicSigningPublicKey(usage)
    .valueOrFail("convert public signing key")

  override def convertToEncryptionPublicKey(
      kmsEpk: KmsEncryptionPublicKey
  ): EncryptionPublicKey = kmsEpk.convertToSymbolicEncryptionPublicKey
    .valueOrFail("convert public encryption key")

  protected def checkKeyIsDeleted(keyId: KmsKeyId): FutureUnlessShutdown[Assertion] =
    for {
      res <- defaultKms.keyExistsAndIsActive(keyId).value
    } yield res shouldBe false

  "Symbolic KMS" must {
    behave like kms()
  }
}
