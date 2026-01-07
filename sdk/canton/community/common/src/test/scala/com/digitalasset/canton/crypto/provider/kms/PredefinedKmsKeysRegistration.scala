// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.kms.KmsKeyId
import com.digitalasset.canton.crypto.{
  Crypto,
  CryptoTestHelper,
  EncryptionKeyGenerationError,
  EncryptionKeySpec,
  EncryptionPublicKey,
  SigningKeyGenerationError,
  SigningKeySpec,
  SigningKeyUsage,
  SigningPublicKey,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import org.scalatest.wordspec.AsyncWordSpec

/** Defines the necessary methods to register predefined KMS keys in Canton.
  */
trait PredefinedKmsKeysRegistration extends CryptoTestHelper with HasPredefinedKmsKeys {
  this: AsyncWordSpec =>

  /** Gets a new signing key by registering and returning a KMS pre-generated key.
    *
    * @return
    *   a signing public key
    */
  override protected def getSigningPublicKey(
      crypto: Crypto,
      usage: NonEmpty[Set[SigningKeyUsage]],
      keySpec: SigningKeySpec,
  ): FutureUnlessShutdown[SigningPublicKey] = {
    val (kmsKeyId, _) = predefinedSigningKeys
      .get(keySpec)
      .valueOrFail(s"no pre-generated signing KMS keys for $keySpec")
    registerSigningPublicKey(crypto, keySpec, usage, kmsKeyId)
  }

  /** Gets two new signing keys by registering two different KMS pre-generated keys.
    */
  override protected def getTwoSigningPublicKeys(
      crypto: Crypto,
      usage: NonEmpty[Set[SigningKeyUsage]],
      keySpec: SigningKeySpec,
  ): FutureUnlessShutdown[(SigningPublicKey, SigningPublicKey)] = {
    val (kmsKeyId, kmsKeyIdOther) = predefinedSigningKeys
      .get(keySpec)
      .valueOrFail(s"no pre-generated signing KMS keys for $keySpec")
    for {
      pubKey1 <- registerSigningPublicKey(crypto, keySpec, usage, kmsKeyId)
      pubKey2 <- registerSigningPublicKey(crypto, keySpec, usage, kmsKeyIdOther)
    } yield (pubKey1, pubKey2)
  }

  private def registerSigningPublicKey(
      crypto: Crypto,
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
      kmsKeyId: KmsKeyId,
  ): FutureUnlessShutdown[SigningPublicKey] = {

    def checkSigningScheme(
        key: SigningPublicKey
    ): Either[SigningKeyGenerationError, SigningPublicKey] =
      if (key.keySpec != keySpec)
        Left(
          SigningKeyGenerationError.GeneralKmsError(
            s"it is a ${key.keySpec} key but we are expecting a $keySpec signing key"
          )
        )
      else Right(key)

    crypto.privateCrypto match {
      case crypto: KmsPrivateCrypto =>
        crypto
          .registerSigningKey(kmsKeyId, usage)
          .subflatMap(spk => checkSigningScheme(spk))
          .valueOrFail("check signing key")
      case _ => fail("using an incorrect private crypto api")
    }
  }

  /** Gets a new encryption key by registering and returning a KMS pre-generated key.
    *
    * @return
    *   an encryption public key
    */
  override def getEncryptionPublicKey(
      crypto: Crypto,
      encryptionKeySpec: EncryptionKeySpec,
  ): FutureUnlessShutdown[EncryptionPublicKey] = {
    val (kmsKeyId, _) = predefinedAsymmetricEncryptionKeys
      .get(encryptionKeySpec)
      .valueOrFail(s"no pre-generated encryption KMS keys for $encryptionKeySpec")
    registerEncryptionPublicKey(crypto, encryptionKeySpec, kmsKeyId)
  }

  /** Gets two new encryption keys by registering two different KMS pre-generated keys.
    */
  override protected def getTwoEncryptionPublicKeys(
      crypto: Crypto,
      encryptionKeySpec: EncryptionKeySpec,
  ): FutureUnlessShutdown[(EncryptionPublicKey, EncryptionPublicKey)] = {
    val (kmsKeyId, kmsKeyIdOther) = predefinedAsymmetricEncryptionKeys
      .get(encryptionKeySpec)
      .valueOrFail(s"no pre-generated encryption KMS keys for $encryptionKeySpec")
    for {
      pubKey1 <- registerEncryptionPublicKey(crypto, encryptionKeySpec, kmsKeyId)
      pubKey2 <- registerEncryptionPublicKey(crypto, encryptionKeySpec, kmsKeyIdOther)
    } yield (pubKey1, pubKey2)
  }

  private def registerEncryptionPublicKey(
      crypto: Crypto,
      keySpec: EncryptionKeySpec,
      kmsKeyId: KmsKeyId,
  ): FutureUnlessShutdown[EncryptionPublicKey] = {

    def checkEncryptionScheme(
        key: EncryptionPublicKey
    ): Either[EncryptionKeyGenerationError, EncryptionPublicKey] =
      if (key.keySpec != keySpec)
        Left(
          EncryptionKeyGenerationError.GeneralKmsError(
            s"it is a ${key.keySpec} key but we are expecting a $keySpec encryption key"
          )
        )
      else Right(key)

    crypto.privateCrypto match {
      case crypto: KmsPrivateCrypto =>
        crypto
          .registerEncryptionKey(kmsKeyId)
          .subflatMap(epk => checkEncryptionScheme(epk))
          .valueOrFail("retrieve encryption key")
      case _ => fail("using an incorrect private crypto api")
    }

  }

}
