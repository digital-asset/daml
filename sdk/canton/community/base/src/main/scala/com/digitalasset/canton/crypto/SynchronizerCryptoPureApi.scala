// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.SignatureCheckError.UnsupportedKeySpec
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HasToByteString
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

/** Wraps the CryptoPureApi to include static synchronizer parameters, ensuring that during
  * signature verification and decryption (both asymmetric and symmetric), the static synchronizer
  * parameters are explicitly checked. This is crucial because a malicious counter participant could
  * potentially use a downgraded scheme. For other methods, such as key generation, signing, or
  * encryption by this (honest) participant, we rely on the synchronizer handshake to ensure that
  * only supported schemes within the synchronizer are used.
  *
  * TODO(#25260): Refactor SynchronizerCryptoPureApi
  */
final class SynchronizerCryptoPureApi(
    staticSynchronizerParameters: StaticSynchronizerParameters,
    @VisibleForTesting
    val pureCrypto: CryptoPureApi,
) extends CryptoPureApi {

  private def checkAgainstStaticSynchronizerParams(
      keySpec: SigningKeySpec
  ): Either[SignatureCheckError, Unit] =
    Either.cond(
      staticSynchronizerParameters.requiredSigningSpecs.keys.contains(keySpec),
      (),
      UnsupportedKeySpec(
        keySpec,
        staticSynchronizerParameters.requiredSigningSpecs.keys,
      ),
    )

  override def verifySignature(
      hash: Hash,
      publicKey: SigningPublicKey,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): Either[SignatureCheckError, Unit] =
    for {
      _ <- checkAgainstStaticSynchronizerParams(publicKey.keySpec)
      _ <- pureCrypto.verifySignature(hash, publicKey, signature, usage)
    } yield ()

  override def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit traceContext: TraceContext): Either[SignatureCheckError, Unit] =
    for {
      _ <- checkAgainstStaticSynchronizerParams(publicKey.keySpec)
      _ <- pureCrypto.verifySignature(bytes, publicKey, signature, usage)
    } yield ()

  override protected[crypto] def decryptWithInternal[M](
      encrypted: AsymmetricEncrypted[M],
      privateKey: EncryptionPrivateKey,
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] = pureCrypto.decryptWithInternal(encrypted, privateKey)(deserialize)

  override def defaultSymmetricKeyScheme: SymmetricKeyScheme = pureCrypto.defaultSymmetricKeyScheme

  override def generateSymmetricKey(
      scheme: SymmetricKeyScheme
  ): Either[EncryptionKeyGenerationError, SymmetricKey] = pureCrypto.generateSymmetricKey(scheme)

  override def createSymmetricKey(
      bytes: SecureRandomness,
      scheme: SymmetricKeyScheme,
  ): Either[EncryptionKeyCreationError, SymmetricKey] = pureCrypto.createSymmetricKey(bytes, scheme)

  override def defaultEncryptionAlgorithmSpec: EncryptionAlgorithmSpec =
    pureCrypto.defaultEncryptionAlgorithmSpec

  override def supportedEncryptionAlgorithmSpecs: NonEmpty[Set[EncryptionAlgorithmSpec]] =
    pureCrypto.supportedEncryptionAlgorithmSpecs

  override def encryptWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    pureCrypto.encryptWith(message, publicKey, encryptionAlgorithmSpec)

  override def encryptDeterministicWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  )(implicit traceContext: TraceContext): Either[EncryptionError, AsymmetricEncrypted[M]] =
    pureCrypto.encryptDeterministicWith(message, publicKey, encryptionAlgorithmSpec)

  override private[crypto] def encryptSymmetricWith(
      data: ByteString,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, ByteString] = pureCrypto.encryptSymmetricWith(data, symmetricKey)

  override def decryptWith[M](
      encrypted: Encrypted[M],
      symmetricKey: SymmetricKey,
  )(deserialize: ByteString => Either[DeserializationError, M]): Either[DecryptionError, M] =
    pureCrypto.decryptWith(encrypted, symmetricKey)(deserialize)

  override def defaultHashAlgorithm: HashAlgorithm = pureCrypto.defaultHashAlgorithm

  override protected[crypto] def defaultPbkdfScheme: PbkdfScheme = pureCrypto.defaultPbkdfScheme

  override def deriveSymmetricKey(
      password: String,
      symmetricKeyScheme: SymmetricKeyScheme,
      pbkdfScheme: PbkdfScheme,
      saltO: Option[SecureRandomness],
  ): Either[PasswordBasedEncryptionError, PasswordBasedEncryptionKey] =
    pureCrypto.deriveSymmetricKey(password, symmetricKeyScheme, pbkdfScheme, saltO)

  override protected[crypto] def generateRandomBytes(length: Int): Array[Byte] =
    pureCrypto.generateRandomBytes(length)

  override def defaultSigningAlgorithmSpec: SigningAlgorithmSpec =
    pureCrypto.defaultSigningAlgorithmSpec

  override def supportedSigningAlgorithmSpecs: NonEmpty[Set[SigningAlgorithmSpec]] =
    pureCrypto.supportedSigningAlgorithmSpecs

  override protected[crypto] def signBytes(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec = defaultSigningAlgorithmSpec,
  )(implicit traceContext: TraceContext): Either[SigningError, Signature] =
    pureCrypto.signBytes(bytes, signingKey, usage, signingAlgorithmSpec)
}
