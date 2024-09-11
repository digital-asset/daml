// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{HasToByteString, HasVersionedToByteString, ProtocolVersion}
import com.google.protobuf.ByteString

/** Wraps the CryptoPureApi to include static domain parameters, ensuring that during signature verification and
  * decryption (both asymmetric and symmetric), the static domain parameters are explicitly checked. This is crucial
  * because a malicious counter participant could potentially use a downgraded scheme. For other methods, such as key
  * generation, signing, or encryption by this (honest) participant, we rely on the domain handshake to ensure that
  * only supported schemes within the domain are used.
  *
  * TODO(#20714): decryption checks come in a separate PR
  */
final class DomainCryptoPureApi(
    staticDomainParameters: StaticDomainParameters,
    pureCrypto: CryptoPureApi,
) extends CryptoPureApi {

  private def checkAgainstStaticDomainParams(
      scheme: SigningKeyScheme
  ): Either[SignatureCheckError, Unit] =
    Either.cond(
      staticDomainParameters.requiredSigningKeySchemes.contains(scheme),
      (),
      SignatureCheckError.InvalidCryptoScheme(
        s"The signing key scheme $scheme is not part of the " +
          s"required schemes: ${staticDomainParameters.requiredSigningKeySchemes}"
      ),
    )

  override def verifySignature(
      hash: Hash,
      publicKey: SigningPublicKey,
      signature: Signature,
  ): Either[SignatureCheckError, Unit] =
    for {
      _ <- checkAgainstStaticDomainParams(publicKey.scheme)
      _ <- pureCrypto.verifySignature(hash, publicKey, signature)
    } yield ()

  override protected[crypto] def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
  ): Either[SignatureCheckError, Unit] =
    for {
      _ <- checkAgainstStaticDomainParams(publicKey.scheme)
      _ <- pureCrypto.verifySignature(bytes, publicKey, signature)
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

  override def encryptWithVersion[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    pureCrypto.encryptWithVersion(message, publicKey, version, encryptionAlgorithmSpec)

  override def encryptWith[M <: HasToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    pureCrypto.encryptWith(message, publicKey, encryptionAlgorithmSpec)

  override def encryptDeterministicWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  )(implicit traceContext: TraceContext): Either[EncryptionError, AsymmetricEncrypted[M]] =
    pureCrypto.encryptDeterministicWith(message, publicKey, version, encryptionAlgorithmSpec)

  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SymmetricKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]] = pureCrypto.encryptWith(message, symmetricKey, version)

  override def encryptWith[M <: HasToByteString](
      message: M,
      symmetricKey: SymmetricKey,
  ): Either[EncryptionError, Encrypted[M]] = pureCrypto.encryptWith(message, symmetricKey)

  override def decryptWith[M](
      encrypted: Encrypted[M],
      symmetricKey: SymmetricKey,
  )(deserialize: ByteString => Either[DeserializationError, M]): Either[DecryptionError, M] =
    pureCrypto.decryptWith(encrypted, symmetricKey)(deserialize)

  override def defaultHashAlgorithm: HashAlgorithm = pureCrypto.defaultHashAlgorithm

  override protected[crypto] def hkdfExpandInternal(
      keyMaterial: SecureRandomness,
      outputBytes: Int,
      info: HkdfInfo,
      algorithm: HmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] =
    pureCrypto.hkdfExpandInternal(keyMaterial, outputBytes, info, algorithm)

  override protected[crypto] def computeHkdfInternal(
      keyMaterial: ByteString,
      outputBytes: Int,
      info: HkdfInfo,
      salt: ByteString,
      algorithm: HmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] =
    pureCrypto.computeHkdfInternal(keyMaterial, outputBytes, info, salt, algorithm)

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

  override protected[crypto] def sign(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
  ): Either[SigningError, Signature] = pureCrypto.sign(bytes, signingKey)
}
