// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.symbolic

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.serialization.{DeserializationError, DeterministicEncoding}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ByteStringUtil
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

class SymbolicPureCrypto() extends CryptoPureApi {

  /** This flag is used to control the randomness during asymmetric encryption.
    * This is only intended to be used for testing purposes and it overrides the randomization flag given to
    * [[encryptWith]].
    */
  private val neverRandomizeAsymmetricEncryption = new AtomicBoolean(false)

  private val symmetricKeyCounter = new AtomicInteger
  private val randomnessCounter = new AtomicInteger
  private val signatureCounter = new AtomicInteger

  @VisibleForTesting
  def setRandomnessFlag(newValue: Boolean) = {
    neverRandomizeAsymmetricEncryption.set(newValue)
  }

  // iv to pre-append to the asymmetric ciphertext
  private val ivForAsymmetricEncryptInBytes = 16

  // NOTE: The following schemes are not really used by Symbolic crypto, but we pretend to support them
  override val defaultSymmetricKeyScheme: SymmetricKeyScheme = SymmetricKeyScheme.Aes128Gcm
  override val defaultPbkdfScheme: PbkdfScheme = PbkdfScheme.Argon2idMode1

  override protected[crypto] def sign(
      bytes: ByteString,
      signingKey: SigningPrivateKey,
  ): Either[SigningError, Signature] = {
    val counter = signatureCounter.getAndIncrement()
    Right(SymbolicPureCrypto.createSignature(bytes, signingKey.id, counter))
  }

  override protected[crypto] def verifySignature(
      bytes: ByteString,
      publicKey: SigningPublicKey,
      signature: Signature,
  ): Either[SignatureCheckError, Unit] =
    for {
      _ <- Either.cond(
        publicKey.format == CryptoKeyFormat.Symbolic,
        (),
        SignatureCheckError.InvalidKeyError(s"Public key $publicKey is not a symbolic key"),
      )
      _ <- Either.cond(
        publicKey.id == signature.signedBy,
        (),
        SignatureCheckError.SignatureWithWrongKey(
          s"Signature was signed by ${signature.signedBy} whereas key is ${publicKey.id}"
        ),
      )
      signedContent <- Either.cond(
        signature.unwrap.size() >= 4,
        signature.unwrap.substring(0, signature.unwrap.size() - 4),
        SignatureCheckError.InvalidSignature(
          signature,
          bytes,
          s"Symbolic signature ${signature.unwrap} lacks the four randomness bytes at the end",
        ),
      )
      _ <- Either.cond(
        signedContent == bytes,
        (),
        SignatureCheckError.InvalidSignature(
          signature,
          bytes,
          s"Symbolic signature with ${signedContent} does not match payload $bytes",
        ),
      )
    } yield ()

  override def defaultHashAlgorithm: HashAlgorithm = HashAlgorithm.Sha256

  override def generateSymmetricKey(
      scheme: SymmetricKeyScheme
  ): Either[EncryptionKeyGenerationError, SymmetricKey] = {
    val key = ByteString.copyFromUtf8(s"key-${symmetricKeyCounter.incrementAndGet()}")
    Right(SymmetricKey(CryptoKeyFormat.Symbolic, key, scheme))
  }

  override def createSymmetricKey(
      bytes: SecureRandomness,
      scheme: SymmetricKeyScheme,
  ): Either[EncryptionKeyCreationError, SymmetricKey] =
    Right(SymmetricKey(CryptoKeyFormat.Symbolic, bytes.unwrap, scheme))

  private def encryptWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
      randomized: Boolean,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    for {
      _ <- Either.cond(
        publicKey.format == CryptoKeyFormat.Symbolic,
        (),
        EncryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $publicKey"),
      )
      // For a symbolic encrypted message, prepend the key id that was used to encrypt
      payload = DeterministicEncoding
        .encodeString(publicKey.id.toProtoPrimitive)
        .concat(
          DeterministicEncoding.encodeBytes(
            message.toByteString(version)
          )
        )
      iv =
        if (randomized && !neverRandomizeAsymmetricEncryption.get())
          generateRandomByteString(ivForAsymmetricEncryptInBytes)
        else ByteString.copyFrom(new Array[Byte](ivForAsymmetricEncryptInBytes))

      encrypted = new AsymmetricEncrypted[M](iv.concat(payload), publicKey.id)
    } yield encrypted

  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, AsymmetricEncrypted[M]] =
    encryptWith(message, publicKey, version, randomized = true)

  def encryptDeterministicWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
  )(implicit traceContext: TraceContext): Either[EncryptionError, AsymmetricEncrypted[M]] =
    encryptWith(message, publicKey, version, randomized = false)

  override protected def decryptWithInternal[M](
      encrypted: AsymmetricEncrypted[M],
      privateKey: EncryptionPrivateKey,
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    for {
      _ <- Either.cond(
        privateKey.format == CryptoKeyFormat.Symbolic,
        (),
        DecryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $privateKey"),
      )
      // Remove iv
      ciphertextWithoutIv = encrypted.ciphertext.substring(ivForAsymmetricEncryptInBytes)

      // For a symbolic encrypted message, the encryption key id is prepended before the ciphertext/plaintext
      keyIdAndCiphertext <- DeterministicEncoding
        .decodeString(ciphertextWithoutIv)
        .leftMap[DecryptionError](err =>
          DecryptionError.FailedToDecrypt(
            s"Cannot extract public key id from symbolic encrypted payload: $err"
          )
        )
      (keyId, ciphertext) = keyIdAndCiphertext
      _ <- Either.cond(
        keyId == privateKey.id.toProtoPrimitive,
        (),
        DecryptionError.FailedToDecrypt(
          s"Provided symbolic private key $privateKey does not match used public key $keyId"
        ),
      )
      plaintextAndBytes <- DeterministicEncoding
        .decodeBytes(ciphertext)
        .leftMap[DecryptionError](err =>
          DecryptionError.FailedToDecrypt(
            s"Cannot extract ciphertext from symbolic encrypted payload: $err"
          )
        )
      (plaintext, bytes) = plaintextAndBytes
      _ <- Either.cond(
        bytes.isEmpty,
        (),
        DecryptionError.FailedToDecrypt(s"Payload contains more than key id and ciphertext"),
      )
      message <- deserialize(plaintext).leftMap(DecryptionError.FailedToDeserialize)

    } yield message

  override def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SymmetricKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]] =
    for {
      _ <- Either.cond(
        symmetricKey.format == CryptoKeyFormat.Symbolic,
        (),
        EncryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $symmetricKey"),
      )
      // For a symbolic symmetric encrypted message, prepend the symmetric key
      payload = DeterministicEncoding
        .encodeBytes(symmetricKey.key)
        .concat(
          DeterministicEncoding.encodeBytes(
            message.toByteString(version)
          )
        )
      encrypted = new Encrypted[M](payload)
    } yield encrypted

  override def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M] =
    for {
      _ <- Either.cond(
        symmetricKey.format == CryptoKeyFormat.Symbolic,
        (),
        DecryptionError.InvalidEncryptionKey(s"Provided key not a symbolic key: $symmetricKey"),
      )
      // For a symbolic symmetric encrypted message, the encryption key is prepended before the ciphertext/plaintext
      keyAndCiphertext <- DeterministicEncoding
        .decodeBytes(encrypted.ciphertext)
        .leftMap[DecryptionError](err =>
          DecryptionError.FailedToDecrypt(
            s"Cannot extract key from symbolic encrypted payload: $err"
          )
        )
      (key, ciphertext) = keyAndCiphertext
      _ <- Either.cond(
        key == symmetricKey.key,
        (),
        DecryptionError.FailedToDecrypt(
          s"Provided symbolic key $symmetricKey does not match used key $key"
        ),
      )
      plaintextAndBytes <- DeterministicEncoding
        .decodeBytes(ciphertext)
        .leftMap[DecryptionError](err =>
          DecryptionError.FailedToDecrypt(
            s"Cannot extract ciphertext from symbolic encrypted payload: $err"
          )
        )
      (plaintext, bytes) = plaintextAndBytes
      _ <- Either.cond(
        bytes.isEmpty,
        (),
        DecryptionError.FailedToDecrypt(s"Payload contains more than key and ciphertext"),
      )
      message <- deserialize(plaintext).leftMap(DecryptionError.FailedToDeserialize)

    } yield message

  override protected def computeHkdfInternal(
      keyMaterial: ByteString,
      outputBytes: Int,
      info: HkdfInfo,
      salt: ByteString,
      algorithm: HmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] =
    NonNegativeInt.create(outputBytes) match {
      case Left(_) => Left(HkdfError.HkdfOutputNegative(outputBytes))
      case Right(size) =>
        Right(
          SecureRandomness(
            ByteStringUtil
              .padOrTruncate(keyMaterial.concat(salt).concat(info.bytes), size)
          )
        )
    }

  override protected def hkdfExpandInternal(
      keyMaterial: SecureRandomness,
      outputBytes: Int,
      info: HkdfInfo,
      algorithm: HmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] =
    Right(SecureRandomness(keyMaterial.unwrap.concat(info.bytes)))

  override def computeHkdf(
      keyMaterial: ByteString,
      outputBytes: Int,
      info: HkdfInfo,
      salt: ByteString,
      algorithm: HmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] =
    computeHkdfInternal(keyMaterial, outputBytes, info, salt, algorithm)

  override protected def generateRandomBytes(length: Int): Array[Byte] = {
    // Not really random
    val random =
      DeterministicEncoding.encodeInt(randomnessCounter.getAndIncrement()).toByteArray.take(length)

    // Pad the rest of the request bytes with 0 if necessary
    if (random.length < length)
      random.concat(new Array[Byte](length - random.length))
    else
      random
  }

  override def deriveSymmetricKey(
      password: String,
      symmetricKeyScheme: SymmetricKeyScheme,
      pbkdfScheme: PbkdfScheme,
      saltO: Option[SecureRandomness],
  ): Either[PasswordBasedEncryptionError, PasswordBasedEncryptionKey] = {
    val salt =
      saltO
        .getOrElse(generateSecureRandomness(pbkdfScheme.defaultSaltLengthInBytes))

    // We just hash the salt and password, then truncate/pad to desired length
    val hash = TestHash.build.addWithoutLengthPrefix(salt.unwrap).add(password).finish()
    val keyBytes =
      ByteStringUtil.padOrTruncate(
        hash.unwrap,
        NonNegativeInt.tryCreate(symmetricKeyScheme.keySizeInBytes),
      )
    val key = SymmetricKey(CryptoKeyFormat.Symbolic, keyBytes, symmetricKeyScheme)

    Right(PasswordBasedEncryptionKey(key, salt))
  }
}

object SymbolicPureCrypto {

  /** Symbolic signatures use the content as the signature, padded with a `counter` to simulate the general non-determinism of signing */
  private[symbolic] def createSignature(
      bytes: ByteString,
      signingKey: Fingerprint,
      counter: Int,
  ): Signature =
    new Signature(
      SignatureFormat.Raw,
      bytes.concat(DeterministicEncoding.encodeInt(counter)),
      signingKey,
    )
}
