// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CachingConfigs
import com.digitalasset.canton.crypto.CryptoPureApiError.KeyParseAndValidateError
import com.digitalasset.canton.crypto.SigningKeyUsage.compatibleUsageForSignAndVerify
import com.digitalasset.canton.crypto.provider.jce.{JceJavaKeyConverter, JceSecurityProvider}
import com.digitalasset.canton.util.ErrorUtil
import com.google.crypto.tink.internal.EllipticCurvesUtil
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.bouncycastle.math.ec.rfc8032.Ed25519

import java.security.interfaces.ECPublicKey
import java.security.spec.{ECGenParameterSpec, ECParameterSpec}
import java.security.{AlgorithmParameters, GeneralSecurityException, PublicKey as JPublicKey}
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.concurrent.blocking

object CryptoKeyValidation {

  // Keeps track of the public keys that have been validated.
  // TODO(#15634): Once the crypto provider is available in the validation context, move this to the provider object
  // and replace it with a proper cache.
  private lazy val validatedPublicKeys: TrieMap[PublicKey, Either[KeyParseAndValidateError, Unit]] =
    TrieMap.empty

  // To prevent concurrent cache cleanups
  private val cacheLock = new Object

  private[crypto] def parseAndValidateDerKey(
      publicKey: PublicKey
  ): Either[KeyParseAndValidateError, JPublicKey] =
    JceJavaKeyConverter
      .toJava(publicKey)
      .leftMap(err => KeyParseAndValidateError(err.show))

  /** Validates that the given public key is a correctly encoded Ed25519 key and represents a valid
    * point on the Ed25519 curve. Assumes that the public key is provided in DER-encoded
    * SubjectPublicKeyInfo (SPKI) format.
    */
  private[crypto] def validateEd25519PublicKey(
      pubKey: PublicKey
  ): Either[KeyParseAndValidateError, Unit] =
    Either
      .catchOnly[GeneralSecurityException] {
        // Extract raw 32-byte Ed25519 public key from DER-encoded SPKI
        val spki = SubjectPublicKeyInfo.getInstance(pubKey.key.toByteArray)
        val rawKeyBytes = spki.getPublicKeyData.getBytes
        Ed25519.validatePublicKeyFull(rawKeyBytes, 0)
      }
      .map(_ => ())
      .leftMap(err =>
        KeyParseAndValidateError(
          s"EC key not in curve Ed25519: ${ErrorUtil.messageWithStacktrace(err)}"
        )
      )

  /** Validates the given public key by ensuring that the EC public key point lies on the correct
    * curve, using Tink `checkPointOnCurve` primitive.
    */
  private[crypto] def validateEcPublicKey(
      pubKey: JPublicKey,
      ecKeySpec: EcKeySpec,
  ): Either[KeyParseAndValidateError, Unit] =
    for {
      ecPublicKey <- pubKey match {
        case k: ECPublicKey => Right(k)
        case _ =>
          Left(KeyParseAndValidateError(s"Public key is not an EC public key"))
      }
      point = ecPublicKey.getW

      paramSpec = new ECGenParameterSpec(ecKeySpec.jcaCurveName)
      params = AlgorithmParameters.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
      _ = params.init(paramSpec)
      ecSpec = params.getParameterSpec(classOf[ECParameterSpec])
      curve = ecSpec.getCurve

      // Ensures the point lies on the elliptic curve defined by the given parameters.
      _ <- Either
        .catchOnly[GeneralSecurityException](
          EllipticCurvesUtil.checkPointOnCurve(point, curve)
        )
        .leftMap(err =>
          KeyParseAndValidateError(
            s"EC key not in curve $curve: ${ErrorUtil.messageWithStacktrace(err)}"
          )
        )
    } yield ()

  // TODO(#15634): Verify crypto scheme as part of key validation
  /** Parses and validates a public key. Validates:
    *   - Public key format and serialization
    *   - Elliptic curve public key point on the curve
    *
    * Validation results are cached.
    */
  private[crypto] def parseAndValidatePublicKey[E](
      publicKey: PublicKey,
      errFn: String => E,
  ): Either[E, Unit] = {

    @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
    lazy val parseRes = publicKey.format match {
      case CryptoKeyFormat.DerX509Spki =>
        for {
          jKey <- parseAndValidateDerKey(publicKey)
          _ <- publicKey match {
            case encKey: EncryptionPublicKey =>
              encKey.keySpec match {
                case ks: EcKeySpec => validateEcPublicKey(jKey, ks)
                // TODO(#15652): validate RSA public key
                case EncryptionKeySpec.Rsa2048 => Right(None)
              }
            case signKey: SigningPublicKey =>
              signKey.keySpec match {
                case SigningKeySpec.EcCurve25519 => validateEd25519PublicKey(publicKey)
                case ks: EcKeySpec => validateEcPublicKey(jKey, ks)
              }
            case _ => Left(KeyParseAndValidateError("Unknown key type"))
          }
        } yield ()
      case CryptoKeyFormat.Symbolic =>
        Either.unit
      case format @ (CryptoKeyFormat.Der | CryptoKeyFormat.Raw | CryptoKeyFormat.DerPkcs8Pki) =>
        Left(KeyParseAndValidateError(s"Invalid format for public key: $format"))
    }

    // Temporary workaround to clear this TrieMap and prevent memory leaks.
    // TODO(#15634): Remove this once `validatedPublicKeys` uses a proper cache.
    blocking(
      cacheLock.synchronized {
        if (
          validatedPublicKeys.size > CachingConfigs.defaultPublicKeyConversionCache.maximumSize.value
        ) {
          validatedPublicKeys.clear()
        }
      }
    )
    // If the result is already in the cache it means the key has already been validated.
    validatedPublicKeys
      .getOrElseUpdate(publicKey, parseRes)
      .leftMap(err => errFn(s"Failed to deserialize ${publicKey.format} public key: $err"))
  }

  private[crypto] def selectEncryptionAlgorithmSpec[E](
      keySpec: EncryptionKeySpec,
      defaultAlgorithmSpec: EncryptionAlgorithmSpec,
      supportedAlgorithmSpecs: Set[EncryptionAlgorithmSpec],
      errFn: EncryptionAlgorithmSpec => E,
  ): Either[E, EncryptionAlgorithmSpec] =
    if (defaultAlgorithmSpec.supportedEncryptionKeySpecs.contains(keySpec))
      Right(defaultAlgorithmSpec)
    else
      supportedAlgorithmSpecs
        .find(_.supportedEncryptionKeySpecs.contains(keySpec))
        .toRight(errFn(defaultAlgorithmSpec))

  private[crypto] def selectSigningAlgorithmSpec[E](
      keySpec: SigningKeySpec,
      defaultAlgorithmSpec: SigningAlgorithmSpec,
      supportedAlgorithmSpecs: Set[SigningAlgorithmSpec],
      errFn: SigningAlgorithmSpec => E,
  ): Either[E, SigningAlgorithmSpec] =
    if (defaultAlgorithmSpec.supportedSigningKeySpecs.contains(keySpec))
      Right(defaultAlgorithmSpec)
    else
      supportedAlgorithmSpecs
        .find(_.supportedSigningKeySpecs.contains(keySpec))
        .toRight(errFn(defaultAlgorithmSpec))

  private[crypto] def ensureCryptoKeySpec[KeySpec, E](
      keySpec: KeySpec,
      supportedKeySpecs: Set[KeySpec],
      errFnKey: (KeySpec, Set[KeySpec]) => E,
  ): Either[E, Unit] =
    Either.cond(
      supportedKeySpecs.contains(keySpec),
      (),
      errFnKey(keySpec, supportedKeySpecs),
    )

  private[crypto] def ensureCryptoAlgorithmSpec[AlgorithmSpec, E](
      algorithmSpec: AlgorithmSpec,
      supportedAlgorithmSpecs: Set[AlgorithmSpec],
      errFnAlgorithm: (AlgorithmSpec, Set[AlgorithmSpec]) => E,
  ): Either[E, Unit] =
    Either.cond(
      supportedAlgorithmSpecs.contains(algorithmSpec),
      (),
      errFnAlgorithm(algorithmSpec, supportedAlgorithmSpecs),
    )

  /** Ensures that a given key specification is supported by the selected crypto algorithm. It also
    * checks if this crypto algorithm is part of the set of supported algorithms.
    */
  private[crypto] def ensureCryptoSpec[KeySpec, AlgorithmSpec, E](
      keySpec: KeySpec,
      algorithmSpec: AlgorithmSpec,
      supportedKeySpecs: Set[KeySpec],
      supportedAlgorithmSpecs: Set[AlgorithmSpec],
      errFnKey: (KeySpec, Set[KeySpec]) => E,
      errFnAlgorithm: (AlgorithmSpec, Set[AlgorithmSpec]) => E,
  ): Either[E, Unit] =
    for {
      _ <- ensureCryptoKeySpec(keySpec, supportedKeySpecs, errFnKey)
      _ <- ensureCryptoAlgorithmSpec(algorithmSpec, supportedAlgorithmSpecs, errFnAlgorithm)
    } yield ()

  private[crypto] def ensureFormat[E](
      actual: CryptoKeyFormat,
      acceptedFormats: Set[CryptoKeyFormat],
      errFn: (CryptoKeyFormat, Set[CryptoKeyFormat]) => E,
  ): Either[E, Unit] =
    Either.cond(
      acceptedFormats.contains(actual),
      (),
      errFn(actual, acceptedFormats),
    )

  /** @param errFn
    *   An error function that takes the key's fingerprint, its actual key usages, and the expected
    *   usages, and is invoked when these do not match.
    */
  private[crypto] def ensureUsage[E](
      usage: NonEmpty[Set[SigningKeyUsage]],
      keyUsage: NonEmpty[Set[SigningKeyUsage]],
      fingerprint: Fingerprint,
      errFn: (Fingerprint, Set[SigningKeyUsage], Set[SigningKeyUsage]) => E,
  ): Either[E, Unit] =
    Either.cond(
      compatibleUsageForSignAndVerify(keyUsage, usage),
      (),
      errFn(fingerprint, keyUsage.forgetNE, usage.forgetNE),
    )

  private[crypto] def ensureSignatureFormat[E](
      actual: SignatureFormat,
      acceptedFormats: Set[SignatureFormat],
      errFn: (SignatureFormat, Set[SignatureFormat]) => E,
  ): Either[E, Unit] =
    Either.cond(
      acceptedFormats.contains(actual),
      (),
      errFn(actual, acceptedFormats),
    )

}
