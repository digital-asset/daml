// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{CacheConfig, CachingConfigs}
import com.digitalasset.canton.crypto.CryptoPureApiError.KeyParseAndValidateError
import com.digitalasset.canton.crypto.SigningKeyUsage.compatibleUsageForSignAndVerify
import com.digitalasset.canton.crypto.provider.jce.{JceJavaKeyConverter, JceSecurityProvider}
import com.digitalasset.canton.util.{EitherUtil, ErrorUtil}
import com.github.blemale.scaffeine.Cache
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import org.bouncycastle.jcajce.provider.asymmetric.edec.{BCEdDSAPrivateKey, BCEdDSAPublicKey}
import org.bouncycastle.math.ec.custom.sec.{SecP256K1Curve, SecP256R1Curve, SecP384R1Curve}
import org.bouncycastle.math.ec.rfc8032.Ed25519.{SECRET_KEY_SIZE, validatePublicKeyFull}

import java.math.BigInteger
import java.security.interfaces.{ECPrivateKey, ECPublicKey, RSAPrivateKey, RSAPublicKey}
import java.security.spec.{
  ECGenParameterSpec,
  ECParameterSpec,
  ECPrivateKeySpec,
  RSAKeyGenParameterSpec,
}
import java.security.{
  AlgorithmParameters,
  GeneralSecurityException,
  KeyFactory,
  PrivateKey as JPrivateKey,
  PublicKey as JPublicKey,
}
import scala.annotation.nowarn

object CryptoKeyValidation {

  private val cacheConfig = CacheConfig(
    maximumSize = CachingConfigs.defaultPublicKeyConversionCache.maximumSize
  )

  // Keeps track of the public keys that have been validated.
  private lazy val validatedPublicKeys: Cache[Fingerprint, Either[KeyParseAndValidateError, Unit]] =
    cacheConfig
      .buildScaffeineWithoutExecutor()
      .build[Fingerprint, Either[KeyParseAndValidateError, Unit]]()

  private lazy val validatedPrivateKeys
      : Cache[Fingerprint, Either[KeyParseAndValidateError, Unit]] =
    cacheConfig
      .buildScaffeineWithoutExecutor()
      .build[Fingerprint, Either[KeyParseAndValidateError, Unit]]()

  @VisibleForTesting
  def clearValidationCaches(): Unit = {
    validatedPublicKeys.invalidateAll()
    validatedPrivateKeys.invalidateAll()
  }

  /** Validates a symmetric key by checking:
    *   - Symmetric key format
    *   - Symmetric key has the correct length
    */
  private[crypto] def validateSymmetricKey[E](
      symmetricKey: SymmetricKey,
      errFn: String => E,
  ): Either[E, Unit] = {

    def validateAes128Key(): Either[KeyParseAndValidateError, Unit] =
      EitherUtil.condUnit(
        symmetricKey.key.size() == SymmetricKeyScheme.Aes128Gcm.keySizeInBytes,
        KeyParseAndValidateError(
          s"AES128 key size ${symmetricKey.key.size()} does not match expected " +
            s"size ${SymmetricKeyScheme.Aes128Gcm.keySizeInBytes}."
        ),
      )

    @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
    lazy val parseRes = symmetricKey.format match {
      case CryptoKeyFormat.Raw =>
        symmetricKey.scheme match {
          case SymmetricKeyScheme.Aes128Gcm =>
            validateAes128Key()
        }
      case CryptoKeyFormat.Symbolic =>
        Either.unit
      case format @ (CryptoKeyFormat.Der | CryptoKeyFormat.DerX509Spki |
          CryptoKeyFormat.DerPkcs8Pki) =>
        Left(KeyParseAndValidateError(s"Invalid format for symmetric key: $format"))
    }

    parseRes.leftMap(err => errFn(s"Failed to validate ${symmetricKey.format} symmetric key: $err"))
  }

  private[crypto] def parseAndValidateDerPublicKey(
      publicKey: PublicKey
  ): Either[KeyParseAndValidateError, JPublicKey] =
    JceJavaKeyConverter
      .toJava(publicKey)
      .leftMap(err => KeyParseAndValidateError(err.show))

  private[crypto] def parseAndValidateDerPrivateKey(
      privateKey: PrivateKey
  ): Either[KeyParseAndValidateError, JPrivateKey] =
    JceJavaKeyConverter
      .toJava(privateKey)
      .leftMap(err => KeyParseAndValidateError(err.show))

  /** Validates that the given Ed25519 private key contains a valid 32-byte raw private key. Assumes
    * the key is encoded using the DER-encoded PKCS#8 structure.
    */
  private def validateEd25519PrivateKey(
      privateKey: JPrivateKey
  ): Either[KeyParseAndValidateError, Unit] =
    for {
      edPrivateKey <- privateKey match {
        case k: BCEdDSAPrivateKey => Right(k)
        case _ =>
          Left(KeyParseAndValidateError(s"Private key is not an ED25519 private key"))
      }
      // Extract raw 32-byte Ed25519 private key from PKCS #8 PKI
      rawPrivateKey <- CryptoKeyFormat.extractPrivateKeyFromPkcs8Pki(
        ByteString.copyFrom(edPrivateKey.getEncoded)
      )
      _ <- EitherUtil.condUnit(
        rawPrivateKey.length == SECRET_KEY_SIZE,
        KeyParseAndValidateError(
          s"Ed25519 private key seed should be $SECRET_KEY_SIZE bytes, " +
            s"got ${rawPrivateKey.length}"
        ),
      )
    } yield ()

  /** Validates that the given public key is a correctly encoded Ed25519 key and represents a valid
    * point on the Ed25519 curve. Assumes that the public key is provided in DER-encoded
    * SubjectPublicKeyInfo (SPKI) format.
    */
  private def validateEd25519PublicKey(
      pubKey: JPublicKey
  ): Either[KeyParseAndValidateError, Unit] =
    for {
      edPublicKey <- pubKey match {
        case k: BCEdDSAPublicKey => Right(k)
        case _ =>
          Left(KeyParseAndValidateError(s"Public key is not an ED25519 public key"))
      }
      // Extract raw 32-byte Ed25519 public key from DER-encoded SPKI
      rawKeyBytes <- CryptoKeyFormat.extractPublicKeyFromX509Spki(
        ByteString.copyFrom(edPublicKey.getEncoded)
      )
      _ <- Either
        .catchOnly[GeneralSecurityException] {
          validatePublicKeyFull(rawKeyBytes, 0)
        }
        .map(_ => ())
        .leftMap(err =>
          KeyParseAndValidateError(
            s"Ed25519 public key validation failed: ${ErrorUtil.messageWithStacktrace(err)}"
          )
        )
    } yield ()

  /** Validates the private key by making sure the private scalar lies within the valid range for
    * the curve.
    */
  private def validateEcPrivateKey(
      privKey: JPrivateKey,
      ecKeySpec: EcKeySpec,
  ): Either[KeyParseAndValidateError, Unit] =
    for {
      ecPublicKey <- privKey match {
        case k: ECPrivateKey => Right(k)
        case _ =>
          Left(KeyParseAndValidateError(s"Private key is not an EC private key"))
      }
      s = ecPublicKey.getS

      paramSpec = new ECGenParameterSpec(ecKeySpec.jcaCurveName)
      params = AlgorithmParameters.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
      _ = params.init(paramSpec)

      ecSpec = params.getParameterSpec(classOf[ECParameterSpec])
      orderEcSpec = ecSpec.getOrder

      kf = KeyFactory.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
      keySpec = kf.getKeySpec(privKey, classOf[ECPrivateKeySpec])

      curveFromKey = keySpec.getParams
      curveFromSpec = ecSpec

      // Compare the curve parameters
      mismatches = Seq(
        "curve" -> (curveFromKey.getCurve, curveFromSpec.getCurve),
        "generator" -> (curveFromKey.getGenerator, curveFromSpec.getGenerator),
        "order" -> (curveFromKey.getOrder, curveFromSpec.getOrder),
        "cofactor" -> (curveFromKey.getCofactor, curveFromSpec.getCofactor),
      ).collect {
        case (name, (actual, expected)) if actual != expected =>
          s"$name: actual = $actual, expected = $expected"
      }

      _ <- EitherUtil.condUnit(
        mismatches.isEmpty,
        KeyParseAndValidateError(
          s"EC private key parameters do not match expected curve:\n${mismatches.mkString("\n")}"
        ),
      )

      // the private key scalar `d` must be within the valid range for the curve: 1 <= d < order. Refer to
      // https://www.secg.org/sec1-v2.pdf, section 3.2.1 Elliptic Curve Key Pair Generation Primitive.
      d = BigInt(s)
      order = BigInt(orderEcSpec)
      _ <- EitherUtil.condUnit(
        d > 0 && d < order,
        KeyParseAndValidateError(
          s"Private key scalar is out of range for curve ${ecKeySpec.jcaCurveName}"
        ),
      )
    } yield ()

  /** Validates the public key by ensuring that the EC public key point lies on the correct curve.
    */
  private def validateEcPublicKey(
      pubKey: JPublicKey,
      ecKeySpec: EcKeySpec,
  ): Either[KeyParseAndValidateError, Unit] =
    for {
      ecPublicKey <- pubKey match {
        case k: ECPublicKey => Right(k)
        case _ =>
          Left(KeyParseAndValidateError(s"Public key is not an EC public key"))
      }

      x = ecPublicKey.getW.getAffineX
      y = ecPublicKey.getW.getAffineY

      curve <- ecKeySpec match {
        case EncryptionKeySpec.EcP256 | SigningKeySpec.EcP256 =>
          Right(new SecP256R1Curve())
        case SigningKeySpec.EcSecp256k1 =>
          Right(new SecP256K1Curve())
        case SigningKeySpec.EcP384 =>
          Right(new SecP384R1Curve())
        case other =>
          Left(KeyParseAndValidateError(s"Unsupported elliptic curve key spec: $other"))
      }

      // Ensures the point lies on the elliptic curve defined by the given parameters.
      point <- Either
        .catchOnly[GeneralSecurityException](curve.createPoint(x, y))
        .leftMap(err =>
          KeyParseAndValidateError(
            s"Failed to create point on curve '${ecKeySpec.jcaCurveName}': ${ErrorUtil.messageWithStacktrace(err)}"
          )
        )
      _ <- EitherUtil.condUnit(
        point.isValid && !point.isInfinity,
        KeyParseAndValidateError(
          s"EC key not in curve '${ecKeySpec.jcaCurveName}'."
        ),
      )
    } yield ()

  private def checkRsaModulus(modulus: BigInteger) =
    EitherUtil.condUnit(
      modulus.bitLength == EncryptionKeySpec.Rsa2048.keySizeInBits,
      KeyParseAndValidateError(
        s"RSA key modulus size ${modulus.bitLength} does not match expected " +
          s"size ${EncryptionKeySpec.Rsa2048.keySizeInBits}"
      ),
    )

  /** Validates the given private key by ensuring that the RSA private key has the correct modulus
    * length and private exponent.
    */
  private def validateRsa2048PrivateKey(
      privKey: JPrivateKey
  ): Either[KeyParseAndValidateError, Unit] = {

    def checkPrivateExponent(privateExponent: BigInteger) =
      EitherUtil.condUnit(
        // Refer to https://www.rfc-editor.org/rfc/rfc3447.html#section-3.2
        privateExponent.signum() > 0,
        KeyParseAndValidateError(
          s"RSA private exponent must be a positive integer"
        ),
      )

    for {
      rsaPrivateKey <- privKey match {
        case k: RSAPrivateKey => Right(k)
        case _ =>
          Left(KeyParseAndValidateError(s"Public key is not an RSA private key"))
      }
      modulus = rsaPrivateKey.getModulus
      privateExponent = rsaPrivateKey.getPrivateExponent
      _ <- checkRsaModulus(modulus)
      _ <- checkPrivateExponent(privateExponent)
    } yield ()
  }

  /** Validates the given public key by ensuring that the RSA public key has the correct modulus
    * length and public exponent.
    */
  private def validateRsa2048PublicKey(
      pubKey: JPublicKey
  ): Either[KeyParseAndValidateError, Unit] = {

    def checkPublicExponent(publicExponent: BigInteger) =
      EitherUtil.condUnit(
        publicExponent == RSAKeyGenParameterSpec.F4,
        KeyParseAndValidateError(
          s"RSA public exponent $publicExponent does not match expected value " +
            s"${RSAKeyGenParameterSpec.F4}"
        ),
      )

    for {
      rsaPublicKey <- pubKey match {
        case k: RSAPublicKey => Right(k)
        case _ =>
          Left(KeyParseAndValidateError(s"Public key is not an RSA public key"))
      }
      modulus = rsaPublicKey.getModulus
      publicExponent = rsaPublicKey.getPublicExponent
      _ <- checkRsaModulus(modulus)
      _ <- checkPublicExponent(publicExponent)
    } yield ()
  }

  /** Parses and validates a private key. Validates:
    *   - Private key format and serialization
    *   - Elliptic curve private key scalar is within the valid range for the curve
    *   - RSA private key is valid (e.g. correct modulus length)
    *
    * Validation results are cached.
    */
  private[crypto] def parseAndValidatePrivateKey[E](
      privateKey: PrivateKey,
      errFn: String => E,
      cacheValidation: Boolean = true,
  ): Either[E, Unit] = {

    @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
    lazy val parseRes = privateKey.format match {
      case CryptoKeyFormat.DerPkcs8Pki =>
        for {
          jKey <- parseAndValidateDerPrivateKey(privateKey)
          _ <- privateKey match {
            case encKey: EncryptionPrivateKey =>
              encKey.keySpec match {
                case ks: EcKeySpec => validateEcPrivateKey(jKey, ks)
                case EncryptionKeySpec.Rsa2048 => validateRsa2048PrivateKey(jKey)
              }
            case signKey: SigningPrivateKey =>
              signKey.keySpec match {
                case SigningKeySpec.EcCurve25519 => validateEd25519PrivateKey(jKey)
                case ks: EcKeySpec => validateEcPrivateKey(jKey, ks)
              }
            case _ => Left(KeyParseAndValidateError("Unknown key type"))
          }
        } yield ()
      case CryptoKeyFormat.Symbolic =>
        Either.unit
      case format @ (CryptoKeyFormat.Der | CryptoKeyFormat.Raw | CryptoKeyFormat.DerX509Spki) =>
        Left(KeyParseAndValidateError(s"Invalid format for private key: $format"))
    }

    // If the result is already in the cache it means the key has already been validated.
    (if (cacheValidation)
       validatedPrivateKeys.get(privateKey.id, _ => parseRes)
     else parseRes).leftMap(err =>
      errFn(s"Failed to deserialize or validate ${privateKey.format} private key: $err")
    )
  }

  /** Parses and validates a public key. Validates:
    *   - Public key format and serialization
    *   - Elliptic curve public key point on the curve
    *   - RSA public key is valid (e.g. correct modulus length)
    *
    * Validation results are cached.
    */
  private[crypto] def parseAndValidatePublicKey[E](
      publicKey: PublicKey,
      errFn: String => E,
      cacheValidation: Boolean = true,
  ): Either[E, Unit] = {

    @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
    lazy val parseRes = publicKey.format match {
      case CryptoKeyFormat.DerX509Spki =>
        for {
          jKey <- parseAndValidateDerPublicKey(publicKey)
          _ <- publicKey match {
            case encKey: EncryptionPublicKey =>
              encKey.keySpec match {
                case ks: EcKeySpec => validateEcPublicKey(jKey, ks)
                case EncryptionKeySpec.Rsa2048 => validateRsa2048PublicKey(jKey)
              }
            case signKey: SigningPublicKey =>
              signKey.keySpec match {
                case SigningKeySpec.EcCurve25519 => validateEd25519PublicKey(jKey)
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

    // If the result is already in the cache it means the key has already been validated.
    (if (cacheValidation)
       validatedPublicKeys.get(publicKey.id, _ => parseRes)
     else parseRes).leftMap(err =>
      errFn(s"Failed to deserialize or validate ${publicKey.format} public key: $err")
    )
  }

  /** Checks if the selected encryption algorithm specification is supported and compatible with the
    * given key specification. If not, attempts to find a supported encryption algorithm that can be
    * used with this key specification. This method is intended for `encrypt` operations where the
    * target public key might use a key specification that is not supported by the node’s default
    * algorithm.
    */
  private[crypto] def selectEncryptionAlgorithmSpec[E](
      keySpec: EncryptionKeySpec,
      algorithmSpec: EncryptionAlgorithmSpec,
      supportedAlgorithmSpecs: Set[EncryptionAlgorithmSpec],
      errFn: () => E,
  ): Either[E, EncryptionAlgorithmSpec] =
    if (
      supportedAlgorithmSpecs.contains(algorithmSpec)
      && algorithmSpec.supportedEncryptionKeySpecs.contains(keySpec)
    )
      Right(algorithmSpec)
    else
      supportedAlgorithmSpecs
        .find(_.supportedEncryptionKeySpecs.contains(keySpec))
        .toRight(errFn())

  /** Checks if the selected signing algorithm specification is supported and compatible with the
    * given key specification. If not, attempts to find a supported signing algorithm that can be
    * used with this key specification. This method is intended for `sign` operations where the
    * target private key might use a key specification that is not supported by the node’s default
    * algorithm.
    */
  private[crypto] def selectSigningAlgorithmSpec[E](
      keySpec: SigningKeySpec,
      algorithmSpec: SigningAlgorithmSpec,
      supportedAlgorithmSpecs: Set[SigningAlgorithmSpec],
      errFn: () => E,
  ): Either[E, SigningAlgorithmSpec] =
    if (
      supportedAlgorithmSpecs.contains(algorithmSpec)
      && algorithmSpec.supportedSigningKeySpecs.contains(keySpec)
    )
      Right(algorithmSpec)
    else
      supportedAlgorithmSpecs
        .find(_.supportedSigningKeySpecs.contains(keySpec))
        .toRight(errFn())

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
