// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.{BaseTest, Generators}
import com.google.crypto.tink.subtle.{Ed25519Sign, EllipticCurves}
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
import org.scalacheck.*

object GeneratorsCrypto {
  import Generators.*
  import com.digitalasset.canton.data.GeneratorsDataTime.*
  import org.scalatest.EitherValues.*

  implicit val signingKeyUsageArb: Arbitrary[SigningKeyUsage] = genArbitrary
  implicit val signingAlgorithmSpecArb: Arbitrary[SigningAlgorithmSpec] = genArbitrary
  implicit val signingKeySpecArb: Arbitrary[SigningKeySpec] = genArbitrary
  implicit val symmetricKeySchemeArb: Arbitrary[SymmetricKeyScheme] = genArbitrary
  implicit val encryptionAlgorithmSpecArb: Arbitrary[EncryptionAlgorithmSpec] = genArbitrary
  implicit val encryptionKeySpecArb: Arbitrary[EncryptionKeySpec] = genArbitrary
  implicit val hashAlgorithmArb: Arbitrary[HashAlgorithm] = genArbitrary
  implicit val saltAlgorithmArb: Arbitrary[SaltAlgorithm] = genArbitrary
  implicit val cryptoKeyFormatArb: Arbitrary[CryptoKeyFormat] = genArbitrary

  implicit val signingKeySpecsNESArb: Arbitrary[NonEmpty[Set[SigningKeySpec]]] =
    Generators.nonEmptySet[SigningKeySpec]
  implicit val encryptionKeySpecsNESArb: Arbitrary[NonEmpty[Set[EncryptionKeySpec]]] =
    Generators.nonEmptySet[EncryptionKeySpec]
  implicit val symmetricKeySchemeNESArb: Arbitrary[NonEmpty[Set[SymmetricKeyScheme]]] =
    Generators.nonEmptySet[SymmetricKeyScheme]
  implicit val hashAlgorithmNESArb: Arbitrary[NonEmpty[Set[HashAlgorithm]]] =
    Generators.nonEmptySet[HashAlgorithm]
  implicit val cryptoKeyFormatNESArb: Arbitrary[NonEmpty[Set[CryptoKeyFormat]]] =
    Generators.nonEmptySet[CryptoKeyFormat]

  implicit val fingerprintArb: Arbitrary[Fingerprint] = Arbitrary(
    Generators.lengthLimitedStringGen(String68).map(s => Fingerprint.tryCreate(s.str))
  )

  // TODO(#15813): Change arbitrary signing keys to match real keys
  implicit val signingPublicKeyArb: Arbitrary[SigningPublicKey] = Arbitrary(for {
    key <- Arbitrary.arbitrary[ByteString]
    keySpec <- Arbitrary.arbitrary[SigningKeySpec]
    format = CryptoKeyFormat.Symbolic
    usage <- Gen
      .nonEmptyListOf[SigningKeyUsage](Arbitrary.arbitrary[SigningKeyUsage])
      .map(usageAux => NonEmptyUtil.fromUnsafe(usageAux.toSet))
  } yield SigningPublicKey.create(format, key, keySpec, usage).value)

  implicit val signatureDelegationArb: Arbitrary[SignatureDelegation] = Arbitrary(
    for {
      periodFrom <- Arbitrary.arbitrary[CantonTimestamp]
      periodDuration <- Gen.choose(1, 86400L).map(PositiveSeconds.tryOfSeconds)
      period = SignatureDelegationValidityPeriod(periodFrom, periodDuration)

      signingKeySpec <- Arbitrary
        .arbitrary[SigningKeySpec]
        // TODO(#22209): Remove this when DerX509Spki becomes the standard for all key specifications, particularly for EcP256 and EcP384.
        .retryUntil(_ == SigningKeySpec.EcCurve25519)

      /** The session signing keys inside the signature delegation are a special type of signing key where
        * the format is fixed (i.e. DerX509Spki) and their scheme is identified by the 'sessionKeySpec' protobuf field.
        * Therefore, we cannot use the usual Arbitrary.arbitrary[SigningKey] because it produces keys in a Symbolic format.
        */
      encodedKey = ByteString.copyFrom(signingKeySpec match {
        case SigningKeySpec.EcCurve25519 =>
          new SubjectPublicKeyInfo(
            new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519),
            Ed25519Sign.KeyPair.newKeyPair().getPublicKey,
          ).getEncoded
        case SigningKeySpec.EcP256 =>
          EllipticCurves.generateKeyPair(EllipticCurves.CurveType.NIST_P256).getPublic.getEncoded
        case SigningKeySpec.EcP384 =>
          EllipticCurves.generateKeyPair(EllipticCurves.CurveType.NIST_P384).getPublic.getEncoded
      })

      sessionKey = SigningPublicKey
        .create(
          CryptoKeyFormat.DerX509Spki,
          encodedKey,
          signingKeySpec,
          SigningKeyUsage.ProtocolOnly,
        )
        .value
      format <- Arbitrary.arbitrary[SignatureFormat]
      signature <- Arbitrary.arbitrary[ByteString]
      algorithmO <- Arbitrary.arbitrary[Option[SigningAlgorithmSpec]]
    } yield SignatureDelegation
      .create(
        sessionKey,
        period,
        new Signature(
          format = format,
          signature = signature,
          signedBy = sessionKey.fingerprint,
          signingAlgorithmSpec = algorithmO,
          signatureDelegation = None,
        ),
      )
      .value
  )

  implicit val signatureArb: Arbitrary[Signature] = genArbitrary

  implicit val hashArb: Arbitrary[Hash] = Arbitrary(
    for {
      hashAlgorithm <- hashAlgorithmArb.arbitrary
      hash <- Gen
        .stringOfN(hashAlgorithm.length.toInt, Gen.alphaNumChar)
        .map(ByteString.copyFromUtf8)
    } yield Hash.tryCreate(hash, hashAlgorithm)
  )

  implicit val saltArb: Arbitrary[Salt] = Arbitrary(
    for {
      saltAlgorithm <- saltAlgorithmArb.arbitrary
      salt <- Gen
        .stringOfN(saltAlgorithm.length.toInt, Gen.alphaNumChar)
        .map(ByteString.copyFromUtf8)
    } yield Salt.create(salt, saltAlgorithm).value
  )

  private lazy val loggerFactoryNotUsed =
    NamedLoggerFactory.unnamedKey("test", "NotUsed-GeneratorsCrypto")

  private lazy val crypto = SymbolicCrypto.create(
    BaseTest.testedReleaseProtocolVersion,
    DefaultProcessingTimeouts.testing,
    loggerFactoryNotUsed,
  )

  private lazy val sequencerKey = crypto.generateSymbolicSigningKey()

  // TODO(#15813): Change arbitrary encryption keys to match real keys
  implicit val encryptionPublicKeyArb: Arbitrary[EncryptionPublicKey] = Arbitrary(for {
    key <- Arbitrary.arbitrary[ByteString]
    keySpec <- Arbitrary.arbitrary[EncryptionKeySpec]
    format = CryptoKeyFormat.Symbolic
  } yield new EncryptionPublicKey(format, key, keySpec)())

  // TODO(#14515) Check that the generator is exhaustive
  implicit val publicKeyArb: Arbitrary[PublicKey] = Arbitrary(
    Gen.oneOf(Arbitrary.arbitrary[SigningPublicKey], Arbitrary.arbitrary[EncryptionPublicKey])
  )

  def sign(str: String, purpose: HashPurpose): Signature = {
    val hash = crypto.pureCrypto.build(purpose).addWithoutLengthPrefix(str).finish()
    crypto.sign(hash, sequencerKey.id)
  }

  implicit val symmetricKeyArb: Arbitrary[SymmetricKey] =
    Arbitrary(
      for {
        format <- Arbitrary.arbitrary[CryptoKeyFormat]
        key <- Arbitrary.arbitrary[ByteString]
        scheme <- Arbitrary.arbitrary[SymmetricKeyScheme]
      } yield new SymmetricKey(format, key, scheme)
    )

  implicit def asymmetricEncryptedArb[T]: Arbitrary[AsymmetricEncrypted[T]] =
    Arbitrary(
      for {
        ciphertext <- Arbitrary.arbitrary[ByteString]
        encryptionAlgorithmSpec <- Arbitrary.arbitrary[EncryptionAlgorithmSpec]
        fingerprint <- Gen
          .stringOfN(68, Gen.alphaChar)
          .map(str => Fingerprint.tryCreate(String68.tryCreate(str)))
      } yield AsymmetricEncrypted.apply(ciphertext, encryptionAlgorithmSpec, fingerprint)
    )
}
