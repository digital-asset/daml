// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.provider.jce.JcePrivateCrypto
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.{BaseTest, Generators}
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.*

import scala.annotation.nowarn

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
  @nowarn("msg=Der in object CryptoKeyFormat is deprecated")
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
    Generators.lengthLimitedStringGen(String68).map(s => Fingerprint.tryFromString(s.str))
  )

  val validUsageGen: Gen[Set[SigningKeyUsage]] = for {
    usages <- Gen.someOf(SigningKeyUsage.All)
    if SigningKeyUsage.isUsageValid(NonEmptyUtil.fromUnsafe(usages.toSet))
  } yield usages.toSet

  // TODO(#15813): Change arbitrary signing keys to match real keys
  implicit val signingPublicKeyArb: Arbitrary[SigningPublicKey] = Arbitrary(for {
    key <- Arbitrary.arbitrary[ByteString]
    keySpec <- Arbitrary.arbitrary[SigningKeySpec]
    format = CryptoKeyFormat.Symbolic
    usage <- Gen
      .nonEmptyListOf(Gen.oneOf(SigningKeyUsage.All.toList))
      .map(usages => NonEmptyUtil.fromUnsafe(usages.toSet))
      .suchThat(usagesNE => SigningKeyUsage.isUsageValid(usagesNE))
  } yield SigningPublicKey.create(format, key, keySpec, usage).value)

  @nowarn("msg=Raw in object SignatureFormat is deprecated")
  implicit val signatureFormatArb: Arbitrary[SignatureFormat] = genArbitrary

  implicit val signatureDelegationArb: Arbitrary[SignatureDelegation] = Arbitrary(
    for {
      periodFrom <- Arbitrary.arbitrary[CantonTimestamp]
      periodDuration <- Gen.choose(1, 86400L).map(PositiveSeconds.tryOfSeconds)
      period = SignatureDelegationValidityPeriod(periodFrom, periodDuration)

      signingKeySpec <- Arbitrary
        .arbitrary[SigningKeySpec]

      /** The session signing keys inside the signature delegation are a special type of signing key
        * where the format is fixed (i.e. DerX509Spki) and their scheme is identified by the
        * 'sessionKeySpec' protobuf field. Therefore, we cannot use the usual
        * Arbitrary.arbitrary[SigningKey] because it produces keys in a Symbolic format.
        */
      sessionKey = JcePrivateCrypto
        .generateSigningKeypair(
          signingKeySpec,
          SigningKeyUsage.ProtocolOnly,
        )
        .value

      format <- Arbitrary.arbitrary[SignatureFormat]
      signature <- Arbitrary.arbitrary[ByteString]
      algorithmO <- Arbitrary.arbitrary[Option[SigningAlgorithmSpec]]

    } yield SignatureDelegation
      .create(
        sessionKey.publicKey,
        period,
        Signature.create(
          format = format,
          signature = signature,
          signedBy = sessionKey.id,
          signingAlgorithmSpec = algorithmO,
          signatureDelegation = None,
        ),
      )
      .value
  )

  // Needed to ensure we go via `create()`, which migrates `Raw`
  implicit val signatureArb: Arbitrary[Signature] = Arbitrary(
    for {
      signature <- Arbitrary.arbitrary[ByteString]
      longTermKey <- Arbitrary.arbitrary[Fingerprint]
      signingAlgorithmSpec <- Arbitrary.arbitrary[Option[SigningAlgorithmSpec]]
      format <- Arbitrary.arbitrary[SignatureFormat]
      signatureDelegation <- Arbitrary.arbitrary[Option[SignatureDelegation]]
      signedBy = signatureDelegation.map(_.sessionKey.id).getOrElse(longTermKey)
    } yield Signature.create(format, signature, signedBy, signingAlgorithmSpec, signatureDelegation)
  )

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

  // Test key intended for signing an unassignment result message.
  lazy val testSigningKey: SigningPublicKey =
    crypto.generateSymbolicSigningKey(usage = SigningKeyUsage.ProtocolOnly)

  def sign(
      signingKeyId: Fingerprint,
      strToSign: String,
      purpose: HashPurpose,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Signature = {
    val hash = crypto.pureCrypto.build(purpose).addWithoutLengthPrefix(strToSign).finish()
    crypto.sign(hash, signingKeyId, usage)
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
          .map(str => Fingerprint.tryFromString(String68.tryCreate(str)))
      } yield AsymmetricEncrypted.apply(ciphertext, encryptionAlgorithmSpec, fingerprint)
    )
}
