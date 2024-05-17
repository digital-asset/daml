// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.{BaseTest, Generators}
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.*

object GeneratorsCrypto {
  import Generators.*
  import org.scalatest.EitherValues.*

  implicit val signingKeySchemeArb: Arbitrary[SigningKeyScheme] = genArbitrary
  implicit val symmetricKeySchemeArb: Arbitrary[SymmetricKeyScheme] = genArbitrary
  implicit val encryptionKeySchemeArb: Arbitrary[EncryptionKeyScheme] = genArbitrary
  implicit val hashAlgorithmArb: Arbitrary[HashAlgorithm] = genArbitrary
  implicit val saltAlgorithmArb: Arbitrary[SaltAlgorithm] = genArbitrary
  implicit val cryptoKeyFormatArb: Arbitrary[CryptoKeyFormat] = genArbitrary

  implicit val signingKeySchemeNESArb: Arbitrary[NonEmpty[Set[SigningKeyScheme]]] =
    Generators.nonEmptySet[SigningKeyScheme]
  implicit val encryptionKeySchemeNESArb: Arbitrary[NonEmpty[Set[EncryptionKeyScheme]]] =
    Generators.nonEmptySet[EncryptionKeyScheme]
  implicit val symmetricKeySchemeNESArb: Arbitrary[NonEmpty[Set[SymmetricKeyScheme]]] =
    Generators.nonEmptySet[SymmetricKeyScheme]
  implicit val hashAlgorithmNESArb: Arbitrary[NonEmpty[Set[HashAlgorithm]]] =
    Generators.nonEmptySet[HashAlgorithm]
  implicit val cryptoKeyFormatNESArb: Arbitrary[NonEmpty[Set[CryptoKeyFormat]]] =
    Generators.nonEmptySet[CryptoKeyFormat]

  implicit val fingerprintArb: Arbitrary[Fingerprint] = Arbitrary(
    Generators.lengthLimitedStringGen(String68).map(s => Fingerprint.tryCreate(s.str))
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

  // TODO(#15813): Change arbitrary signing keys to match real keys
  implicit val signingPublicKeyArb: Arbitrary[SigningPublicKey] = Arbitrary(for {
    key <- Arbitrary.arbitrary[ByteString]
    scheme <- Arbitrary.arbitrary[SigningKeyScheme]
    format = CryptoKeyFormat.Symbolic
  } yield new SigningPublicKey(format, key, scheme))

  // TODO(#15813): Change arbitrary encryption keys to match real keys
  implicit val encryptionPublicKeyArb: Arbitrary[EncryptionPublicKey] = Arbitrary(for {
    key <- Arbitrary.arbitrary[ByteString]
    scheme <- Arbitrary.arbitrary[EncryptionKeyScheme]
    format = CryptoKeyFormat.Symbolic
  } yield new EncryptionPublicKey(format, key, scheme))

  // TODO(#14515) Check that the generator is exhaustive
  implicit val publicKeyArb: Arbitrary[PublicKey] = Arbitrary(
    Gen.oneOf(Arbitrary.arbitrary[SigningPublicKey], Arbitrary.arbitrary[EncryptionPublicKey])
  )

  def sign(str: String, purpose: HashPurpose): Signature = {
    val hash = crypto.pureCrypto.build(purpose).addWithoutLengthPrefix(str).finish()
    crypto.sign(hash, sequencerKey.id)
  }
}
