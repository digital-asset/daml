// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.Generators
import com.digitalasset.canton.config.CantonRequireTypes.String68
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingIdentityFactoryX}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.*
import org.scalacheck.*

import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}

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

  lazy val cryptoFactory =
    TestingIdentityFactoryX(loggerFactoryNotUsed).forOwnerAndDomain(
      DefaultTestIdentities.sequencerId
    )
  private lazy val sequencerKey =
    TestingIdentityFactoryX(loggerFactoryNotUsed)
      .newSigningPublicKey(DefaultTestIdentities.sequencerId)
      .fingerprint
  private lazy val privateCrypto = cryptoFactory.crypto.privateCrypto
  private lazy val pureCryptoApi: CryptoPureApi = cryptoFactory.pureCrypto

  implicit val signingPublicKeyArb: Arbitrary[SigningPublicKey] = Arbitrary(for {
    id <- Arbitrary.arbitrary[Fingerprint]
    format <- Arbitrary.arbitrary[CryptoKeyFormat]
    key <- Arbitrary.arbitrary[ByteString]
    scheme <- Arbitrary.arbitrary[SigningKeyScheme]
  } yield new SigningPublicKey(id, format, key, scheme))

  implicit val encryptionPublicKeyArb: Arbitrary[EncryptionPublicKey] = Arbitrary(for {
    id <- Arbitrary.arbitrary[Fingerprint]
    format <- Arbitrary.arbitrary[CryptoKeyFormat]
    key <- Arbitrary.arbitrary[ByteString]
    scheme <- Arbitrary.arbitrary[EncryptionKeyScheme]
  } yield new EncryptionPublicKey(id, format, key, scheme))

  // TODO(#14515) Check that the generator is exhaustive
  implicit val publicKeyArb: Arbitrary[PublicKey] = Arbitrary(
    Gen.oneOf(Arbitrary.arbitrary[SigningPublicKey], Arbitrary.arbitrary[EncryptionPublicKey])
  )

  def sign(str: String, purpose: HashPurpose)(implicit
      executionContext: ExecutionContext
  ): Signature = {
    val hash =
      pureCryptoApi.build(purpose).addWithoutLengthPrefix(str).finish()
    Await.result(
      privateCrypto
        .sign(hash, sequencerKey)(TraceContext.empty)
        .valueOr(err => throw new RuntimeException(err.toString)),
      10.seconds,
    )
  }
}
