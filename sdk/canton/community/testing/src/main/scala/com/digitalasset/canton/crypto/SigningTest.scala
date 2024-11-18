// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.SignatureCheckError.{
  InvalidSignature,
  KeyAlgoSpecsMismatch,
  SignatureWithWrongKey,
}
import com.digitalasset.canton.crypto.SigningError.UnknownSigningKey
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait SigningTest extends AsyncWordSpec with BaseTest with CryptoTestHelper {

  def signingProvider(
      supportedSigningAlgorithmSpecs: Set[SigningAlgorithmSpec],
      newCrypto: => FutureUnlessShutdown[Crypto],
  ): Unit = {
    forAll(supportedSigningAlgorithmSpecs) { signingAlgorithmSpec =>
      forAll(signingAlgorithmSpec.supportedSigningKeySpecs.forgetNE) { signingKeySpec =>
        s"Sign with $signingKeySpec key and $signingAlgorithmSpec algorithm" should {

          "serialize and deserialize a signing public key via protobuf" in {
            for {
              crypto <- newCrypto
              publicKey <- getSigningPublicKey(
                crypto,
                SigningKeyUsage.ProtocolOnly,
                signingKeySpec,
              )
              publicKeyP = publicKey.toProtoVersioned(testedProtocolVersion)
              publicKey2 = SigningPublicKey
                .fromProtoVersioned(publicKeyP)
                .valueOrFail("serialize key")
            } yield publicKey shouldEqual publicKey2
          }.failOnShutdown

          "serialize and deserialize a signature via protobuf" in {
            for {
              crypto <- newCrypto
              publicKey <- getSigningPublicKey(
                crypto,
                SigningKeyUsage.ProtocolOnly,
                signingKeySpec,
              )
              hash = TestHash.digest("foobar")
              sig <- crypto.privateCrypto
                .sign(hash, publicKey.id, signingAlgorithmSpec)
                .valueOrFail("sign")
              sigP = sig.toProtoVersioned(testedProtocolVersion)
              sig2 = Signature.fromProtoVersioned(sigP).valueOrFail("serialize signature")
            } yield sig shouldEqual sig2
          }.failOnShutdown

          "sign and verify" in {
            for {
              crypto <- newCrypto
              publicKey <- getSigningPublicKey(
                crypto,
                SigningKeyUsage.ProtocolOnly,
                signingKeySpec,
              )
              hash = TestHash.digest("foobar")
              sig <- crypto.privateCrypto
                .sign(hash, publicKey.id, signingAlgorithmSpec)
                .valueOrFail("sign")
              res = crypto.pureCrypto.verifySignature(hash, publicKey, sig)
            } yield res shouldEqual Either.unit
          }.failOnShutdown

          "fail to sign with unknown private key" in {
            for {
              crypto <- newCrypto
              unknownKeyId = Fingerprint.create(ByteString.copyFromUtf8("foobar"))
              hash = TestHash.digest("foobar")
              sig <- crypto.privateCrypto
                .sign(hash, unknownKeyId, signingAlgorithmSpec)
                .value
            } yield sig.left.value shouldBe a[UnknownSigningKey]
          }.failOnShutdown

          "fail to verify if signature is invalid" in {
            for {
              crypto <- newCrypto
              publicKey <- getSigningPublicKey(
                crypto,
                SigningKeyUsage.ProtocolOnly,
                signingKeySpec,
              )
              hash = TestHash.digest("foobar")
              realSig <- crypto.privateCrypto
                .sign(hash, publicKey.id, signingAlgorithmSpec)
                .valueOrFail("sign")
              randomBytes = ByteString.copyFromUtf8(PseudoRandom.randomAlphaNumericString(16))
              fakeSig = new Signature(
                realSig.format,
                randomBytes,
                realSig.signedBy,
                Some(signingAlgorithmSpec),
              )
              _ = supportedSigningAlgorithmSpecs
                .find(_ != signingAlgorithmSpec)
                .foreach { otherSigningAlgorithmSpec =>
                  val wrongSpecSig = new Signature(
                    realSig.format,
                    realSig.unwrap,
                    realSig.signedBy,
                    Some(otherSigningAlgorithmSpec),
                  )
                  crypto.pureCrypto
                    .verifySignature(hash, publicKey, wrongSpecSig)
                    .left
                    .value shouldBe a[KeyAlgoSpecsMismatch]
                }
              res = crypto.pureCrypto.verifySignature(hash, publicKey, fakeSig)
            } yield res.left.value shouldBe a[InvalidSignature]
          }.failOnShutdown

          "correctly verify signature if the signing algorithm specification is not present" in {
            for {
              crypto <- newCrypto
              publicKey <- getSigningPublicKey(
                crypto,
                SigningKeyUsage.ProtocolOnly,
                signingKeySpec,
              )
              hash = TestHash.digest("foobar")
              realSig <- crypto.privateCrypto
                .sign(hash, publicKey.id, signingAlgorithmSpec)
                .valueOrFail("sign")
              noSpecSig = new Signature(
                realSig.format,
                realSig.unwrap,
                realSig.signedBy,
                None, // for backwards compatibility, the algorithm specification will be derived from the key's supported algorithms if it is not explicitly set.
              )
              res = crypto.pureCrypto.verifySignature(hash, publicKey, noSpecSig)
            } yield res shouldEqual Either.unit
          }.failOnShutdown

          "fail to verify with a different public key" in {
            for {
              crypto <- newCrypto
              publicKeys <- getTwoSigningPublicKeys(
                crypto,
                SigningKeyUsage.ProtocolOnly,
                signingKeySpec,
              )
              (publicKey, publicKey2) = publicKeys
              _ = assert(publicKey != publicKey2)
              hash = TestHash.digest("foobar")
              sig <- crypto.privateCrypto
                .sign(hash, publicKey.id, signingAlgorithmSpec)
                .valueOrFail("sign")
              res = crypto.pureCrypto.verifySignature(hash, publicKey2, sig)
            } yield res.left.value shouldBe a[SignatureWithWrongKey]
          }.failOnShutdown

        }
      }
    }

  }

}
