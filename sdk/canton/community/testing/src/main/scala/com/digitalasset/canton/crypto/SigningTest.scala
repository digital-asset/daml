// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.crypto.SignatureCheckError.{
  InvalidSignature,
  KeyAlgoSpecsMismatch,
  SignatureWithWrongKey,
}
import com.digitalasset.canton.crypto.SigningError.UnknownSigningKey
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait SigningTest extends AsyncWordSpec with BaseTest with CryptoTestHelper with FailOnShutdown {

  def migrationTest(newCrypto: => FutureUnlessShutdown[Crypto]): Unit =
    "when deserializing keys" should {
      "migrate legacy keys" in {
        for {
          crypto <- newCrypto

          publicKey <- getSigningPublicKey(crypto, SigningKeyUsage.All, SigningKeySpec.EcCurve25519)
          privateStore = crypto.cryptoPrivateStore.toExtended.valueOrFail("extend store")
          privateKeyO <- privateStore.signingKey(publicKey.id).valueOrFail("read private key")
          privateKey = privateKeyO.valueOrFail("find private key")
        } yield {
          val (legacyPublicKey, legacyPrivateKey) = makeLegacyEd25519Keys(publicKey, privateKey)
          legacyPublicKey.format shouldBe CryptoKeyFormat.Raw
          legacyPrivateKey.format shouldBe CryptoKeyFormat.Raw

          val newPublicKey = SigningPublicKey
            .fromProtoV30(legacyPublicKey.toProtoV30)
            .valueOrFail("deserialize public")

          newPublicKey.format shouldBe CryptoKeyFormat.DerX509Spki
          newPublicKey.migrated shouldBe true
          newPublicKey shouldBe publicKey

          val newPrivateKey = SigningPrivateKey
            .fromProtoV30(legacyPrivateKey.toProtoV30)
            .valueOrFail("deserialize private")

          newPrivateKey.format shouldBe CryptoKeyFormat.DerPkcs8Pki
          newPrivateKey.migrated shouldBe true
          newPrivateKey shouldBe privateKey
        }
      }.failOnShutdown

      def makeLegacyEd25519Keys(
          publicKey: SigningPublicKey,
          privateKey: SigningPrivateKey,
      ): (SigningPublicKey, SigningPrivateKey) = {
        val legacyPublicKey = publicKey.reverseMigrate().valueOrFail("public key reverse migration")
        legacyPublicKey.id shouldBe publicKey.id

        val legacyPrivateKey =
          privateKey.reverseMigrate().valueOrFail("private key reverse migration")

        (legacyPublicKey, legacyPrivateKey)
      }
    }

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
          }

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
          }

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
          }

          "fail to sign with unknown private key" in {
            for {
              crypto <- newCrypto
              unknownKeyId = Fingerprint.create(ByteString.copyFromUtf8("foobar"))
              hash = TestHash.digest("foobar")
              sig <- crypto.privateCrypto
                .sign(hash, unknownKeyId, signingAlgorithmSpec)
                .value
            } yield sig.left.value shouldBe a[UnknownSigningKey]
          }

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
          }

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
          }

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
          }

        }
      }
    }

  }

}
