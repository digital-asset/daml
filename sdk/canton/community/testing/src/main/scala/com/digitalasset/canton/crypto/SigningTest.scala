// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*
import com.digitalasset.canton.crypto.SignatureCheckError.{
  InvalidSignature,
  InvalidSignatureFormat,
  KeyAlgoSpecsMismatch,
  SignatureWithWrongKey,
}
import com.digitalasset.canton.crypto.SigningError.UnknownSigningKey
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.{BaseTest, FailOnShutdown}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.nowarn

trait SigningTest extends AsyncWordSpec with BaseTest with CryptoTestHelper with FailOnShutdown {

  def signingProvider(
      supportedSigningAlgorithmSpecs: Set[SigningAlgorithmSpec],
      newCrypto: => FutureUnlessShutdown[Crypto],
  ): Unit = {
    // Get all the supported signature formats
    val supportedSignatureFormats =
      supportedSigningAlgorithmSpecs.foldLeft(Seq.empty[SignatureFormat])(
        _ ++ _.supportedSignatureFormats
      )

    forAll(supportedSigningAlgorithmSpecs) { signingAlgorithmSpec =>
      forAll(signingAlgorithmSpec.supportedSigningKeySpecs.forgetNE) { signingKeySpec =>
        s"Sign with $signingKeySpec key and $signingAlgorithmSpec algorithm" should {

          "serialize and deserialize a signing public key via protobuf" in {
            for {
              crypto <- newCrypto
              publicKey <- getSigningPublicKey(
                crypto,
                // proof of ownership is added internally
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
                .sign(hash, publicKey.id, SigningKeyUsage.ProtocolOnly, signingAlgorithmSpec)
                .valueOrFail("sign")
              sigP = sig.toProtoVersioned(testedProtocolVersion)
              sig2 = Signature.fromProtoVersioned(sigP).valueOrFail("serialize signature")
            } yield sig shouldEqual sig2
          }

          "perform migration during deserialization" in {
            def checkMigration(signature: Signature) = {
              val legacySignature = signature.reverseMigrate()
              legacySignature.format should (be(
                SignatureFormat.Raw: @nowarn(
                  "msg=Raw in object SignatureFormat is deprecated"
                )
              ) or be(SignatureFormat.Symbolic))

              val migratedSignature = Signature
                .fromProtoVersioned(legacySignature.toProtoVersioned(testedProtocolVersion))
                .valueOrFail("deserialize signature")

              migratedSignature shouldEqual signature
            }

            for {
              crypto <- newCrypto
              publicKey <- getSigningPublicKey(
                crypto,
                SigningKeyUsage.ProtocolOnly,
                signingKeySpec,
              )
              hash = TestHash.digest("foobar")

              signature <- crypto.privateCrypto
                .sign(hash, publicKey.id, SigningKeyUsage.ProtocolOnly, signingAlgorithmSpec)
                .valueOrFail("sign")
              signatureWithoutSigningAlgo = Signature.create(
                format = signature.format,
                signature = signature.unwrap,
                signedBy = signature.signedBy,
                signingAlgorithmSpec = None,
              )
            } yield {
              checkMigration(signature)
              // Check also the format "guess" when there is no signing algo spec
              checkMigration(signatureWithoutSigningAlgo)
            }
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
                .sign(hash, publicKey.id, SigningKeyUsage.ProtocolOnly, signingAlgorithmSpec)
                .valueOrFail("sign")
              res = crypto.pureCrypto.verifySignature(
                hash,
                publicKey,
                sig,
                SigningKeyUsage.ProtocolOnly,
              )
            } yield res shouldEqual Either.unit
          }

          "fail to sign with unknown private key" in {
            for {
              crypto <- newCrypto
              unknownKeyId = Fingerprint.create(ByteString.copyFromUtf8("foobar"))
              hash = TestHash.digest("foobar")
              sig <- crypto.privateCrypto
                .sign(hash, unknownKeyId, SigningKeyUsage.ProtocolOnly, signingAlgorithmSpec)
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
                .sign(hash, publicKey.id, SigningKeyUsage.ProtocolOnly, signingAlgorithmSpec)
                .valueOrFail("sign")
              randomBytes = ByteString.copyFromUtf8(PseudoRandom.randomAlphaNumericString(16))
              fakeSig = Signature.create(
                realSig.format,
                randomBytes,
                realSig.signedBy,
                Some(signingAlgorithmSpec),
              )
              _ = supportedSigningAlgorithmSpecs
                .find(_ != signingAlgorithmSpec)
                .foreach { otherSigningAlgorithmSpec =>
                  val wrongSpecSig = Signature.create(
                    otherSigningAlgorithmSpec.supportedSignatureFormats.head1,
                    realSig.unwrap,
                    realSig.signedBy,
                    Some(otherSigningAlgorithmSpec),
                  )
                  crypto.pureCrypto
                    .verifySignature(hash, publicKey, wrongSpecSig, SigningKeyUsage.ProtocolOnly)
                    .left
                    .value shouldBe a[KeyAlgoSpecsMismatch]

                  // Check that the signature format is validated when verifying a signature
                  supportedSignatureFormats
                    .find(_ != realSig.format)
                    .foreach { otherSignatureFormat =>
                      val wrongFormatSig = Signature.create(
                        otherSignatureFormat,
                        realSig.unwrap,
                        realSig.signedBy,
                        realSig.signingAlgorithmSpec,
                      )
                      crypto.pureCrypto
                        .verifySignature(
                          hash,
                          publicKey,
                          wrongFormatSig,
                          SigningKeyUsage.ProtocolOnly,
                        )
                        .left
                        .value shouldBe a[InvalidSignatureFormat]
                    }
                }
              res = crypto.pureCrypto.verifySignature(
                hash,
                publicKey,
                fakeSig,
                SigningKeyUsage.ProtocolOnly,
              )
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
                .sign(hash, publicKey.id, SigningKeyUsage.ProtocolOnly, signingAlgorithmSpec)
                .valueOrFail("sign")
              noSpecSig = Signature.create(
                realSig.format,
                realSig.unwrap,
                realSig.signedBy,
                None, // for backwards compatibility, the algorithm specification will be derived from the key's supported algorithms if it is not explicitly set.
              )
              res = crypto.pureCrypto.verifySignature(
                hash,
                publicKey,
                noSpecSig,
                SigningKeyUsage.ProtocolOnly,
              )
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
                .sign(hash, publicKey.id, SigningKeyUsage.ProtocolOnly, signingAlgorithmSpec)
                .valueOrFail("sign")
              verifyErr = crypto.pureCrypto.verifySignature(
                hash,
                publicKey2,
                sig,
                SigningKeyUsage.ProtocolOnly,
              )
            } yield verifyErr.left.value shouldBe a[SignatureWithWrongKey]
          }.failOnShutdown

          "fail to sign and verify if the public key does not have the correct usage" in {
            for {
              crypto <- newCrypto
              publicKey <- getSigningPublicKey(
                crypto,
                SigningKeyUsage.ProtocolOnly,
                signingKeySpec,
              )
              hash = TestHash.digest("foobar")
              sig <- crypto.privateCrypto
                .sign(hash, publicKey.id, SigningKeyUsage.ProtocolOnly, signingAlgorithmSpec)
                .valueOrFail("sign")
              sigErr <- crypto.privateCrypto
                .sign(
                  hash,
                  publicKey.id,
                  SigningKeyUsage.NamespaceOrIdentityDelegation,
                  signingAlgorithmSpec,
                )
                .value
              _ <- crypto.privateCrypto
                .sign(hash, publicKey.id, SigningKeyUsage.All, signingAlgorithmSpec)
                .valueOrFail("sign")
              res = crypto.pureCrypto.verifySignature(
                hash,
                publicKey,
                sig,
                SigningKeyUsage.IdentityDelegationOnly,
              )
              _ = crypto.pureCrypto
                .verifySignature(
                  hash,
                  publicKey,
                  sig,
                  SigningKeyUsage.All,
                )
                .valueOrFail("verify")
            } yield {
              sigErr.left.value shouldBe a[SigningError.InvalidKeyUsage]
              res.left.value shouldBe a[SignatureCheckError.InvalidKeyUsage]
            }
          }.failOnShutdown

        }
      }
    }

  }

}
