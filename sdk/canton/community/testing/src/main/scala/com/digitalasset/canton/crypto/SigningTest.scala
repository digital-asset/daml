// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.SignatureCheckError.{InvalidSignature, SignatureWithWrongKey}
import com.digitalasset.canton.crypto.SigningError.UnknownSigningKey
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait SigningTest extends AsyncWordSpec with BaseTest with CryptoTestHelper {

  def signingProvider(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      newCrypto: => FutureUnlessShutdown[Crypto],
  ): Unit =
    forAll(supportedSigningKeySchemes) { signingKeyScheme =>
      s"Sign with $signingKeyScheme" should {

        "serialize and deserialize a signing public key via protobuf" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(
              crypto,
              SigningKeyUsage.ProtocolOnly,
              signingKeyScheme,
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
              signingKeyScheme,
            )
            hash = TestHash.digest("foobar")
            sig <- crypto.privateCrypto.sign(hash, publicKey.id).valueOrFail("sign")
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
              signingKeyScheme,
            )
            hash = TestHash.digest("foobar")
            sig <- crypto.privateCrypto.sign(hash, publicKey.id).valueOrFail("sign")
            res = crypto.pureCrypto.verifySignature(hash, publicKey, sig)
          } yield res shouldEqual Right(())
        }.failOnShutdown

        "fail to sign with unknown private key" in {
          for {
            crypto <- newCrypto
            unknownKeyId = Fingerprint.create(ByteString.copyFromUtf8("foobar"))
            hash = TestHash.digest("foobar")
            sig <- crypto.privateCrypto.sign(hash, unknownKeyId).value
          } yield sig.left.value shouldBe a[UnknownSigningKey]
        }.failOnShutdown

        "fail to verify if signature is invalid" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(
              crypto,
              SigningKeyUsage.ProtocolOnly,
              signingKeyScheme,
            )
            hash = TestHash.digest("foobar")
            realSig <- crypto.privateCrypto
              .sign(hash, publicKey.id)
              .valueOrFail("sign")
            randomBytes = ByteString.copyFromUtf8(PseudoRandom.randomAlphaNumericString(16))
            fakeSig = new Signature(realSig.format, randomBytes, realSig.signedBy)
            res = crypto.pureCrypto.verifySignature(hash, publicKey, fakeSig)
          } yield res.left.value shouldBe a[InvalidSignature]
        }.failOnShutdown

        "fail to verify with a different public key" in {
          for {
            crypto <- newCrypto
            publicKeys <- getTwoSigningPublicKeys(
              crypto,
              SigningKeyUsage.ProtocolOnly,
              signingKeyScheme,
            )
            (publicKey, publicKey2) = publicKeys
            _ = assert(publicKey != publicKey2)
            hash = TestHash.digest("foobar")
            sig <- crypto.privateCrypto.sign(hash, publicKey.id).valueOrFail("sign")
            res = crypto.pureCrypto.verifySignature(hash, publicKey2, sig)
          } yield res.left.value shouldBe a[SignatureWithWrongKey]
        }.failOnShutdown

      }
    }

}
