// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.SignatureCheckError.{InvalidSignature, SignatureWithWrongKey}
import com.digitalasset.canton.crypto.SigningError.UnknownSigningKey
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.Future

trait SigningTest extends BaseTest with CryptoTestHelper {
  this: AsyncWordSpecLike =>

  def signingProvider(
      supportedSigningKeySchemes: Set[SigningKeyScheme],
      newCrypto: => Future[Crypto],
  ): Unit = {

    forAll(supportedSigningKeySchemes) { signingKeyScheme =>
      s"Sign with $signingKeyScheme" should {

        "serialize and deserialize a signing public key via protobuf" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(crypto, signingKeyScheme)
            publicKeyP = publicKey.toProtoVersioned(testedProtocolVersion)
            publicKey2 = SigningPublicKey
              .fromProtoVersioned(publicKeyP)
              .valueOrFail("serialize key")
          } yield publicKey shouldEqual publicKey2
        }

        "serialize and deserialize a signature via protobuf" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(crypto, signingKeyScheme)
            hash = TestHash.digest("foobar")
            sig <- crypto.privateCrypto.sign(hash, publicKey.id).valueOrFail("sign")
            sigP = sig.toProtoVersioned(testedProtocolVersion)
            sig2 = Signature.fromProtoVersioned(sigP).valueOrFail("serialize signature")
          } yield sig shouldEqual sig2
        }

        "sign and verify" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(crypto, signingKeyScheme)
            hash = TestHash.digest("foobar")
            sig <- crypto.privateCrypto.sign(hash, publicKey.id).valueOrFail("sign")
            res = crypto.pureCrypto.verifySignature(hash, publicKey, sig)
          } yield res shouldEqual Right(())
        }

        "fail to sign with unknown private key" in {
          for {
            crypto <- newCrypto
            unknownKeyId = Fingerprint.create(ByteString.copyFromUtf8("foobar"))
            hash = TestHash.digest("foobar")
            sig <- crypto.privateCrypto.sign(hash, unknownKeyId).value
          } yield sig.left.value shouldBe a[UnknownSigningKey]
        }

        "fail to verify if signature is invalid" in {
          for {
            crypto <- newCrypto
            publicKey <- getSigningPublicKey(crypto, signingKeyScheme)
            hash = TestHash.digest("foobar")
            realSig <- crypto.privateCrypto.sign(hash, publicKey.id).valueOrFail("sign")
            randomBytes = ByteString.copyFromUtf8(PseudoRandom.randomAlphaNumericString(16))
            fakeSig = new Signature(realSig.format, randomBytes, realSig.signedBy)
            res = crypto.pureCrypto.verifySignature(hash, publicKey, fakeSig)
          } yield res.left.value shouldBe a[InvalidSignature]
        }

        "fail to verify with a different public key" in {
          for {
            crypto <- newCrypto
            (publicKey, publicKey2) <- getTwoSigningPublicKeys(crypto, signingKeyScheme)
            _ = assert(publicKey != publicKey2)
            hash = TestHash.digest("foobar")
            sig <- crypto.privateCrypto.sign(hash, publicKey.id).valueOrFail("sign")
            res = crypto.pureCrypto.verifySignature(hash, publicKey2, sig)
          } yield res.left.value shouldBe a[SignatureWithWrongKey]
        }

      }
    }

  }

}
