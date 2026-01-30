// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package support.crypto

import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.interfaces.ECPublicKey
import java.security.spec.ECGenParameterSpec
import java.security.{InvalidKeyException, KeyPairGenerator, Security, SignatureException}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MessageSignatureTest extends AnyFreeSpec with Matchers {

  Security.addProvider(new BouncyCastleProvider())

  "correctly sign and verify secp256k1 signatures" in {
    val keyPair = MessageSignatureUtil.generateKeyPair
    val publicKey = keyPair.getPublic
    val privateKey = keyPair.getPrivate
    val message = Ref.HexString.assertFromString("deadbeef")
    val signature = MessageSignatureUtil.sign(message, privateKey)

    MessageSignature.verify(signature, message, publicKey) shouldBe true
  }

  "invalid public keys fail to verify secp256k1 signatures" in {
    val invalidKeyPairGen = KeyPairGenerator.getInstance("RSA")
    invalidKeyPairGen.initialize(1024)
    val privateKey = MessageSignatureUtil.generateKeyPair.getPrivate
    val invalidPublicKey = invalidKeyPairGen.generateKeyPair().getPublic
    val message = Ref.HexString.assertFromString("deadbeef")
    val signature = MessageSignatureUtil.sign(message, privateKey)

    assertThrows[InvalidKeyException](MessageSignature.verify(signature, message, invalidPublicKey))
  }

  "incorrect secp256k1 public keys fail to verify secp256k1 signatures" in {
    val privateKey = MessageSignatureUtil.generateKeyPair.getPrivate
    val incorrectPublicKey = MessageSignatureUtil.generateKeyPair.getPublic
    val message = Ref.HexString.assertFromString("deadbeef")
    val signature = MessageSignatureUtil.sign(message, privateKey)

    MessageSignature.verify(signature, message, incorrectPublicKey) shouldBe false
  }

  "correctly identify invalid messages against secp256k1 signatures" in {
    val keyPair = MessageSignatureUtil.generateKeyPair
    val publicKey = keyPair.getPublic
    val privateKey = keyPair.getPrivate
    val message = Ref.HexString.assertFromString("deadbeef")
    val invalidMessage = Ref.HexString.assertFromString("deadbeefdeadbeef")
    val signature = MessageSignatureUtil.sign(message, privateKey)

    MessageSignature.verify(signature, invalidMessage, publicKey) shouldBe false
  }

  "correctly identify invalid signatures" in {
    val publicKey = MessageSignatureUtil.generateKeyPair.getPublic
    val message = Ref.HexString.assertFromString("deadbeef")
    val invalidSignature = Ref.HexString.assertFromString("deadbeef")

    assertThrows[SignatureException](MessageSignature.verify(invalidSignature, message, publicKey))
  }

  "correctly identify invalid secp256k1 signatures" in {
    val keyPair = MessageSignatureUtil.generateKeyPair
    val publicKey = keyPair.getPublic
    val privateKey = keyPair.getPrivate
    val message = Ref.HexString.assertFromString("deadbeef")
    val altMessage = Ref.HexString.assertFromString("deadbeefdeadbeef")
    val invalidSignature = MessageSignatureUtil.sign(altMessage, privateKey)

    MessageSignature.verify(invalidSignature, message, publicKey) shouldBe false
  }

  "correctly validate ECDSA keys" in {
    val keyPair = MessageSignatureUtil.generateKeyPair
    val publicKey = keyPair.getPublic

    MessageSignature.validateKey(publicKey) shouldBe true
  }

  "correctly identify non-ECDSA public keys" in {
    val invalidKeyPairGen = KeyPairGenerator.getInstance("RSA")
    invalidKeyPairGen.initialize(1024)
    val invalidPublicKey = invalidKeyPairGen.generateKeyPair().getPublic

    assertThrows[InvalidKeyException](MessageSignature.validateKey(invalidPublicKey))
  }

  "correctly identify off curve ECDSA public keys" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
    // Use a different EC curve to generate a ECDSA public key that is not on the secp256k1 curve
    // - we do this as raw ECPoints are checked as being on the secp256k1 curve during key generation!
    keyPairGen.initialize(new ECGenParameterSpec("secp256r1"))
    val fakePublicKey = keyPairGen.generateKeyPair().getPublic.asInstanceOf[ECPublicKey]

    // Ensure we do not have POINT_INFINITY
    fakePublicKey.getW.getAffineX should not be null
    fakePublicKey.getW.getAffineY should not be null
    MessageSignature.validateKey(fakePublicKey) shouldBe false
  }
}
