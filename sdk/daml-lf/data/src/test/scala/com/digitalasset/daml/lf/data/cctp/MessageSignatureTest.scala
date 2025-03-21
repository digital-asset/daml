// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package cctp

import com.daml.lf.data.cctp.MessageSignatureUtil

import java.security.{InvalidKeyException, KeyPairGenerator, SignatureException}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MessageSignatureTest extends AnyFreeSpec with Matchers {

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
}
