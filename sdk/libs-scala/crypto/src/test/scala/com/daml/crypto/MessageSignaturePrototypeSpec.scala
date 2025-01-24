// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import java.security.{InvalidKeyException, KeyPairGenerator, SignatureException}
import java.security.spec.ECGenParameterSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class MessageSignaturePrototypeSpec extends AnyFlatSpec with Matchers {
  behavior of MessageSignaturePrototype.getClass.getSimpleName

  it should "expose algorithm" in {
    MessageSignaturePrototype.Secp256k1.algorithm shouldBe "SHA256withECDSA"
  }

  it should "be able to sign and verify messages" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"))
    val keyPair = keyPairGen.generateKeyPair()
    val publicKey = keyPair.getPublic
    val privateKey = keyPair.getPrivate
    val message = "Hello World".getBytes(StandardCharsets.UTF_8)
    val signature = MessageSignaturePrototypeUtil.Secp256k1.sign(message, privateKey)

    MessageSignaturePrototype.Secp256k1.verify(signature, message, publicKey) shouldBe true
  }

  it should "fail to verify secp256k1 signatures with invalid public keys" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"))
    val invalidKeyPairGen = KeyPairGenerator.getInstance("RSA")
    invalidKeyPairGen.initialize(1024)
    val privateKey = keyPairGen.generateKeyPair().getPrivate
    val invalidPublicKey = invalidKeyPairGen.generateKeyPair().getPublic
    val message = "Hello World".getBytes(StandardCharsets.UTF_8)
    val signature = MessageSignaturePrototypeUtil.Secp256k1.sign(message, privateKey)

    assertThrows[InvalidKeyException](
      MessageSignaturePrototype.Secp256k1.verify(signature, message, invalidPublicKey)
    )
  }

  it should "fail to verify secp256k1 signatures with invalid secp256k1 public keys" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"))
    val privateKey = keyPairGen.generateKeyPair().getPrivate
    val invalidPublicKey = keyPairGen.generateKeyPair().getPublic
    val message = "Hello World".getBytes(StandardCharsets.UTF_8)
    val signature = MessageSignaturePrototypeUtil.Secp256k1.sign(message, privateKey)

    MessageSignaturePrototype.Secp256k1.verify(signature, message, invalidPublicKey) shouldBe false
  }

  it should "be able to identify invalid messages against secp256k1 signatures" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"))
    val keyPair = keyPairGen.generateKeyPair()
    val publicKey = keyPair.getPublic
    val privateKey = keyPair.getPrivate
    val message = "Hello World".getBytes(StandardCharsets.UTF_8)
    val invalidMessage = "Goodbye World".getBytes(StandardCharsets.UTF_8)
    val signature = MessageSignaturePrototypeUtil.Secp256k1.sign(message, privateKey)

    MessageSignaturePrototype.Secp256k1.verify(signature, invalidMessage, publicKey) shouldBe false
  }

  it should "correctly identify invalid signatures" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"))
    val publicKey = keyPairGen.generateKeyPair().getPublic
    val message = "Hello World".getBytes(StandardCharsets.UTF_8)
    val invalidSignature = "Hello World".getBytes(StandardCharsets.UTF_8)

    assertThrows[SignatureException](
      MessageSignaturePrototype.Secp256k1.verify(invalidSignature, message, publicKey)
    )
  }

  it should "correctly identify invalid secp256k1 signatures" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"))
    val keyPair = keyPairGen.generateKeyPair()
    val publicKey = keyPair.getPublic
    val privateKey = keyPair.getPrivate
    val message = "Hello World".getBytes(StandardCharsets.UTF_8)
    val altMessage = "Goodbye World".getBytes(StandardCharsets.UTF_8)
    val invalidSignature = MessageSignaturePrototypeUtil.Secp256k1.sign(altMessage, privateKey)

    MessageSignaturePrototype.Secp256k1.verify(invalidSignature, message, publicKey) shouldBe false
  }
}
