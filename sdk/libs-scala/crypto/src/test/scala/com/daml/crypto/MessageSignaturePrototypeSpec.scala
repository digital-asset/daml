// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.BeforeAndAfterAll

import java.security.{KeyPairGenerator, SecureRandom, Security}
import java.security.spec.ECGenParameterSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

class MessageSignaturePrototypeSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  behavior of MessageSignaturePrototype.getClass.getSimpleName

  override def beforeAll(): Unit = {
    val _ = Security.insertProviderAt(new BouncyCastleProvider, 1)
  }

  it should "expose algorithm" in {
    MessageSignaturePrototype.Secp256k1.algorithm shouldBe "SHA256withECDSA"
  }

  it should "be able to sign and verify messages" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"), new SecureRandom())
    val keyPair = keyPairGen.generateKeyPair()
    val publicKey = keyPair.getPublic
    val privateKey = keyPair.getPrivate
    val message = "Hello World".getBytes(StandardCharsets.UTF_8)

    val signature = MessageSignaturePrototype.Secp256k1.sign(message, privateKey)

    MessageSignaturePrototype.Secp256k1.verify(signature, message, publicKey) shouldBe true
  }
}
