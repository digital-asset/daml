// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package cctp

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.BeforeAndAfterAll

import java.security.{KeyPairGenerator, SecureRandom, Security}
import java.security.spec.ECGenParameterSpec
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class MessageSignatureTest extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    val _ = Security.insertProviderAt(new BouncyCastleProvider, 1)
  }

  "correctly sign and verify secp256k1 signatures" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"), new SecureRandom())
    val keyPair = keyPairGen.generateKeyPair()
    val publicKey = keyPair.getPublic
    val privateKey = keyPair.getPrivate
    val message = Ref.HexString.assertFromString("deadbeef")

    val signature = MessageSignature.sign(message, privateKey)

    MessageSignature.verify(signature, message, publicKey) shouldBe true
  }
}
