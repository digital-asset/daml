// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.speedy.SBuiltinFun.SBSECP256K1Bool
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.security.{KeyPairGenerator, SecureRandom, Security}
import java.security.spec.ECGenParameterSpec

class SBSECP256K1BoolTest extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    val _ = Security.insertProviderAt(new BouncyCastleProvider, 1)
  }

  "PublicKeys are correctly built from hex encoded public key strings" in {
    val keyPairGen = KeyPairGenerator.getInstance("EC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"), new SecureRandom())
    val actualPublicKey = keyPairGen.generateKeyPair().getPublic
    val hexEncodedPublicKey = Ref.HexString.encode(Bytes.fromByteArray(actualPublicKey.getEncoded))

    SBSECP256K1Bool.extractPublicKey(hexEncodedPublicKey) shouldBe actualPublicKey
  }
}
