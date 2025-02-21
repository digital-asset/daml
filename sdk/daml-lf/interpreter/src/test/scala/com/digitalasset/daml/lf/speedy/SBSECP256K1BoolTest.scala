// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.cctp.MessageSignatureUtil
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.speedy.SBuiltinFun.SBSECP256K1Bool
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.security.{KeyPairGenerator, Security}
import java.security.spec.InvalidKeySpecException

class SBSECP256K1BoolTest extends AnyFreeSpec with Matchers {
  Security.addProvider(new BouncyCastleProvider)

  "PublicKeys are correctly built from DER encoded public key blobs" in {
    val actualPublicKey = MessageSignatureUtil.generateKeyPair.getPublic
    val publicKey = Bytes.fromByteArray(actualPublicKey.getEncoded)

    SBSECP256K1Bool.extractPublicKey(publicKey) shouldBe actualPublicKey
  }

  "PublicKeys fail to be built from invalid byte encoded public key blobs" in {
    val invalidPublicKey = Bytes.assertFromString("deadbeef")

    assertThrows[InvalidKeySpecException](
      SBSECP256K1Bool.extractPublicKey(invalidPublicKey)
    )
  }

  "PublicKeys fail to be built from incorrectly formatted DER encoded public key blobs" in {
    val keyPairGen = KeyPairGenerator.getInstance("RSA")
    keyPairGen.initialize(1024)
    val badFormatPublicKey = keyPairGen.generateKeyPair().getPublic
    val invalidPublicKey = Bytes.fromByteArray(badFormatPublicKey.getEncoded)

    assertThrows[InvalidKeySpecException](
      SBSECP256K1Bool.extractPublicKey(invalidPublicKey)
    )
  }
}
