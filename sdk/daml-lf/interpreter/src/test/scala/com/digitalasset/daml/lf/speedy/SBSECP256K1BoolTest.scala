// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.cctp.MessageSignatureUtil
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.speedy.SBuiltinFun.SBSECP256K1Bool
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.security.{KeyPairGenerator, Security}
import java.security.spec.InvalidKeySpecException

class SBSECP256K1BoolTest extends AnyFreeSpec with Matchers {
  Security.addProvider(new BouncyCastleProvider)

  "PublicKeys are correctly built from hex encoded public key strings" in {
    val actualPublicKey = MessageSignatureUtil.generateKeyPair.getPublic
    val hexEncodedPublicKey = Bytes.fromByteArray(actualPublicKey.getEncoded).toHexString

    SBSECP256K1Bool.extractPublicKey(hexEncodedPublicKey) shouldBe actualPublicKey
  }

  "PublicKeys fail to be built from invalid hex encoded public key strings" in {
    val invalidHexEncodedPublicKey = Ref.HexString.assertFromString("deadbeef")

    assertThrows[InvalidKeySpecException](
      SBSECP256K1Bool.extractPublicKey(invalidHexEncodedPublicKey)
    )
  }

  "PublicKeys fail to be built from incorrectly formatted hex encoded public key strings" in {
    val keyPairGen = KeyPairGenerator.getInstance("RSA")
    keyPairGen.initialize(1024)
    val badFormatPublicKey = keyPairGen.generateKeyPair().getPublic
    val invalidHexEncodedPublicKey =
      Ref.HexString.encode(Bytes.fromByteArray(badFormatPublicKey.getEncoded))

    assertThrows[InvalidKeySpecException](
      SBSECP256K1Bool.extractPublicKey(invalidHexEncodedPublicKey)
    )
  }
}
