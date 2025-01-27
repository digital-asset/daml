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

import java.security.Security

class SBSECP256K1BoolTest extends AnyFreeSpec with Matchers {
  Security.addProvider(new BouncyCastleProvider)

  "PublicKeys are correctly built from hex encoded public key strings" in {
    val actualPublicKey = MessageSignatureUtil.generateKeyPair.getPublic
    val hexEncodedPublicKey = Bytes.fromByteArray(actualPublicKey.getEncoded).toHexString

    SBSECP256K1Bool.extractPublicKey(hexEncodedPublicKey) shouldBe actualPublicKey
  }
}
