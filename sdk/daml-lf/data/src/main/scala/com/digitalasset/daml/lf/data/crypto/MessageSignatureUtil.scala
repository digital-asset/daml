// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package crypto

import com.daml.crypto.MessageSignaturePrototypeUtil

import java.security.spec.ECGenParameterSpec
import java.security.{KeyPair, KeyPairGenerator, PrivateKey, SecureRandom}

// The following utility methods should only be used within a testing context.
// They have been moved into a main project scope so that daml-script runners may access this code.
object MessageSignatureUtil {
  def sign(
      message: Ref.HexString,
      privateKey: PrivateKey,
      randomSrc: SecureRandom = new SecureRandom(),
  ): Ref.HexString = {
    val signature =
      MessageSignaturePrototypeUtil.Secp256k1.sign(
        Bytes.fromHexString(message).toByteArray,
        privateKey,
        randomSrc,
      )

    Ref.HexString.encode(Bytes.fromByteArray(signature))
  }

  def generateKeyPair: KeyPair = {
    val keyPairGen = KeyPairGenerator.getInstance("EC", "BC")
    keyPairGen.initialize(new ECGenParameterSpec("secp256k1"))

    keyPairGen.generateKeyPair()
  }
}
