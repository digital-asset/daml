// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.{PrivateKey, SecureRandom, Security, Signature}

// The following utility methods should only be used within a testing context.
// They have been moved into a main project scope so that daml-script runners may access this code.
class MessageSignaturePrototypeUtil(val algorithm: String) {
  Security.addProvider(new BouncyCastleProvider)

  def sign(
      message: Array[Byte],
      privateKey: PrivateKey,
      randomSrc: SecureRandom = new SecureRandom(),
  ): Array[Byte] = {
    val messageSign = Signature.getInstance(algorithm, "BC")

    messageSign.initSign(privateKey, randomSrc)
    messageSign.update(message)

    messageSign.sign()
  }
}

object MessageSignaturePrototypeUtil {
  val Secp256k1 = new MessageSignaturePrototypeUtil("SHA256withECDSA")
}
