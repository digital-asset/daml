// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.{PrivateKey, Security, Signature}

class MessageSignaturePrototypeUtil(val algorithm: String) {
  Security.addProvider(new BouncyCastleProvider)

  def sign(message: Array[Byte], privateKey: PrivateKey): Array[Byte] = {
    val messageSign = Signature.getInstance(algorithm, "BC")

    messageSign.initSign(privateKey)
    messageSign.update(message)

    messageSign.sign()
  }
}

object MessageSignaturePrototypeUtil {
  val Secp256k1 = new MessageSignaturePrototypeUtil("SHA256withECDSA")
}
