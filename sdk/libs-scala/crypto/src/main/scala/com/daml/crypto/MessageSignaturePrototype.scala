// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import java.security.{PrivateKey, PublicKey, Signature}

final class MessageSignaturePrototype(val algorithm: String) {

  def sign(message: Array[Byte], privateKey: PrivateKey): Array[Byte] = {
    val messageSign = Signature.getInstance(algorithm)

    messageSign.initSign(privateKey)
    messageSign.update(message)

    messageSign.sign()
  }

  def verify(signature: Array[Byte], message: Array[Byte], publicKey: PublicKey): Boolean = {
    val signatureVerify = Signature.getInstance(algorithm)

    signatureVerify.initVerify(publicKey)
    signatureVerify.update(message)

    signatureVerify.verify(signature)
  }
}

object MessageSignaturePrototype {
  val Secp256k1 = new MessageSignaturePrototype("SHA256withECDSA")
}
