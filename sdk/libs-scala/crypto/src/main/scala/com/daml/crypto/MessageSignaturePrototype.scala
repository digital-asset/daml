// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.security.{PrivateKey, PublicKey, Signature}

final class MessageSignaturePrototype(val algorithm: String) {

  def sign(message: String, privateKey: PrivateKey): String = {
    val messageSign = Signature.getInstance(algorithm)

    messageSign.initSign(privateKey)
    messageSign.update(message.getBytes(StandardCharsets.UTF_8))

    messageSign.sign().map("%02x" format _).mkString
  }

  def verify(signature: String, message: String, publicKey: PublicKey): Boolean = {
    val signatureVerify = Signature.getInstance(algorithm)

    signatureVerify.initVerify(publicKey)
    signatureVerify.update(message.getBytes(StandardCharsets.UTF_8))

    signatureVerify.verify(new BigInteger(signature, 16).toByteArray)
  }
}

object MessageSignaturePrototype {
  val Secp256k1 = new MessageSignaturePrototype("SHA256withECDSA")
}
