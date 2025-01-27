// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.bouncycastle.jce.provider.BouncyCastleProvider

import java.security.{
  InvalidKeyException,
  NoSuchProviderException,
  PublicKey,
  Security,
  Signature,
  SignatureException,
}

/*
 * This class is prototype level and should not generally be used. Type safe wrapping classes should instead be defined
 * and used.
 */
final class MessageSignaturePrototype(val algorithm: String) {
  Security.addProvider(new BouncyCastleProvider)

  @throws(classOf[NoSuchProviderException])
  @throws(classOf[InvalidKeyException])
  @throws(classOf[SignatureException])
  def verify(signature: Array[Byte], message: Array[Byte], publicKey: PublicKey): Boolean = {
    val signatureVerify = Signature.getInstance(algorithm, "BC")

    signatureVerify.initVerify(publicKey)
    signatureVerify.update(message)

    signatureVerify.verify(signature)
  }
}

object MessageSignaturePrototype {
  val Secp256k1 = new MessageSignaturePrototype("SHA256withECDSA")
}
