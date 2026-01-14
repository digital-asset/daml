// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.crypto

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.math.ec.custom.sec.SecP256K1Curve

import java.security.interfaces.ECPublicKey
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

  @throws(classOf[NoSuchProviderException])
  @throws(classOf[InvalidKeyException])
  def validateKey(publicKey: PublicKey): Boolean =
    publicKey match {
      case key: ECPublicKey =>
        val x = key.getW.getAffineX
        val y = key.getW.getAffineY
        val point = new SecP256K1Curve().createPoint(x, y)
        point.isValid && !point.isInfinity

      case _ =>
        throw new InvalidKeyException("Invalid type for public key")
    }
}

object MessageSignaturePrototype {
  val Secp256k1 = new MessageSignaturePrototype("NONEwithECDSA")
}
