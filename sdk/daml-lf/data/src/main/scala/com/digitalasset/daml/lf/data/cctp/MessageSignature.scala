// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package cctp

import com.daml.crypto.MessageSignaturePrototype

import java.security.{InvalidKeyException, NoSuchProviderException, PublicKey, SignatureException}

object MessageSignature {
  @throws(classOf[NoSuchProviderException])
  @throws(classOf[InvalidKeyException])
  @throws(classOf[SignatureException])
  def verify(signature: Bytes, message: Bytes, publicKey: PublicKey): Boolean = {
    MessageSignaturePrototype.Secp256k1.verify(
      signature.toByteArray,
      message.toByteArray,
      publicKey,
    )
  }
}
