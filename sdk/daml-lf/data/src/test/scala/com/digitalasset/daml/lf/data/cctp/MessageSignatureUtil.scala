// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package cctp

import com.daml.crypto.MessageSignaturePrototypeUtil

import java.security.PrivateKey

object MessageSignatureUtil {
  def sign(message: Ref.HexString, privateKey: PrivateKey): Ref.HexString = {
    val signature =
      MessageSignaturePrototypeUtil.Secp256k1.sign(
        Bytes.fromHexString(message).toByteArray,
        privateKey,
      )

    Ref.HexString.encode(Bytes.fromByteArray(signature))
  }
}
