// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package crypto

import com.daml.crypto.MessageDigestPrototype

object MessageDigest {
  def digest(message: Ref.HexString): Ref.HexString = {
    val digest = MessageDigestPrototype.KecCak256.newDigest
    digest.update(Ref.HexString.decode(message).toByteBuffer)

    Ref.HexString.encode(Bytes.fromByteArray(digest.digest()))
  }
}
