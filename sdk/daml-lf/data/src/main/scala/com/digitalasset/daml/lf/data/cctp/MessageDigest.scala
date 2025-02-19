// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data
package cctp

import com.daml.crypto.MessageDigestPrototype

object MessageDigest {
  def digest(message: Bytes): Bytes = {
    val digest = MessageDigestPrototype.KecCak256.newDigest
    digest.update(message.toByteBuffer)

    Bytes.fromByteArray(digest.digest())
  }
}
