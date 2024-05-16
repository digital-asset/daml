// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.google.protobuf.ByteString

object TestFingerprint {

  def generateFingerprint(id: String): Fingerprint = {
    val hash = Hash.digest(
      HashPurpose.PublicKeyFingerprint,
      ByteString.copyFrom(id.getBytes),
      HashAlgorithm.Sha256,
    )
    Fingerprint.tryCreate(hash.toLengthLimitedHexString)
  }

}
