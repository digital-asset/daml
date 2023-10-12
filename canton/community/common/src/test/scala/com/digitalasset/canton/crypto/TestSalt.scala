// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either.*

object TestSalt {

  def generateSeed(index: Int): SaltSeed = {
    SaltSeed(TestHash.digest(index).unwrap)
  }

  // Generates a deterministic salt for hashing based on the provided index
  // Assumes TestHash uses SHA-256
  def generateSalt(index: Int): Salt =
    Salt
      .create(TestHash.digest(index).unwrap, SaltAlgorithm.Hmac(HmacAlgorithm.HmacSha256))
      .valueOr(err => throw new IllegalStateException(err.toString))

}
