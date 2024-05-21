// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import org.scalatest.wordspec.AsyncWordSpec

class SeedGeneratorTest extends AsyncWordSpec with BaseTest {
  "SeedGenerator" should {
    "generate fresh salts" in {
      val pureCrypto = new SymbolicPureCrypto
      val seedGenerator = new SeedGenerator(pureCrypto)

      val salt1 = seedGenerator.generateSaltSeed()
      val salt2 = seedGenerator.generateSaltSeed()

      salt1 should not equal salt2
    }
  }
}
