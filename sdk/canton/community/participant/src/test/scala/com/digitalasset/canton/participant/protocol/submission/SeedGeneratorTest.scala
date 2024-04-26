// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingIdentityFactory}
import org.scalatest.wordspec.AsyncWordSpec

class SeedGeneratorTest extends AsyncWordSpec with BaseTest {
  "SeedGenerator" should {
    "generate fresh salts" in {
      val crypto =
        TestingIdentityFactory.newCrypto(loggerFactory)(DefaultTestIdentities.mediator)
      val seedGenerator = new SeedGenerator(crypto.pureCrypto)

      val salt1 = seedGenerator.generateSaltSeed()
      val salt2 = seedGenerator.generateSaltSeed()

      salt1 should not equal salt2
    }
  }
}
