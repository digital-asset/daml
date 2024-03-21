// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.{SymbolicCrypto, SymbolicPureCrypto}
import org.scalatest.wordspec.AnyWordSpec

class SaltTest extends AnyWordSpec with BaseTest {

  "Salt" should {

    "serializing and deserializing via protobuf" in {
      val salt = TestSalt.generateSalt(0)
      val saltP = salt.toProtoV0
      Salt.fromProtoV0(saltP).value shouldBe salt
    }

    "generate a fresh salt seeds" in {
      val crypto = SymbolicCrypto.create(
        testedReleaseProtocolVersion,
        timeouts,
        loggerFactory,
      )
      val salt1 = SaltSeed.generate()(crypto.pureCrypto)
      val salt2 = SaltSeed.generate()(crypto.pureCrypto)

      salt1 shouldBe a[SaltSeed]
      salt1 should not equal salt2
    }

    "derive a salt" in {
      val hmacOps = new SymbolicPureCrypto
      val seedSalt = TestSalt.generateSeed(0)
      val salt = Salt.deriveSalt(seedSalt, 0, hmacOps)

      // The derived salt must be different than the seed salt value
      salt.value.unwrap should not equal seedSalt.unwrap
    }

  }

}
