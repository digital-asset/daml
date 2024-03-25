// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.deterministic.encryption

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class SP800HashDRBGSecureRandomTest extends AnyWordSpec with BaseTest {

  "SP800HashDRBGSecureRandom" should {

    "generate the same bytes if the 'seed' used is the same" in {
      val message = "test"
      val numBytes = 1024

      val newRandom1 = SP800HashDRBGSecureRandom(message.getBytes(), loggerFactory)
      val newRandom2 = SP800HashDRBGSecureRandom(message.getBytes(), loggerFactory)

      val arrRandom1FirstBytes = new Array[Byte](numBytes)
      val arrRandom1SecondBytes = new Array[Byte](numBytes)
      val arrRandom2FirstBytes = new Array[Byte](numBytes)
      val arrRandom2SecondBytes = new Array[Byte](numBytes)

      newRandom1.nextBytes(arrRandom1FirstBytes)
      newRandom1.nextBytes(arrRandom1SecondBytes)
      newRandom2.nextBytes(arrRandom2FirstBytes)
      newRandom2.nextBytes(arrRandom2SecondBytes)

      arrRandom1FirstBytes shouldBe arrRandom2FirstBytes
      arrRandom1SecondBytes shouldBe arrRandom2SecondBytes
      arrRandom1FirstBytes should not be arrRandom1SecondBytes
    }

  }

}
