// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

import scala.io.Source

class Blake2xbTest extends AnyWordSpec with BaseTest {

  import TestVectors.*

  def hexString(buf: Array[Byte]): String = buf.map("%02X" format _).mkString.toLowerCase()

  "Blake2xb" should {
    // Golden test taken from
    // https://github.com/facebook/folly/blob/993de57926e7b17306ac9c5c46781a15d1b04414/folly/experimental/crypto/test/Blake2xbTest.cpp
    "pass the golden tests" in {
      val input = new Array[Byte](256)

      for (i <- 0.until(256)) {
        input(i) = i.toByte
      }

      forAll(hexVectors) { v =>
        val len = v.length / 2
        val out = hexString(Blake2xb.digest(input, len))
        assert(
          out == v,
          s"""Blake2xb digest of length $len should be
             |$v
             |but was
             |$out""".stripMargin,
        )
      }
    }
  }
}

object TestVectors {

  val resourceName = "blake2xb-golden-tests.txt"
  val hexVectors: List[String] = Source.fromResource(resourceName).getLines().toList

}
