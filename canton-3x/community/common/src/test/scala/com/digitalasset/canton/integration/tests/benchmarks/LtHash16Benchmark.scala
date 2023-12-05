// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.benchmarks

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.LtHash16
import org.scalatest.wordspec.AnyWordSpec

class LtHash16Benchmark extends AnyWordSpec with BaseTest {

  import org.bouncycastle.crypto.digests.SHA256Digest

  def sha256(bytes: Array[Byte]): Array[Byte] = {
    val digest = new SHA256Digest()
    digest.update(bytes, 0, bytes.length)
    val out = new Array[Byte](digest.getDigestSize)
    digest.doFinal(out, 0)
    out
  }

  "LtHash16" should {

    // A simple benchmark to ensure that we can hash a batch of contract IDs at a rate of at least 0.5 per ms
    "be fast" in {
      def testMillis(numContractIds: Int): Long = {
        val muchoCids =
          for (i <- 1.to(numContractIds))
            yield sha256(s"disc$i".getBytes) ++ sha256(s"unic$i".getBytes)
        val h = LtHash16()
        val start = System.currentTimeMillis()
        muchoCids.foreach(v => h.add(v))
        System.currentTimeMillis() - start
      }
      val numContracts = 1000
      val testTime = testMillis(numContracts).toInt
      testTime should be < numContracts * 2
    }
  }
}
