// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.driver.BftBlockOrderer
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.time.BftTime
import org.scalatest.wordspec.AnyWordSpec

class BftBlockOrdererTest extends AnyWordSpec with BaseTest {

  "Config creation" should {
    "fail only" when {
      "there are too many requests per block proposal" in {
        Table(
          ("maxRequestsInBatch", "maxBatchesPerProposal"),
          (31, 31), // 961 < 999
          (999, 1), // 999
          (100, 10), // 1000 > 999
          (32, 32), // 1024 > 999
        ).foreach { case (maxRequestsInBatch, maxBatchesPerProposal) =>
          val maxRequestsPerBlock = maxRequestsInBatch * maxBatchesPerProposal
          if (maxRequestsPerBlock > BftTime.MaxRequestsPerBlock) {
            val expectedMessage =
              s"requirement failed: Maximum block size too big: $maxRequestsInBatch maximum requests per batch and " +
                s"$maxBatchesPerProposal maximum batches per block proposal means " +
                s"$maxRequestsPerBlock maximum requests per block, " +
                s"but the maximum number allowed of requests per block is ${BftTime.MaxRequestsPerBlock}"
            the[IllegalArgumentException] thrownBy BftBlockOrderer.Config(
              maxRequestsInBatch = maxRequestsInBatch.toShort,
              maxBatchesPerBlockProposal = maxBatchesPerProposal.toShort,
            ) should have message expectedMessage
          }
        }
      }
    }
  }
}
