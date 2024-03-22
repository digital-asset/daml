// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.{BaseTest, DiscardOps}
import org.scalatest.wordspec.AnyWordSpec

class HashPurposeTest extends AnyWordSpec with BaseTest {
  "HashPurpose" should {
    "allow purposes with different IDs" in {
      HashPurpose(java.lang.Integer.MAX_VALUE - 1, "MAX-1").discard
      HashPurpose(java.lang.Integer.MAX_VALUE, "MAX").discard
    }

    "fail when two HashPurposes have the same ID" in {
      assertThrows[IllegalArgumentException](
        HashPurpose(HashPurpose.MerkleTreeInnerNode.id, "duplicate")
      )
    }
  }
}

object HashPurposeTest {

  /** Mockito argument matcher that matches any [[HashPurpose]]. */
  def anyHashPurpose: HashPurpose = {
    org.mockito.ArgumentMatchers.any[Int]
    TestHash.testHashPurpose
  }
}
