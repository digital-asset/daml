// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

import org.scalatest.RecoverMethods
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class OffsetSpec extends AnyWordSpec with Matchers with RecoverMethods {
  "Offset.predecessor" should {
    "fail if beforeBegin" in {
      a[UnsupportedOperationException] should be thrownBy {
        Offset.beforeBegin.predecessor
      }
    }

    "fail if all zeros" in {
      a[UnsupportedOperationException] should be thrownBy {
        Offset.fromByteArray(Array(0.toByte, 0.toByte)).predecessor
      }
    }

    "return predecessor" in {
      Offset.fromByteArray(Array(255.toByte)).predecessor shouldBe
        Offset.fromByteArray(Array(254.toByte))

      Offset.fromByteArray(Array(128.toByte)).predecessor shouldBe
        Offset.fromByteArray(Array(127.toByte))

      Offset.fromByteArray(Array(128.toByte, 0.toByte)).predecessor shouldBe
        Offset.fromByteArray(Array(127.toByte, 255.toByte))

      val _0_1_1_0_0 =
        Offset.fromByteArray(Array(0.toByte, 1.toByte, 1.toByte, 0.toByte, 0.toByte))

      val _0_1_0_255_255 =
        Offset.fromByteArray(Array(0.toByte, 1.toByte, 0.toByte, 255.toByte, 255.toByte))

      _0_1_1_0_0.predecessor shouldBe _0_1_0_255_255
    }
  }
}
