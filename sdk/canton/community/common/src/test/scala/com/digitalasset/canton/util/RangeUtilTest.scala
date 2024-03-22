// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTestWordSpec

class RangeUtilTest extends BaseTestWordSpec {
  "RangeUtil" can {
    "partition an index range" in {
      RangeUtil.partitionIndexRange(0, -1, 1) shouldBe Seq(0 -> -1)
      RangeUtil.partitionIndexRange(0, 0, 1) shouldBe Seq(0 -> 0)
      RangeUtil.partitionIndexRange(0, 1, 1) shouldBe Seq(0 -> 1)
      RangeUtil.partitionIndexRange(0, 2, 1) shouldBe Seq(0 -> 1, 1 -> 2)

      RangeUtil.partitionIndexRange(0, 2, 3) shouldBe Seq(0 -> 2)
      RangeUtil.partitionIndexRange(0, 3, 3) shouldBe Seq(0 -> 3)
      RangeUtil.partitionIndexRange(0, 4, 3) shouldBe Seq(0 -> 3, 3 -> 4)
      RangeUtil.partitionIndexRange(0, 7, 3) shouldBe Seq(0 -> 3, 3 -> 6, 6 -> 7)

      RangeUtil.partitionIndexRange(2, 9, 3) shouldBe Seq(2 -> 5, 5 -> 8, 8 -> 9)
    }
  }
}
