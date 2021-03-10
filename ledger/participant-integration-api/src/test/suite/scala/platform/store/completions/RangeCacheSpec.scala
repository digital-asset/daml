// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.completions

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.daml.platform.store.completions.OffsetsGenerator.genOffset

class RangeCacheSpec extends AnyFlatSpec with Matchers {

  "RangeCache" should "foo" in {
    Range(genOffset(3), genOffset(2))
  }
}
