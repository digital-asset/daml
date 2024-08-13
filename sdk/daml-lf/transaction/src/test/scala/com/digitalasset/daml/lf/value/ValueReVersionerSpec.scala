// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.value

import com.digitalasset.daml.lf.transaction.TransactionVersion.StableVersions
import com.digitalasset.daml.lf.transaction.Versioned
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ValueReVersionerSpec extends AnyWordSpec with Matchers {

  "reVersion" should {
    // When this no longer holds addition testing will be required
    "be able to reversion the oldest stable LF version as the latest" in {
      val expected = Versioned(StableVersions.max, Value.ValueNil)
      ValueReVersioner
        .reVersionValue(
          Versioned(StableVersions.min, expected.unversioned),
          StableVersions.max,
        ) shouldBe Right(expected)
    }
  }

}
