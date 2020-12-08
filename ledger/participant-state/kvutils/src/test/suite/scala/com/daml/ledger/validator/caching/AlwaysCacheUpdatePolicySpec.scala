// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.ledger.validator.TestHelper
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AlwaysCacheUpdatePolicySpec extends AnyWordSpec with Matchers {
  private val policy = AlwaysCacheUpdatePolicy

  "cache update policy" should {
    "allow caching of all types of keys" in {
      for (stateKey <- TestHelper.allDamlStateKeyTypes) {
        policy.shouldCacheOnWrite(stateKey) shouldBe true
      }
    }

    "allow caching of all read values" in {
      for (stateKey <- TestHelper.allDamlStateKeyTypes) {
        policy.shouldCacheOnRead(stateKey) shouldBe true
      }
    }
  }
}
