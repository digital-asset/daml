// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.ledger.validator.TestHelper
import com.daml.ledger.validator.caching.AlwaysCacheUpdatePolicy._
import org.scalatest.{Matchers, WordSpec}

class AlwaysCacheUpdatePolicySpec extends WordSpec with Matchers {
  "cache update policy" should {
    "allow caching of all types of keys" in {
      for (stateKey <- TestHelper.allDamlStateKeyTypes) {
        shouldCacheOnWrite(stateKey) shouldBe true
      }
    }

    "allow caching of all read values" in {
      for (stateKey <- TestHelper.allDamlStateKeyTypes) {
        shouldCacheOnRead(stateKey) shouldBe true
      }
    }
  }
}
