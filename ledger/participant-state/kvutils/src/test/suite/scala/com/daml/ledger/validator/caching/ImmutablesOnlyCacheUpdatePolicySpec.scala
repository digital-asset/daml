// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlStateKey
import com.daml.ledger.validator.TestHelper
import com.daml.ledger.validator.caching.ImmutablesOnlyCacheUpdatePolicy._
import org.scalatest.{Matchers, WordSpec}

class ImmutablesOnlyCacheUpdatePolicySpec extends WordSpec with Matchers {
  "cache update policy" should {
    "allow write-through caching of packages" in {
      shouldCacheOnWrite(aPackageKey) shouldBe true
    }

    "not allow write-through caching of non-packages" in {
      for (stateKey <- nonPackageKeyTypes) {
        shouldCacheOnWrite(stateKey) shouldBe false
      }
    }

    "allow caching of read packages or parties" in {
      shouldCacheOnRead(aPackageKey) shouldBe true
      shouldCacheOnRead(aPartyKey) shouldBe true
    }

    "not allow caching of read mutable values" in {
      for (stateKey <- nonPackageKeyTypes if stateKey.getParty.isEmpty) {
        shouldCacheOnRead(stateKey) shouldBe false
      }
    }
  }

  private val aPackageKey: DamlStateKey = DamlStateKey.newBuilder
    .setPackageId("a package ID")
    .build

  private val aPartyKey: DamlStateKey = DamlStateKey.newBuilder
    .setParty("a party")
    .build

  private val nonPackageKeyTypes: Seq[DamlStateKey] =
    TestHelper.allDamlStateKeyTypes.filter(_.getPackageId.isEmpty)
}
