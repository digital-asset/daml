// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator.caching

import com.daml.ledger.participant.state.kvutils.store.DamlStateKey
import com.daml.ledger.validator.TestHelper
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ImmutablesOnlyCacheUpdatePolicySpec extends AnyWordSpec with Matchers {
  private val policy = ImmutablesOnlyCacheUpdatePolicy

  "cache update policy" should {
    "allow write-through caching of packages" in {
      policy.shouldCacheOnWrite(aPackageKey) shouldBe true
    }

    "not allow write-through caching of non-packages" in {
      for (stateKey <- nonPackageKeyTypes) {
        policy.shouldCacheOnWrite(stateKey) shouldBe false
      }
    }

    "allow caching of read packages or parties" in {
      policy.shouldCacheOnRead(aPackageKey) shouldBe true
      policy.shouldCacheOnRead(aPartyKey) shouldBe true
    }

    "not allow caching of read mutable values" in {
      for (stateKey <- nonPackageKeyTypes if stateKey.getParty.isEmpty) {
        policy.shouldCacheOnRead(stateKey) shouldBe false
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
