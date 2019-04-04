// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.deduplicator

import org.scalatest.{Matchers, WordSpec}

import com.digitalasset.ledger.api.domain.{ApplicationId, CommandId}

class DeduplicatorSpec extends WordSpec with Matchers {

  "Deduplicator" should {
    "catch transactions" in {
      val deduplicator0 = Deduplicator()
      val (deduplicator1, dedupRes1) =
        deduplicator0.checkAndAdd(ApplicationId("appId0"), CommandId("commandId0"))
      dedupRes1 shouldBe false
      val (deduplicator2, dedupRes2) =
        deduplicator1.checkAndAdd(ApplicationId("appId0"), CommandId("commandId0"))
      dedupRes2 shouldBe true
      deduplicator1 shouldBe deduplicator2
      val (deduplicator3, dedupRes3) =
        deduplicator2.checkAndAdd(ApplicationId("appId1"), CommandId("commandId1"))
      dedupRes3 shouldBe false
    }
  }
}
