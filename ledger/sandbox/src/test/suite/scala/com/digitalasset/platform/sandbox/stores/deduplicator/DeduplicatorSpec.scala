// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.deduplicator

import com.digitalasset.daml.lf.data.Ref
import org.scalatest.{Matchers, WordSpec}
import com.digitalasset.ledger.api.domain.{ApplicationId, CommandId}

class DeduplicatorSpec extends WordSpec with Matchers {

  "Deduplicator" should {
    "catch transactions" in {
      val deduplicator0 = Deduplicator()
      val (deduplicator1, dedupRes1) =
        deduplicator0.checkAndAdd(
          Ref.Party.assertFromString("party0"),
          ApplicationId(Ref.LedgerString.assertFromString("appId0")),
          CommandId(Ref.LedgerString.assertFromString("commandId0"))
        )
      dedupRes1 shouldBe false
      val (deduplicator2, dedupRes2) =
        deduplicator1.checkAndAdd(
          Ref.Party.assertFromString("party0"),
          ApplicationId(Ref.LedgerString.assertFromString("appId0")),
          CommandId(Ref.LedgerString.assertFromString("commandId0"))
        )
      dedupRes2 shouldBe true
      deduplicator1 shouldBe deduplicator2
      val (deduplicator3, dedupRes3) =
        deduplicator2.checkAndAdd(
          Ref.Party.assertFromString("party0"),
          ApplicationId(Ref.LedgerString.assertFromString("appId1")),
          CommandId(Ref.LedgerString.assertFromString("commandId1"))
        )
      dedupRes3 shouldBe false
      val (deduplicator4, dedupRes4) =
        deduplicator2.checkAndAdd(
          Ref.Party.assertFromString("party1"),
          ApplicationId(Ref.LedgerString.assertFromString("appId0")),
          CommandId(Ref.LedgerString.assertFromString("commandId0"))
        )
      dedupRes4 shouldBe false
    }
  }
}
