// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.MessageId
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec

trait SendTrackerStoreTest extends BeforeAndAfter {
  this: AnyWordSpec & BaseTest =>

  def sendTrackerStore(mk: () => SendTrackerStore): Unit =
    "pending sends" should {
      val (msgId1, msgId2, msgId3) =
        (MessageId.tryCreate("1"), MessageId.tryCreate("2"), MessageId.tryCreate("3"))
      val (ts1, ts2, ts3) =
        (
          CantonTimestamp.MinValue,
          CantonTimestamp.MinValue.plusSeconds(1),
          CantonTimestamp.MinValue.plusSeconds(2),
        )

      "be able to add, remove and list pending sends" in {
        val store = mk()

        valueOrFail(store.savePendingSend(msgId1, ts1))("savePendingSend msgId1")
        valueOrFail(store.savePendingSend(msgId2, ts2))("savePendingSend msgId2")
        val pendingSends1 = store.fetchPendingSends
        pendingSends1 shouldBe Map(msgId1 -> ts1, msgId2 -> ts2)
        store.removePendingSend(msgId2)
        valueOrFail(store.savePendingSend(msgId3, ts3))("savePendingSend msgId3")
        val pendingSends2 = store.fetchPendingSends
        pendingSends2 shouldBe Map(msgId1 -> ts1, msgId3 -> ts3)
      }

      "fail if we try to track a send with an already tracked id" in {
        val store = mk()

        valueOrFail(store.savePendingSend(msgId1, ts1))("savePendingSend msgId1")
        store
          .savePendingSend(msgId1, ts2)
          .left
          .value shouldBe SavePendingSendError.MessageIdAlreadyTracked
      }

      "be okay tracking a send with a tracked id that has been previously used but since removed" in {
        val store = mk()

        valueOrFail(store.savePendingSend(msgId1, ts1))("savePendingSend msgId1")
        store.removePendingSend(msgId1)
        valueOrFail(store.savePendingSend(msgId1, ts2))("savePendingSend msgId1 again")

        store.fetchPendingSends shouldBe Map(msgId1 -> ts2)
      }
    }
}
