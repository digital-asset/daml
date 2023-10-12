// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.traffic

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.data.CantonTimestamp
import org.scalatest.flatspec.AnyFlatSpec

class TopUpQueueTest extends AnyFlatSpec with BaseTest {
  behavior of "TopUpQueue"

  private val topUp1 = TopUpEvent(
    PositiveLong.tryCreate(100L),
    CantonTimestamp.Epoch,
    PositiveInt.tryCreate(1),
  )
  private val topUp2 = TopUpEvent(
    PositiveLong.tryCreate(200L),
    CantonTimestamp.Epoch.plusSeconds(1),
    PositiveInt.tryCreate(2),
  )
  private val topUp3 = TopUpEvent(
    PositiveLong.tryCreate(300L),
    CantonTimestamp.Epoch.plusSeconds(2),
    PositiveInt.tryCreate(3),
  )

  it should "get initialized" in {
    val topUps = List(topUp1, topUp2, topUp3)
    val q = new TopUpQueue(topUps)
    q.getAllTopUps should contain theSameElementsInOrderAs topUps
  }

  it should "add top ups" in {
    val q = new TopUpQueue(List.empty)
    q.addOne(topUp1)
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp1)
    q.addOne(topUp2)
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp1, topUp2)
  }

  it should "prune and return current traffic limit for a timestamp" in {
    val q = new TopUpQueue(List.empty)
    q.addOne(topUp1)
    q.addOne(topUp2)
    q.addOne(topUp3)

    q.pruneUntilAndGetLimitFor(CantonTimestamp.Epoch) shouldBe topUp1.limit.toNonNegative
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp1, topUp2, topUp3)
    q.pruneUntilAndGetLimitFor(
      CantonTimestamp.Epoch.plusMillis(500)
    ) shouldBe topUp1.limit.toNonNegative
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp1, topUp2, topUp3)
    q.pruneUntilAndGetLimitFor(
      CantonTimestamp.Epoch.plusMillis(1000)
    ) shouldBe topUp2.limit.toNonNegative
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp2, topUp3)
    q.pruneUntilAndGetLimitFor(
      CantonTimestamp.Epoch.plusMillis(1500)
    ) shouldBe topUp2.limit.toNonNegative
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp2, topUp3)
    q.pruneUntilAndGetLimitFor(
      CantonTimestamp.Epoch.plusMillis(2000)
    ) shouldBe topUp3.limit.toNonNegative
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp3)
    q.pruneUntilAndGetLimitFor(
      CantonTimestamp.Epoch.plusMillis(3000)
    ) shouldBe topUp3.limit.toNonNegative
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp3)
  }

  it should "prune and return current and future top upe events for a timestamp" in {
    val q = new TopUpQueue(List.empty)
    q.addOne(topUp1)
    q.addOne(topUp2)
    q.addOne(topUp3)

    q.pruneUntilAndGetAllTopUpsFor(
      CantonTimestamp.Epoch
    ) should contain theSameElementsInOrderAs List(topUp1, topUp2, topUp3)
    q.pruneUntilAndGetAllTopUpsFor(
      CantonTimestamp.Epoch.plusMillis(500)
    ) should contain theSameElementsInOrderAs List(topUp1, topUp2, topUp3)
    q.pruneUntilAndGetAllTopUpsFor(
      CantonTimestamp.Epoch.plusMillis(1000)
    ) should contain theSameElementsInOrderAs List(topUp2, topUp3)
    q.pruneUntilAndGetAllTopUpsFor(
      CantonTimestamp.Epoch.plusMillis(1500)
    ) should contain theSameElementsInOrderAs List(topUp2, topUp3)
    q.pruneUntilAndGetAllTopUpsFor(
      CantonTimestamp.Epoch.plusMillis(2000)
    ) should contain theSameElementsInOrderAs List(topUp3)
    q.pruneUntilAndGetAllTopUpsFor(
      CantonTimestamp.Epoch.plusMillis(3000)
    ) should contain theSameElementsInOrderAs List(topUp3)
  }

  it should "return all top ups without modifying the queue" in {
    val q = new TopUpQueue(List.empty)
    q.addOne(topUp1)
    q.addOne(topUp2)
    q.addOne(topUp3)
    // Run twice and make sure we get the same result
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp1, topUp2, topUp3)
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp1, topUp2, topUp3)
    q.pruneUntilAndGetLimitFor(CantonTimestamp.Epoch).value shouldBe topUp1.limit.value
  }

  it should "return future limit without modifying the queue" in {
    val q = new TopUpQueue(List.empty)
    q.addOne(topUp1)
    q.addOne(topUp2)
    q.addOne(topUp3)
    // Run twice and make sure we get the same result
    q.getTrafficLimit(topUp3.validFromInclusive) shouldBe topUp3.limit.toNonNegative
    q.getAllTopUps should contain theSameElementsInOrderAs List(topUp1, topUp2, topUp3)
  }

  it should "sort top ups by validity timestamp" in {
    val q = new TopUpQueue(List.empty)
    val t1 = TopUpEvent(
      PositiveLong.tryCreate(100L),
      CantonTimestamp.Epoch,
      PositiveInt.tryCreate(1),
    )
    val t2 = TopUpEvent(
      PositiveLong.tryCreate(200L),
      CantonTimestamp.Epoch.plusSeconds(1),
      PositiveInt.tryCreate(2),
    )

    // Insert in reverse order
    q.addOne(t2)
    q.addOne(t1)

    // t1 should still be the valid one for CantonTimestamp.Epoch
    q.getAllTopUps should contain theSameElementsInOrderAs List(t1, t2)
    q.pruneUntilAndGetLimitFor(CantonTimestamp.Epoch).value shouldBe 100L
    q.pruneUntilAndGetLimitFor(CantonTimestamp.Epoch.plusSeconds(1)).value shouldBe 200L
    q.getAllTopUps should contain theSameElementsInOrderAs List(t2)
  }

  it should "discriminate equal timestamps using serial number" in {
    val q = new TopUpQueue(List.empty)
    val t1 = TopUpEvent(
      PositiveLong.tryCreate(100L),
      CantonTimestamp.Epoch,
      PositiveInt.tryCreate(1),
    )
    val t2 = TopUpEvent(
      PositiveLong.tryCreate(200L),
      CantonTimestamp.Epoch,
      PositiveInt.tryCreate(2),
    )

    // Insert in reverse order
    q.addOne(t2)
    q.addOne(t1)

    // t1 should still be the valid one for CantonTimestamp.Epoch
    q.getAllTopUps should contain theSameElementsInOrderAs List(t1, t2)
    q.pruneUntilAndGetLimitFor(CantonTimestamp.Epoch).value shouldBe 200L
  }
}
