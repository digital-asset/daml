// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.traffic.TrafficBalanceManager.TrafficBalance
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

trait TrafficBalanceStoreTest
    extends BeforeAndAfterAll
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec {
  this: AsyncWordSpec =>

  def trafficBalanceStore(mk: () => TrafficBalanceStore): Unit = {
    val alice = ParticipantId("alice")
    val bob = ParticipantId("bob")
    val t1 = CantonTimestamp.Epoch
    val t2 = t1.plusSeconds(1)
    val t3 = t2.plusSeconds(2)

    "trafficBalanceStore" should {
      "store and lookup balances" in {
        val store = mk()
        val balanceAlice1 =
          TrafficBalance(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val balanceAlice2 =
          TrafficBalance(alice.member, PositiveInt.tryCreate(2), NonNegativeLong.tryCreate(10L), t2)
        val balanceBob =
          TrafficBalance(bob.member, PositiveInt.one, NonNegativeLong.tryCreate(8L), t1)
        for {
          _ <- store.store(balanceAlice1)
          _ <- store.store(balanceAlice2)
          _ <- store.store(balanceBob)
          aliceEvents <- store.lookup(alice)
          bobEvents <- store.lookup(bob)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            balanceAlice1,
            balanceAlice2,
          )
          bobEvents should contain theSameElementsInOrderAs List(balanceBob)
        }
      }

      "be idempotent if inserting the same balance twice" in {
        val store = mk()
        val balanceAlice1 =
          TrafficBalance(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        for {
          _ <- store.store(balanceAlice1)
          _ <- store.store(balanceAlice1)
          aliceEvents <- store.lookup(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            balanceAlice1
          )
        }
      }

      "update if the serial is higher than the previous one for the same timestamp" in {
        val store = mk()
        val balanceAlice1 =
          TrafficBalance(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val balanceAlice2 =
          TrafficBalance(alice.member, PositiveInt.tryCreate(2), NonNegativeLong.tryCreate(10L), t1)
        for {
          _ <- store.store(balanceAlice1)
          _ <- store.store(balanceAlice2)
          aliceEvents <- store.lookup(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            balanceAlice2
          )
        }
      }

      "not update if the serial is lower or equal to the previous one for the same timestamp" in {
        val store = mk()
        val balanceAlice1 =
          TrafficBalance(alice.member, PositiveInt.tryCreate(2), NonNegativeLong.tryCreate(5L), t1)
        val balanceAlice2 =
          TrafficBalance(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(10L), t1)
        for {
          _ <- store.store(balanceAlice1)
          _ <- store.store(balanceAlice2)
          aliceEvents <- store.lookup(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            balanceAlice1
          )
        }
      }

      "remove all balances below a given timestamp, keeping the closest one < below it" in {
        val store = mk()
        // Between t2 and t3
        val t2point5 = t2.plusMillis(500)
        val balanceAlice1 =
          TrafficBalance(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val balanceAlice2 =
          TrafficBalance(alice.member, PositiveInt.tryCreate(2), NonNegativeLong.tryCreate(10L), t2)
        val balanceAlice3 =
          TrafficBalance(alice.member, PositiveInt.tryCreate(3), NonNegativeLong.tryCreate(20L), t3)
        for {
          _ <- store.store(balanceAlice1)
          _ <- store.store(balanceAlice2)
          _ <- store.store(balanceAlice3)
          _ <- store.pruneBelowExclusive(alice.member, t2point5)
          aliceEvents <- store.lookup(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(balanceAlice2, balanceAlice3)
        }
      }

      "remove all balances below a given timestamp for which there is an update" in {
        val store = mk()
        val balanceAlice1 =
          TrafficBalance(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val balanceAlice2 =
          TrafficBalance(alice.member, PositiveInt.tryCreate(2), NonNegativeLong.tryCreate(10L), t2)
        val balanceAlice3 =
          TrafficBalance(alice.member, PositiveInt.tryCreate(3), NonNegativeLong.tryCreate(20L), t3)
        for {
          _ <- store.store(balanceAlice1)
          _ <- store.store(balanceAlice2)
          _ <- store.store(balanceAlice3)
          _ <- store.pruneBelowExclusive(alice.member, t2)
          aliceEvents <- store.lookup(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(balanceAlice2, balanceAlice3)
        }
      }

      "keep the latest balance if they're all in the pruning window" in {
        val store = mk()
        val balanceAlice1 =
          TrafficBalance(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val balanceAlice2 =
          TrafficBalance(alice.member, PositiveInt.tryCreate(2), NonNegativeLong.tryCreate(10L), t2)
        val balanceAlice3 =
          TrafficBalance(alice.member, PositiveInt.tryCreate(3), NonNegativeLong.tryCreate(20L), t3)
        for {
          _ <- store.store(balanceAlice1)
          _ <- store.store(balanceAlice2)
          _ <- store.store(balanceAlice3)
          _ <- store.pruneBelowExclusive(alice.member, t3.plusSeconds(1))
          aliceEvents <- store.lookup(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(balanceAlice3)
        }
      }

      "return the correct max timestamp" in {
        val store = mk()
        val balance1 =
          TrafficBalance(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val balance2 =
          TrafficBalance(bob.member, PositiveInt.tryCreate(2), NonNegativeLong.tryCreate(10L), t2)

        for {
          max0 <- store.maxTsO
          _ <- store.store(balance1)
          max1 <- store.maxTsO
          _ <- store.store(balance2)
          max2 <- store.maxTsO
        } yield {
          max0 shouldBe None
          max1 shouldBe Some(t1)
          max2 shouldBe Some(t2)
        }
      }
    }
  }
}
