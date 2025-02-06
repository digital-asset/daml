// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.traffic.store

import cats.syntax.parallel.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.traffic.TrafficPurchased
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.{BaseTest, FailOnShutdown, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

trait TrafficPurchasedStoreTest
    extends BeforeAndAfterAll
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec
    with FailOnShutdown {
  this: AsyncWordSpec =>

  def trafficPurchasedStore(mk: () => TrafficPurchasedStore): Unit = {
    val alice = ParticipantId("alice")
    val bob = ParticipantId("bob")
    val t0 = CantonTimestamp.Epoch
    val t1 = t0.plusSeconds(1)
    val t2 = t1.plusSeconds(1)
    val t3 = t2.plusSeconds(1)
    val t4 = t3.plusSeconds(1)

    val purchaseEntryBob1 =
      TrafficPurchased(
        bob.member,
        PositiveInt.tryCreate(3),
        NonNegativeLong.tryCreate(20L),
        t1,
      )
    val purchaseEntryBob2 =
      TrafficPurchased(
        bob.member,
        PositiveInt.tryCreate(3),
        NonNegativeLong.tryCreate(20L),
        t2,
      )
    val purchaseEntryBob3 =
      TrafficPurchased(
        bob.member,
        PositiveInt.tryCreate(3),
        NonNegativeLong.tryCreate(20L),
        t3,
      )

    "trafficPurchasedStore" should {

      "store and lookup balances" in {
        val store = mk()
        val purchaseEntryAlice1 =
          TrafficPurchased(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val purchaseEntryAlice2 =
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(2),
            NonNegativeLong.tryCreate(10L),
            t2,
          )
        val purchaseEntryBob =
          TrafficPurchased(bob.member, PositiveInt.one, NonNegativeLong.tryCreate(8L), t1)
        for {
          _ <- store.store(purchaseEntryAlice1)
          _ <- store.store(purchaseEntryAlice2)
          _ <- store.store(purchaseEntryBob)
          aliceEvents <- store.lookup(alice)
          bobEvents <- store.lookup(bob)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            purchaseEntryAlice1,
            purchaseEntryAlice2,
          )
          bobEvents should contain theSameElementsInOrderAs List(purchaseEntryBob)
        }
      }

      "be idempotent if inserting the same balance twice" in {
        val store = mk()
        val purchaseEntryAlice1 =
          TrafficPurchased(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        for {
          _ <- store.store(purchaseEntryAlice1)
          _ <- store.store(purchaseEntryAlice1)
          aliceEvents <- store.lookup(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            purchaseEntryAlice1
          )
        }
      }

      "update if the serial is higher than the previous one for the same timestamp" in {
        val store = mk()
        val purchaseEntryAlice1 =
          TrafficPurchased(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val purchaseEntryAlice2 =
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(2),
            NonNegativeLong.tryCreate(10L),
            t1,
          )
        for {
          _ <- store.store(purchaseEntryAlice1)
          _ <- store.store(purchaseEntryAlice2)
          aliceEvents <- store.lookup(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            purchaseEntryAlice2
          )
        }
      }

      "not update if the serial is lower or equal to the previous one for the same timestamp" in {
        val store = mk()
        val purchaseEntryAlice1 =
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(2),
            NonNegativeLong.tryCreate(5L),
            t1,
          )
        val purchaseEntryAlice2 =
          TrafficPurchased(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(10L), t1)
        for {
          _ <- store.store(purchaseEntryAlice1)
          _ <- store.store(purchaseEntryAlice2)
          aliceEvents <- store.lookup(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            purchaseEntryAlice1
          )
        }
      }

      "remove all balances below a given timestamp, keeping the closest one < below it" in {
        val store = mk()
        // Between t2 and t3
        val t2point5 = t2.plusMillis(500)
        val purchaseEntryAlice1 =
          TrafficPurchased(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val purchaseEntryAlice2 =
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(2),
            NonNegativeLong.tryCreate(10L),
            t2,
          )
        val purchaseEntryAlice3 =
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(3),
            NonNegativeLong.tryCreate(20L),
            t3,
          )
        for {
          _ <- store.store(purchaseEntryAlice1)
          _ <- store.store(purchaseEntryBob1)
          _ <- store.store(purchaseEntryAlice2)
          _ <- store.store(purchaseEntryAlice3)
          _ <- store.pruneBelowExclusive(t2point5)
          aliceEvents <- store.lookup(alice)
          bobEvents <- store.lookup(bob)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            purchaseEntryAlice2,
            purchaseEntryAlice3,
          )
          // We should keep bob's balance because it's the only one
          bobEvents should contain theSameElementsInOrderAs List(purchaseEntryBob1)
        }
      }

      "remove all balances below a given timestamp for which there is an update" in {
        val store = mk()
        val purchaseEntryAlice1 =
          TrafficPurchased(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val purchaseEntryAlice2 =
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(2),
            NonNegativeLong.tryCreate(10L),
            t2,
          )
        val purchaseEntryAlice3 =
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(3),
            NonNegativeLong.tryCreate(20L),
            t3,
          )
        for {
          _ <- store.store(purchaseEntryAlice1)
          _ <- store.store(purchaseEntryAlice2)
          _ <- store.store(purchaseEntryAlice3)
          _ <- store.store(purchaseEntryBob1)
          _ <- store.store(purchaseEntryBob2)
          _ <- store.store(purchaseEntryBob3)
          _ <- store.pruneBelowExclusive(t2)
          aliceEvents <- store.lookup(alice)
          bobEvents <- store.lookup(bob)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            purchaseEntryAlice2,
            purchaseEntryAlice3,
          )
          bobEvents should contain theSameElementsInOrderAs List(
            purchaseEntryBob2,
            purchaseEntryBob3,
          )
        }
      }

      "keep the latest balance if they're all in the pruning window" in {
        val store = mk()
        val purchaseEntryAlice1 =
          TrafficPurchased(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val purchaseEntryAlice2 =
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(2),
            NonNegativeLong.tryCreate(10L),
            t2,
          )
        val purchaseEntryAlice3 =
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(3),
            NonNegativeLong.tryCreate(20L),
            t3,
          )
        for {
          _ <- store.store(purchaseEntryAlice1)
          _ <- store.store(purchaseEntryAlice2)
          _ <- store.store(purchaseEntryAlice3)
          _ <- store.store(purchaseEntryBob1)
          _ <- store.store(purchaseEntryBob2)
          _ <- store.store(purchaseEntryBob3)
          _ <- store.pruneBelowExclusive(t3.plusSeconds(1))
          aliceEvents <- store.lookup(alice)
          bobEvents <- store.lookup(bob)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(purchaseEntryAlice3)
          bobEvents should contain theSameElementsInOrderAs List(purchaseEntryBob3)
        }
      }

      "return the correct max timestamp" in {
        val store = mk()
        val balance1 =
          TrafficPurchased(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1)
        val balance2 =
          TrafficPurchased(bob.member, PositiveInt.tryCreate(2), NonNegativeLong.tryCreate(10L), t2)

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

      "set and get the initial timestamp" in {
        val store = mk()

        for {
          // We should not really ever set the initial timestamp twice, but if we do make sure
          // we take the highest one
          _ <- store.setInitialTimestamp(t1)
          _ <- store.setInitialTimestamp(t0)
          t1get <- store.getInitialTimestamp
        } yield {
          t1get shouldBe Some(t1)
        }
      }

      "return latest balances at given timestamp" in {
        val store = mk()

        val aliceBalances = Seq(
          TrafficPurchased(alice.member, PositiveInt.one, NonNegativeLong.tryCreate(5L), t1),
          TrafficPurchased(
            alice.member,
            PositiveInt.tryCreate(2),
            NonNegativeLong.tryCreate(55L),
            t3,
          ),
        )
        val bobBalances = Seq(
          TrafficPurchased(bob.member, PositiveInt.one, NonNegativeLong.tryCreate(10L), t2),
          TrafficPurchased(
            bob.member,
            PositiveInt.tryCreate(2),
            NonNegativeLong.tryCreate(100L),
            t4,
          ),
        )

        val expectedBalancesAtT1 = Seq(aliceBalances(0))
        val expectedBalancesAtT2 = Seq(aliceBalances(0), bobBalances(0))
        val expectedBalancesAtT3 = Seq(aliceBalances(1), bobBalances(0))
        val expectedBalancesAtT4 = Seq(aliceBalances(1), bobBalances(1))

        for {
          _ <- (aliceBalances ++ bobBalances).parTraverse(store.store(_))
          balancesAtT0 <- store.lookupLatestBeforeInclusive(t0)
          balancesAtT1 <- store.lookupLatestBeforeInclusive(t1)
          balancesAtT2 <- store.lookupLatestBeforeInclusive(t2)
          balancesAtT2_5 <- store.lookupLatestBeforeInclusive(t2.plusMillis(500))
          balancesAtT3 <- store.lookupLatestBeforeInclusive(t3)
          balancesAtT3_5 <- store.lookupLatestBeforeInclusive(t3.plusMillis(500))
          balancesAtT4 <- store.lookupLatestBeforeInclusive(t4)
          balancesAtT4_5 <- store.lookupLatestBeforeInclusive(t4.plusMillis(500))
        } yield {
          balancesAtT0 shouldBe Seq.empty

          balancesAtT1 should contain theSameElementsAs expectedBalancesAtT1
          balancesAtT2 should contain theSameElementsAs expectedBalancesAtT2
          balancesAtT2_5 should contain theSameElementsAs expectedBalancesAtT2
          balancesAtT3 should contain theSameElementsAs expectedBalancesAtT3
          balancesAtT3_5 should contain theSameElementsAs expectedBalancesAtT3
          balancesAtT4 should contain theSameElementsAs expectedBalancesAtT4
          balancesAtT4_5 should contain theSameElementsAs expectedBalancesAtT4
        }
      }
    }
  }
}
