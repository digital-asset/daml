// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import cats.syntax.parallel.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.traffic.TrafficConsumed
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AsyncWordSpec

trait TrafficConsumedStoreTest
    extends BeforeAndAfterAll
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec {
  this: AsyncWordSpec =>

  def trafficConsumedStore(mk: () => TrafficConsumedStore): Unit = {
    val alice = ParticipantId("alice")
    val bob = ParticipantId("bob")
    val t0 = CantonTimestamp.Epoch
    val t1 = t0.plusSeconds(1)
    val t2 = t1.plusSeconds(1)
    val t3 = t2.plusSeconds(1)
    val t4 = t3.plusSeconds(1)

    val consumedAlice1 =
      TrafficConsumed(
        alice.member,
        t1,
        NonNegativeLong.tryCreate(3),
        NonNegativeLong.tryCreate(20L),
      )
    val consumedAlice2 = consumedAlice1.copy(sequencingTimestamp = t2)
    val consumedAlice3 = consumedAlice1.copy(sequencingTimestamp = t3)
    val consumedBob1 =
      TrafficConsumed(
        bob.member,
        t1,
        NonNegativeLong.tryCreate(3),
        NonNegativeLong.tryCreate(20L),
      )
    val consumedBob2 = consumedBob1.copy(sequencingTimestamp = t2)
    val consumedBob3 = consumedBob1.copy(sequencingTimestamp = t3)

    "trafficConsumedStore" should {
      "store and lookup traffic consumed at a specific timestamp" in {
        val store = mk()
        for {
          _ <- store.store(consumedAlice1)
          _ <- store.store(consumedBob1)
          alice1 <- store.lookupAt(alice, t1)
          alice2 <- store.lookupAt(alice, t2)
          bob1 <- store.lookupAt(bob, t1)
        } yield {
          alice1 shouldBe Some(consumedAlice1)
          alice2 shouldBe None
          bob1 shouldBe Some(consumedBob1)
        }
      }

      "store and lookup last entry for a member" in {
        val store = mk()
        for {
          _ <- store.store(consumedAlice1)
          _ <- store.store(consumedAlice2)
          alice1 <- store.lookupLast(alice)
        } yield {
          alice1 shouldBe Some(consumedAlice2)
        }
      }

      "store and lookup last entry for a member below a timestamp" in {
        val store = mk()
        for {
          _ <- store.store(consumedAlice1)
          _ <- store.store(consumedAlice2)
          alice1 <- store.lookupLatestBeforeInclusiveForMember(alice, t3)
        } yield {
          alice1 shouldBe Some(consumedAlice2)
        }
      }

      "store and lookup latest entries before timestamp for all members" in {
        val store = mk()
        for {
          _ <- store.store(consumedAlice1)
          _ <- store.store(consumedAlice2)
          _ <- store.store(consumedAlice3)
          _ <- store.store(consumedBob1)
          _ <- store.store(consumedBob2)
          _ <- store.store(consumedBob3)
          res <- store.lookupLatestBeforeInclusive(t2)
        } yield {
          res should contain theSameElementsAs List(consumedAlice2, consumedBob2)
        }
      }

      "be idempotent if inserting the same consumed twice" in {
        val store = mk()
        for {
          _ <- store.store(consumedAlice1)
          _ <- store.store(consumedAlice1)
          alice1 <- store.lookupAt(alice, t1)
        } yield {
          alice1 shouldBe Some(consumedAlice1)
        }
      }

      "remove all balances below a given timestamp, keeping the closest one < below it" in {
        val store = mk()
        // Between t2 and t3
        val t2point5 = t2.plusMillis(500)
        for {
          _ <- store.store(consumedAlice1)
          _ <- store.store(consumedBob1)
          _ <- store.store(consumedAlice2)
          _ <- store.store(consumedAlice3)
          _ <- store.pruneBelowExclusive(t2point5)
          aliceConsumed <- store.lookup(alice)
          bobConsumed <- store.lookup(bob)
        } yield {
          aliceConsumed should contain theSameElementsInOrderAs List(consumedAlice2, consumedAlice3)
          // We should keep bob's consumed entry because it's the only one below 2.5
          bobConsumed should contain theSameElementsInOrderAs List(consumedBob1)
        }
      }

      "remove all balances below a given timestamp for which there is an update" in {
        val store = mk()
        for {
          _ <- store.store(consumedAlice1)
          _ <- store.store(consumedAlice2)
          _ <- store.store(consumedAlice3)
          _ <- store.store(consumedBob1)
          _ <- store.store(consumedBob2)
          _ <- store.store(consumedBob3)
          _ <- store.pruneBelowExclusive(t2)
          aliceConsumed <- store.lookup(alice)
          bobConsumed <- store.lookup(bob)
        } yield {
          aliceConsumed should contain theSameElementsInOrderAs List(consumedAlice2, consumedAlice3)
          bobConsumed should contain theSameElementsInOrderAs List(consumedBob2, consumedBob3)
        }
      }

      "keep the latest balance if they're all in the pruning window" in {
        val store = mk()
        for {
          _ <- store.store(consumedAlice1)
          _ <- store.store(consumedAlice2)
          _ <- store.store(consumedAlice3)
          _ <- store.store(consumedBob1)
          _ <- store.store(consumedBob2)
          _ <- store.store(consumedBob3)
          _ <- store.pruneBelowExclusive(t3.plusSeconds(1))
          res <- store.lookupLatestBeforeInclusive(t3)
        } yield {
          res should contain theSameElementsInOrderAs List(consumedAlice3, consumedBob3)
        }
      }

      "return latest balances at given timestamp" in {
        val store = mk()

        val aliceConsumed = Seq(
          TrafficConsumed(alice.member, t1, NonNegativeLong.one, NonNegativeLong.tryCreate(5L)),
          TrafficConsumed(
            alice.member,
            t3,
            NonNegativeLong.tryCreate(2),
            NonNegativeLong.tryCreate(55L),
          ),
        )
        val bobConsumed = Seq(
          TrafficConsumed(bob.member, t2, NonNegativeLong.one, NonNegativeLong.tryCreate(10L)),
          TrafficConsumed(
            bob.member,
            t4,
            NonNegativeLong.tryCreate(2),
            NonNegativeLong.tryCreate(100L),
          ),
        )

        val expectedConsumedAtT1 = Seq(aliceConsumed(0))
        val expectedConsumedAtT2 = Seq(aliceConsumed(0), bobConsumed(0))
        val expectedConsumedAtT3 = Seq(aliceConsumed(1), bobConsumed(0))
        val expectedConsumedAtT4 = Seq(aliceConsumed(1), bobConsumed(1))

        for {
          _ <- (aliceConsumed ++ bobConsumed).parTraverse(store.store(_))
          consumedAtT0 <- store.lookupLatestBeforeInclusive(t0)
          consumedAtT1 <- store.lookupLatestBeforeInclusive(t1)
          consumedAtT2 <- store.lookupLatestBeforeInclusive(t2)
          consumedAtT2_5 <- store.lookupLatestBeforeInclusive(t2.plusMillis(500))
          consumedAtT3 <- store.lookupLatestBeforeInclusive(t3)
          consumedAtT3_5 <- store.lookupLatestBeforeInclusive(t3.plusMillis(500))
          consumedAtT4 <- store.lookupLatestBeforeInclusive(t4)
          consumedAtT4_5 <- store.lookupLatestBeforeInclusive(t4.plusMillis(500))
        } yield {
          consumedAtT0 shouldBe Seq.empty

          consumedAtT1 should contain theSameElementsAs expectedConsumedAtT1
          consumedAtT2 should contain theSameElementsAs expectedConsumedAtT2
          consumedAtT2_5 should contain theSameElementsAs expectedConsumedAtT2
          consumedAtT3 should contain theSameElementsAs expectedConsumedAtT3
          consumedAtT3_5 should contain theSameElementsAs expectedConsumedAtT3
          consumedAtT4 should contain theSameElementsAs expectedConsumedAtT4
          consumedAtT4_5 should contain theSameElementsAs expectedConsumedAtT4
        }
      }
    }
  }
}
