// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.traffic.store

import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.traffic.TopUpEvent
import com.digitalasset.canton.{BaseTest, ProtocolVersionChecksAsyncWordSpec}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import org.scalatest.{BeforeAndAfterAll, EitherValues, OptionValues}

trait TrafficLimitsStoreTest
    extends BeforeAndAfterAll
    with EitherValues
    with BaseTest
    with ProtocolVersionChecksAsyncWordSpec {
  this: AsyncWordSpec with Matchers with OptionValues =>

  def trafficLimitsStore(mk: () => TrafficLimitsStore): Unit = {
    val alice = ParticipantId("alice")
    val bob = ParticipantId("bob")
    val t1 = CantonTimestamp.Epoch
    val t2 = t1.plusSeconds(1)
    val t3 = t2.plusSeconds(1)

    "updateTotalExtraTrafficLimit" should {
      "add top ups" in {
        val store = mk()
        val topUpEventAlice = TopUpEvent(PositiveLong.tryCreate(5L), t1, PositiveInt.tryCreate(1))
        val topUpEventAlice2 = TopUpEvent(PositiveLong.tryCreate(9L), t2, PositiveInt.tryCreate(2))
        val topUpEventBob = TopUpEvent(PositiveLong.tryCreate(6L), t1, PositiveInt.tryCreate(3))
        for {
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice,
              bob -> topUpEventBob,
            )
          )
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice2
            )
          )
          aliceEvents <- store.getExtraTrafficLimits(alice)
          bobEvents <- store.getExtraTrafficLimits(bob)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            topUpEventAlice,
            topUpEventAlice2,
          )
          bobEvents should contain theSameElementsInOrderAs List(topUpEventBob)
        }
      }

      "be idempotent if inserting the same top up twice" in {
        val store = mk()
        val topUpEventAlice = TopUpEvent(PositiveLong.tryCreate(5L), t1, PositiveInt.tryCreate(1))
        for {
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice
            )
          )
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice
            )
          )
          aliceEvents <- store.getExtraTrafficLimits(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            topUpEventAlice
          )
        }
      }

      "add top ups with same effective timestamps" in {
        val store = mk()
        val topUpEventAlice = TopUpEvent(PositiveLong.tryCreate(5L), t1, PositiveInt.tryCreate(1))
        val topUpEventAlice2 = TopUpEvent(PositiveLong.tryCreate(50L), t1, PositiveInt.tryCreate(2))
        for {
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice2
            )
          )
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice
            )
          )
          aliceEvents <- store.getExtraTrafficLimits(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            topUpEventAlice,
            topUpEventAlice2,
          )
        }
      }

      "insert top ups out of order but get them back in the right order" in {
        val store = mk()
        val topUpEventAlice = TopUpEvent(PositiveLong.tryCreate(5L), t1, PositiveInt.tryCreate(1))
        val topUpEventAlice2 = TopUpEvent(PositiveLong.tryCreate(50L), t2, PositiveInt.tryCreate(2))
        for {
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice2
            )
          )
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice
            )
          )
          aliceEvents <- store.getExtraTrafficLimits(alice)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            topUpEventAlice,
            topUpEventAlice2,
          )
        }
      }

      "fail to insert 2 top ups with the same sequencer counter but different limits" in {
        val store = mk()
        val topUpEventAlice = TopUpEvent(PositiveLong.tryCreate(5L), t1, PositiveInt.tryCreate(1))
        val topUpEventAlice2 = TopUpEvent(PositiveLong.tryCreate(9L), t2, PositiveInt.tryCreate(1))

        val res = loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            for {
              _ <- store.updateTotalExtraTrafficLimit(
                Map(
                  alice -> topUpEventAlice
                )
              )
              _ <- store.updateTotalExtraTrafficLimit(
                Map(
                  alice -> topUpEventAlice2
                )
              )
            } yield ()
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.errorMessage should include("has existing extra_traffic_limit value of"),
                "expected logged DB failure",
              )
            )
          ),
        )
        recoverToSucceededIf[IllegalStateException](res)
      }
    }

    "pruneBelowCounter" should {
      "remove all events below a given sequencer counter" in {
        val store = mk()
        val topUpEventAlice = TopUpEvent(PositiveLong.tryCreate(5L), t1, PositiveInt.tryCreate(1))
        val topUpEventAlice2 = TopUpEvent(PositiveLong.tryCreate(9L), t2, PositiveInt.tryCreate(2))
        val topUpEventAlice3 = TopUpEvent(PositiveLong.tryCreate(10L), t3, PositiveInt.tryCreate(3))
        val topUpEventBob = TopUpEvent(PositiveLong.tryCreate(6L), t1, PositiveInt.tryCreate(3))
        for {
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice,
              bob -> topUpEventBob,
            )
          )
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice2
            )
          )
          _ <- store.updateTotalExtraTrafficLimit(
            Map(
              alice -> topUpEventAlice3
            )
          )
          _ <- store.pruneBelowSerial(alice, PositiveInt.tryCreate(2))
          aliceEvents <- store.getExtraTrafficLimits(alice)
          bobEvents <- store.getExtraTrafficLimits(bob)
        } yield {
          aliceEvents should contain theSameElementsInOrderAs List(
            topUpEventAlice2,
            topUpEventAlice3,
          )
          bobEvents should contain theSameElementsInOrderAs List(topUpEventBob)
        }
      }
    }
  }
}
