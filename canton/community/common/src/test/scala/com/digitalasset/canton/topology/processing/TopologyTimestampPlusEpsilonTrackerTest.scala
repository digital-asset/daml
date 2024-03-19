// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.store.{TopologyStoreId, ValidatedTopologyTransactionX}
import com.digitalasset.canton.topology.transaction.DomainParametersStateX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingOwnerWithKeysX}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.Outcome
import org.scalatest.wordspec.FixtureAnyWordSpec

import scala.concurrent.Future

class TopologyTimestampPlusEpsilonTrackerTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext {

  import com.digitalasset.canton.topology.client.EffectiveTimeTestHelpers.*

  protected class Fixture {
    val crypto = new TestingOwnerWithKeysX(
      DefaultTestIdentities.domainManager,
      loggerFactory,
      parallelExecutionContext,
    )
    val store = new InMemoryTopologyStoreX(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
      timeouts,
    )
    val tracker =
      new TopologyTimestampPlusEpsilonTracker(
        DefaultProcessingTimeouts.testing,
        loggerFactory,
        FutureSupervisor.Noop,
      )

    def appendEps(
        sequenced: SequencedTime,
        effective: EffectiveTime,
        topologyChangeDelay: NonNegativeFiniteDuration,
    ): Unit = {
      val tx = crypto.mkAdd(
        DomainParametersStateX(
          DefaultTestIdentities.domainId,
          DynamicDomainParameters.initialValues(
            topologyChangeDelay,
            testedProtocolVersion,
          ),
        ),
        crypto.SigningKeys.key1,
      )
      append(sequenced, effective, tx)
    }

    def append(
        sequenced: SequencedTime,
        effective: EffectiveTime,
        txs: GenericSignedTopologyTransactionX*
    ): Unit = {
      logger.debug(s"Storing $sequenced $effective $txs")
      store
        .update(
          sequenced,
          effective,
          removeMapping = Map.empty,
          removeTxs = Set.empty,
          txs.map(ValidatedTopologyTransactionX(_, None)).toList,
        )
        .futureValue
    }

    def initTracker(ts: CantonTimestamp): Unit = {
      unwrap(TopologyTimestampPlusEpsilonTracker.initializeX(tracker, store, ts)).futureValue
    }

    def init(): Unit = {
      val myTs = ts.immediatePredecessor
      val topologyChangeDelay = epsilonFD
      appendEps(
        SequencedTime(myTs),
        EffectiveTime(myTs),
        topologyChangeDelay,
      )
      initTracker(myTs)
    }
  }

  type FixtureParam = Fixture

  override protected def withFixture(test: OneArgTest): Outcome = {
    test(new Fixture)
  }

  private def unwrap[T](fut: FutureUnlessShutdown[T]): Future[T] =
    fut.onShutdown(fail("should not receive a shutdown"))

  private lazy val ts = CantonTimestamp.Epoch
  private lazy val epsilonFD = NonNegativeFiniteDuration.tryOfMillis(250)
  private lazy val epsilon = epsilonFD.duration

  private def assertEffectiveTimeForUpdate(
      tracker: TopologyTimestampPlusEpsilonTracker,
      ts: CantonTimestamp,
      expected: CantonTimestamp,
  ) = {
    val eff = unwrap(tracker.adjustTimestampForUpdate(ts)).futureValue
    eff.value shouldBe expected
    tracker.effectiveTimeProcessed(eff)
  }

  private def adjustEpsilon(
      tracker: TopologyTimestampPlusEpsilonTracker,
      newEpsilon: NonNegativeFiniteDuration,
  ) = {

    val adjustedTs = unwrap(tracker.adjustTimestampForUpdate(ts)).futureValue
    tracker.adjustEpsilon(adjustedTs, ts, newEpsilon)
    tracker.effectiveTimeProcessed(adjustedTs)

    // until adjustedTs, we should still get the old epsilon
    forAll(Seq(1L, 100L, 250L)) { delta =>
      val ts1 = ts.plusMillis(delta)
      assertEffectiveTimeForUpdate(tracker, ts1, ts1.plus(epsilon))
    }

  }

  "timestamp plus epsilon" should {

    "epsilon is constant properly project the timestamp" in { f =>
      import f.*
      init()
      assertEffectiveTimeForUpdate(tracker, ts, ts.plus(epsilon))
      assertEffectiveTimeForUpdate(tracker, ts.plusSeconds(5), ts.plusSeconds(5).plus(epsilon))
      unwrap(
        tracker
          .adjustTimestampForTick(ts.plusSeconds(5))
      ).futureValue.value shouldBe ts.plusSeconds(5).plus(epsilon)

    }

    "properly project during epsilon increase" in { f =>
      import f.*
      init()
      val newEpsilon = NonNegativeFiniteDuration.tryOfSeconds(1)
      adjustEpsilon(tracker, newEpsilon)
      // after adjusted ts, we should get the new epsilon
      forAll(Seq(0L, 100L, 250L)) { delta =>
        val ts1 = ts.plus(epsilon).immediateSuccessor.plusMillis(delta)
        assertEffectiveTimeForUpdate(tracker, ts1, ts1.plus(newEpsilon.duration))
      }
    }

    "properly deal with epsilon decrease" in { f =>
      import f.*
      init()
      val newEpsilon = NonNegativeFiniteDuration.tryOfMillis(100)
      adjustEpsilon(tracker, newEpsilon)

      // after adjusted ts, we should get the new epsilon, avoiding the "deadzone" of ts + 2*oldEpsilon
      forAll(Seq(150L, 250L, 500L)) { case delta =>
        val ts1 = ts.plus(epsilon).immediateSuccessor.plusMillis(delta)
        assertEffectiveTimeForUpdate(tracker, ts1, ts1.plus(newEpsilon.duration))
      }
    }

    "correctly initialise with pending changes" in { f =>
      import f.*

      val eS1 = NonNegativeFiniteDuration.tryOfMillis(100)
      val tsS1 = ts.minusSeconds(1)
      appendEps(SequencedTime(tsS1), EffectiveTime(tsS1), eS1)

      val eM100 = NonNegativeFiniteDuration.tryOfMillis(100)
      val tsM100 = ts.minusMillis(100)

      appendEps(SequencedTime(tsM100), EffectiveTime(ts), eM100)

      val tsM75 = ts.minusMillis(75)
      append(
        SequencedTime(ts),
        EffectiveTime(tsM75.plusMillis(100)),
        f.crypto.TestingTransactions.ns1k2,
        f.crypto.TestingTransactions.p1p1,
      )

      val eM50 = NonNegativeFiniteDuration.tryOfMillis(200)
      val tsM50 = ts.minusMillis(50)
      appendEps(SequencedTime(tsM50), EffectiveTime(tsM50.plusMillis(100)), eM50)

      f.initTracker(ts)

      assertEffectiveTimeForUpdate(tracker, ts.plusMillis(10), ts.plusMillis(110))

    }

    "gracefully deal with epsilon decrease on a buggy domain" in { f =>
      import f.*
      init()
      val newEpsilon = NonNegativeFiniteDuration.tryOfMillis(100)
      adjustEpsilon(tracker, newEpsilon)

      // after adjusted ts, we should get a warning and an adjustment in the "deadzone" of ts + 2*oldEpsilon
      val (_, prepared) = Seq(0L, 100L, 149L).foldLeft(
        (ts.plus(epsilon).plus(epsilon).immediateSuccessor, Seq.empty[(CantonTimestamp, Long)])
      ) { case ((expected, res), delta) =>
        // so expected timestamp is at 2*epsilon
        (expected.immediateSuccessor, res :+ ((expected, delta)))
      }
      forAll(prepared) { case (expected, delta) =>
        val ts1 = ts.plus(epsilon).immediateSuccessor.plusMillis(delta)
        // tick should not advance the clock
        unwrap(
          tracker.adjustTimestampForTick(ts1)
        ).futureValue.value shouldBe expected.immediatePredecessor
        // update should advance the clock
        val res = loggerFactory.assertLogs(
          unwrap(tracker.adjustTimestampForUpdate(ts1)).futureValue,
          _.errorMessage should include("Broken or malicious domain"),
        )
        res.value shouldBe expected
        tracker.effectiveTimeProcessed(res)
      }
    }

    "block until we are in sync again" in { f =>
      import f.*
      init()
      val eps3 = epsilon.dividedBy(3)
      val eps2 = epsilon.dividedBy(2)
      epsilon.toMillis shouldBe 250 // this test assumes this
      // first, we'll kick off the computation at ts (note epsilon is active for ts on)
      val fut1 = unwrap(tracker.adjustTimestampForUpdate(ts)).futureValue
      // now, we need to confirm this effective time, which will "unlock" the any sequenced event update
      // within ts + eps
      tracker.effectiveTimeProcessed(fut1)
      // then, we tick another update with a third and with half epsilon
      val fut2 = unwrap(tracker.adjustTimestampForUpdate(ts.plus(eps3))).futureValue
      val fut3 = unwrap(tracker.adjustTimestampForUpdate(ts.plus(eps2))).futureValue
      // if we didn't get stuck by now, we didn't get blocked. now, let's get futures that gets blocked
      val futB = unwrap(tracker.adjustTimestampForUpdate(ts.plus(epsilon).plusSeconds(1)))
      val futB1 = unwrap(tracker.adjustTimestampForUpdate(ts.plus(epsilon).plusMillis(1001)))
      val futB2 = unwrap(tracker.adjustTimestampForUpdate(ts.plus(epsilon).plusMillis(1200)))
      val futC = unwrap(tracker.adjustTimestampForTick(ts.plus(epsilon).plusSeconds(2)))
      Threading.sleep(500) // just to be sure that the future truly is blocked
      futB.isCompleted shouldBe false
      futB1.isCompleted shouldBe false
      futB2.isCompleted shouldBe false
      futC.isCompleted shouldBe false
      // now, release the first one, we should complete the first, but not the second
      Seq(fut2, fut3, fut1).foreach(tracker.effectiveTimeProcessed)
      val effB = futB.futureValue
      // now, clearing the effB should release B1 and B2, but not C, as C must wait until B2 is finished
      // as there might have been an epsilon change
      tracker.effectiveTimeProcessed(effB)
      // the one following within epsilon should complete as well
      val effB1 = futB1.futureValue
      val effB2 = futB2.futureValue
      // finally, release
      Seq(effB1, effB2).foreach { ts =>
        Threading.sleep(500) // just to be sure that the future is truly blocked
        // should only complete after we touched the last one
        futC.isCompleted shouldBe false
        tracker.effectiveTimeProcessed(ts)
      }
      // finally, should complete now
      futC.futureValue
    }
  }

}
