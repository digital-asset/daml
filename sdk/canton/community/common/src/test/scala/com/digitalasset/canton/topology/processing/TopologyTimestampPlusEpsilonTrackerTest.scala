// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStore
import com.digitalasset.canton.topology.store.{
  TopologyStoreId,
  TopologyTransactionRejection,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{DefaultTestIdentities, TestingOwnerWithKeys}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalactic.source
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.scalatest.{Assertion, Outcome}

class TopologyTimestampPlusEpsilonTrackerTest
    extends FixtureAnyWordSpec
    with BaseTest
    with HasExecutionContext {

  protected class Fixture {
    val crypto = new TestingOwnerWithKeys(
      DefaultTestIdentities.sequencerId,
      loggerFactory,
      parallelExecutionContext,
    )
    val store = new InMemoryTopologyStore(
      TopologyStoreId.DomainStore(DefaultTestIdentities.domainId),
      loggerFactory,
      timeouts,
    )

    var tracker: TopologyTimestampPlusEpsilonTracker = _
    reInit()

    def reInit(): Unit =
      tracker = new TopologyTimestampPlusEpsilonTracker(
        store,
        DefaultProcessingTimeouts.testing,
        loggerFactory,
      )

    def commitChangeDelay(sequenced: Long, effective: Long, topologyChangeDelay: Long): Unit = {
      val sequencedTimeTyped = SequencedTime(CantonTimestamp.ofEpochMicro(sequenced))
      val effectiveTimeTyped = EffectiveTime(CantonTimestamp.ofEpochMicro(effective))
      val topologyChangeDelayTyped = NonNegativeFiniteDuration.tryOfMicros(topologyChangeDelay)

      tracker.adjustTopologyChangeDelay(
        effectiveTimeTyped,
        topologyChangeDelayTyped,
      )
      storeChangeDelay(
        sequencedTimeTyped,
        effectiveTimeTyped,
        topologyChangeDelayTyped,
      )
    }

    def storeChangeDelay(
        sequenced: SequencedTime,
        effective: EffectiveTime,
        topologyChangeDelay: NonNegativeFiniteDuration,
    ): Unit = {
      val tx = crypto.mkAdd(
        DomainParametersState(
          DefaultTestIdentities.domainId,
          DynamicDomainParameters.initialValues(
            topologyChangeDelay,
            testedProtocolVersion,
          ),
        ),
        crypto.SigningKeys.key1,
      )
      store
        .update(
          sequenced,
          effective,
          removeMapping = Map(tx.mapping.uniqueKey -> PositiveInt.one),
          removeTxs = Set.empty,
          List(ValidatedTopologyTransaction(tx, None)),
        )
        .futureValue
    }

    def storeRejection(sequenced: Long, effective: Long): Unit = {

      val tx = ValidatedTopologyTransaction(
        crypto.TestingTransactions.p1p1,
        Some(TopologyTransactionRejection.NotAuthorized),
      )

      store
        .update(
          SequencedTime(CantonTimestamp.ofEpochMicro(sequenced)),
          EffectiveTime(CantonTimestamp.ofEpochMicro(effective)),
          removeMapping = Map.empty,
          removeTxs = Set.empty,
          Seq(tx),
        )
        .futureValue
    }

    def assertEffectiveTime(
        sequenced: Long,
        strictMonotonicity: Boolean,
        expectedEffective: Long,
    )(implicit pos: source.Position): Assertion =
      tracker
        .trackAndComputeEffectiveTime(
          SequencedTime(CantonTimestamp.ofEpochMicro(sequenced)),
          strictMonotonicity,
        )
        .futureValueUS
        .value shouldBe CantonTimestamp.ofEpochMicro(expectedEffective)
  }

  type FixtureParam = Fixture

  override protected def withFixture(test: OneArgTest): Outcome = test(new Fixture)

  "The tracker" should {

    "correctly compute effective times with constant topologyChangeDelay" in { f =>
      import f.*
      commitChangeDelay(-1, -1, 250)

      assertEffectiveTime(0, strictMonotonicity = true, 250)
      assertEffectiveTime(5, strictMonotonicity = true, 255)
      assertEffectiveTime(5, strictMonotonicity = false, 255)
    }

    "correctly compute effective times when the topologyChangeDelay increases" in { f =>
      import f.*
      // initialize delay
      commitChangeDelay(-1, -1, 250)

      // increase delay
      assertEffectiveTime(0, strictMonotonicity = true, 250)
      commitChangeDelay(0, 250, 1000)

      // until 250, we should get the old delay
      assertEffectiveTime(1, strictMonotonicity = true, 251)
      assertEffectiveTime(100, strictMonotonicity = true, 350)
      assertEffectiveTime(250, strictMonotonicity = true, 500)

      // after 250, we should get the new delay
      assertEffectiveTime(251, strictMonotonicity = true, 1251)
      assertEffectiveTime(260, strictMonotonicity = true, 1260)
      assertEffectiveTime(350, strictMonotonicity = true, 1350)
      assertEffectiveTime(500, strictMonotonicity = true, 1500)
    }

    "correctly compute effective times when the topologyChangeDelay decreases" in { f =>
      import f.*

      // initialize delay
      commitChangeDelay(-1, -1, 250)

      // increase delay
      assertEffectiveTime(0, strictMonotonicity = true, 250)
      commitChangeDelay(0, 250, 100)

      // until 250, we should get the old delay
      assertEffectiveTime(1, strictMonotonicity = false, 251)
      assertEffectiveTime(100, strictMonotonicity = false, 350)
      assertEffectiveTime(250, strictMonotonicity = false, 500)

      // after 250, we should get the new delay, but with corrections to guarantee monotonicity
      assertEffectiveTime(251, strictMonotonicity = false, 500)
      assertEffectiveTime(252, strictMonotonicity = false, 500)
      assertEffectiveTime(253, strictMonotonicity = true, 501)
      assertEffectiveTime(254, strictMonotonicity = false, 501)
      assertEffectiveTime(300, strictMonotonicity = false, 501)
      assertEffectiveTime(300, strictMonotonicity = true, 502)

      // after 402, we should get the new delay without corrections
      assertEffectiveTime(403, strictMonotonicity = true, 503)
      assertEffectiveTime(404, strictMonotonicity = false, 504)
      assertEffectiveTime(410, strictMonotonicity = true, 510)
      assertEffectiveTime(500, strictMonotonicity = false, 600)
      assertEffectiveTime(600, strictMonotonicity = true, 700)
    }

    "initialization should load upcoming epsilon changes" in { f =>
      import f.*

      // Commit a series of changes and check effective times.
      assertEffectiveTime(0, strictMonotonicity = true, 0)
      commitChangeDelay(0, 0, 100) // delay1
      assertEffectiveTime(10, strictMonotonicity = true, 110)
      commitChangeDelay(10, 110, 110) // delay2
      assertEffectiveTime(100, strictMonotonicity = false, 200)
      assertEffectiveTime(111, strictMonotonicity = true, 221)
      storeRejection(111, 221)
      assertEffectiveTime(120, strictMonotonicity = true, 230)
      commitChangeDelay(120, 230, 120) // delay3
      assertEffectiveTime(231, strictMonotonicity = false, 351)

      // Now re-initialize tracker and check if up-coming changes are loaded from store
      reInit()
      // This will initialize the tracker to sequencedTime = 100, i.e. delay1 is effective, delay2 is upcoming, and delay3 not yet processed.
      // delay1 should be loaded from the store, as it is effective
      assertEffectiveTime(100, strictMonotonicity = false, 200)
      // delay2 should be loaded from the store, as it has been upcoming during initialization
      assertEffectiveTime(111, strictMonotonicity = true, 221)
      storeRejection(111, 221)
      assertEffectiveTime(120, strictMonotonicity = true, 230)
      // delay3 needs to be replayed as its sequencing time is after the init time of 100
      commitChangeDelay(120, 230, 120)
      assertEffectiveTime(231, strictMonotonicity = false, 351)
    }

    "initialization should load upcoming transactions (including rejections)" in { f =>
      import f.*

      assertEffectiveTime(0, strictMonotonicity = true, 0)
      commitChangeDelay(0, 0, 100) // set initial delay1
      assertEffectiveTime(10, strictMonotonicity = true, 110)
      commitChangeDelay(10, 110, 50) // decrease delay to delay2

      // delay1 is still effective
      assertEffectiveTime(110, strictMonotonicity = true, 210)
      storeRejection(110, 210)

      // delay2 is now effective, but the effective time is corrected
      assertEffectiveTime(111, strictMonotonicity = true, 211)
      storeRejection(111, 211)
      assertEffectiveTime(120, strictMonotonicity = true, 212)
      storeRejection(120, 212)
      assertEffectiveTime(130, strictMonotonicity = false, 212)
      // delay2 is now effective without any correction
      assertEffectiveTime(164, strictMonotonicity = true, 214)
      storeRejection(164, 214)

      // Now re-initialize and check if previous transactions are reloaded
      reInit()
      // delay2 is already effective, but the effective time is corrected
      assertEffectiveTime(130, strictMonotonicity = false, 212)
      // delay2 is now effective without any correction
      assertEffectiveTime(164, strictMonotonicity = true, 214)
      storeRejection(164, 214)
    }

    "initialization should load expired domainParametersChanges" in { f =>
      import f.*

      assertEffectiveTime(0, strictMonotonicity = true, 0)
      commitChangeDelay(0, 0, 100) // set initial delay1
      assertEffectiveTime(10, strictMonotonicity = true, 110)
      commitChangeDelay(10, 110, 50) // decrease delay to delay2

      // delay1 is still effective
      assertEffectiveTime(110, strictMonotonicity = false, 210)
      // delay2 is now effective, but the effective time is corrected
      assertEffectiveTime(120, strictMonotonicity = false, 210)
      // delay2 is now effective without any correction
      assertEffectiveTime(162, strictMonotonicity = false, 212)

      // Now re-initialize and check if the expiry of delay1 is reloaded
      reInit()
      assertEffectiveTime(120, strictMonotonicity = false, 210)
      // delay2 is now effective without any correction
      assertEffectiveTime(162, strictMonotonicity = false, 212)
    }
  }

}
