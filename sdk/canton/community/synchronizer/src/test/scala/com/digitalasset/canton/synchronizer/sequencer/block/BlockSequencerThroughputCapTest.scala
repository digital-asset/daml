// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.SubmissionRequestType
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.{
  IndividualThroughputCapConfig,
  ThroughputCapByMessageTypeConfig,
  ThroughputCapConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerThroughputCap.{
  IndividualBlockSequencerThroughputCap,
  SubmissionRequestEntry,
}
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerThroughputCapTest.*
import com.digitalasset.canton.synchronizer.sequencer.block.SchedulerTestUtil.mockScheduler
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration

class BlockSequencerThroughputCapTest extends AsyncWordSpec with BaseTest {

  private def createIndividualConfig(
      globalTpsCap: Double = defaultGlobalTpsCap,
      globalKbpsCap: Double = defaultGlobalKbpsCap,
      perClientTpsCap: Double = defaultPerClientTpsCap,
      perClientKbpsCap: Double = defaultPerClientKbpsCap,
  ): IndividualThroughputCapConfig = IndividualThroughputCapConfig(
    globalTpsCap = PositiveDouble.tryCreate(globalTpsCap),
    globalKbpsCap = PositiveDouble.tryCreate(globalKbpsCap),
    perClientTpsCap = PositiveDouble.tryCreate(perClientTpsCap),
    perClientKbpsCap = PositiveDouble.tryCreate(perClientKbpsCap),
  )

  private def createConfig(
      observationPeriodSeconds: Int,
      perClientTpsCap: Double = defaultPerClientTpsCap,
  ): ThroughputCapConfig =
    ThroughputCapConfig(
      enabled = true,
      observationPeriodSeconds,
      messages = ThroughputCapByMessageTypeConfig(
        confirmationRequest = createIndividualConfig(perClientTpsCap = perClientTpsCap),
        topology = createIndividualConfig(perClientTpsCap = perClientTpsCap),
      ),
    )

  "BlockSequencerThroughputCap" should {
    "have separate caps per message type and update based on scheduler" in {
      val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
      val observationPeriodSeconds = 1
      val cap = new BlockSequencerThroughputCap(
        createConfig(observationPeriodSeconds),
        clock,
        mockScheduler(clock),
        loggerFactory,
      )

      def confirmationRequest(ts: CantonTimestamp) =
        SubmissionRequestEntry(m1, SubmissionRequestType.ConfirmationRequest, ts, 1L)
      def topologyTransaction(ts: CantonTimestamp) =
        SubmissionRequestEntry(m1, SubmissionRequestType.TopologyTransaction, ts, 1L)
      def confirmationResponse(ts: CantonTimestamp) =
        SubmissionRequestEntry(m1, SubmissionRequestType.ConfirmationResponse, ts, 1L)

      def assertAllAreAccepted: Assertion = {
        cap.shouldRejectTransaction(SubmissionRequestType.ConfirmationRequest, m1, 0) shouldBe false
        cap.shouldRejectTransaction(SubmissionRequestType.TopologyTransaction, m1, 0) shouldBe false
        cap.shouldRejectTransaction(
          SubmissionRequestType.ConfirmationResponse,
          m1,
          0,
        ) shouldBe false
      }

      // initially all are accepted
      assertAllAreAccepted

      // initializing caps
      cap.addBlockUpdateInternal(Seq(confirmationRequest(clock.now)))
      cap.addBlockUpdateInternal(Seq(topologyTransaction(clock.now)))
      cap.addBlockUpdateInternal(Seq(confirmationResponse(clock.now)))
      clock.advance(Duration.ofSeconds(observationPeriodSeconds.toLong))
      assertAllAreAccepted
      // should now be initialized

      // after receiving more than 1 confirmation requests in a second, only confirmation requests are rejected
      cap.addBlockUpdateInternal(Seq(confirmationRequest(clock.now)))
      cap.addBlockUpdateInternal(Seq(confirmationRequest(clock.now)))
      cap.shouldRejectTransaction(SubmissionRequestType.ConfirmationRequest, m1, 0) shouldBe true
      cap.shouldRejectTransaction(SubmissionRequestType.TopologyTransaction, m1, 0) shouldBe false
      cap.shouldRejectTransaction(SubmissionRequestType.ConfirmationResponse, m1, 0) shouldBe false

      // after receiving more than 1 topology transaction requests in a second, they are also rejected
      cap.addBlockUpdateInternal(Seq(topologyTransaction(clock.now)))
      cap.addBlockUpdateInternal(Seq(topologyTransaction(clock.now)))
      cap.shouldRejectTransaction(SubmissionRequestType.TopologyTransaction, m1, 0) shouldBe true
      cap.shouldRejectTransaction(SubmissionRequestType.ConfirmationRequest, m1, 0) shouldBe true
      cap.shouldRejectTransaction(SubmissionRequestType.ConfirmationResponse, m1, 0) shouldBe false

      // confirmation responses (and all other message types) are not counted, so they don't get rejected
      cap.addBlockUpdateInternal(Seq(confirmationResponse(clock.now)))
      cap.addBlockUpdateInternal(Seq(confirmationResponse(clock.now)))
      cap.shouldRejectTransaction(SubmissionRequestType.TopologyTransaction, m1, 0) shouldBe true
      cap.shouldRejectTransaction(SubmissionRequestType.ConfirmationRequest, m1, 0) shouldBe true
      cap.shouldRejectTransaction(SubmissionRequestType.ConfirmationResponse, m1, 0) shouldBe false

      // after advancing, the scheduler takes care of updating the cap (so now submissions are all accepted again)
      clock.advance(Duration.ofSeconds(observationPeriodSeconds.toLong).plusMillis(1L))
      assertAllAreAccepted
    }
  }

  "IndividualBlockSequencerThroughputCap" should {

    "initialize the cap logic correctly" in {
      val config = createIndividualConfig()
      val observationPeriodSeconds = 1
      val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
      val tpsCap = new IndividualBlockSequencerThroughputCap(
        observationPeriodSeconds,
        config,
        clock,
        loggerFactory = loggerFactory,
      )
      val thresholdLevel = 0
      val eventSize: Long = 128

      // originally, initialized = false, so nothing should be rejected
      tpsCap.shouldRejectTransaction(m1, thresholdLevel) shouldBe false

      // add two events for m1, the second of which is directly at the observationPeriod
      tpsCap.addEvent(clock.now, m1, eventSize)
      clock.advance(Duration.ofSeconds(observationPeriodSeconds.toLong))
      tpsCap.addEvent(clock.now, m1, eventSize)

      // the second event is added but does not trigger a removal, and thus does not
      // cause initialized to be set to true
      tpsCap.shouldRejectTransaction(m1, thresholdLevel) shouldBe false

      // finally, initialized becomes true once > observationPeriod has been observed
      clock.advance(Duration.ofNanos(1000))
      tpsCap.addEvent(clock.now, m1, eventSize)
      tpsCap.shouldRejectTransaction(m1, thresholdLevel) shouldBe true
    }

    "advance window if no events arrive and enough time elapsed to run the scheduled tick" in {
      val observationPeriodSeconds = 1
      val config = createIndividualConfig()
      val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
      val tpsCap = new IndividualBlockSequencerThroughputCap(
        observationPeriodSeconds,
        config,
        clock,
        loggerFactory = loggerFactory,
      )
      val thresholdLevel = 0
      val eventSize: Long = 128

      // initialize the cap logic, and exceed the allotted rate of 1 TPS
      tpsCap.addEvent(clock.now, m1, eventSize)
      clock.advance(
        Duration.ofSeconds(observationPeriodSeconds.toLong).plus(Duration.ofNanos(1000))
      )
      tpsCap.addEvent(clock.now, m1, eventSize)
      tpsCap.addEvent(clock.now, m1, eventSize)
      tpsCap.shouldRejectTransaction(m1, thresholdLevel) shouldBe true

      // advance the clock to simulate time passing since the latest event was added
      clock.advance(Duration.ofSeconds(1).plus(Duration.ofMillis(1)))
      tpsCap.advanceWindow() // simulate the parent scheduler tick running
      tpsCap.shouldRejectTransaction(m1, thresholdLevel) shouldBe false

      // ensure that the cap is re-enforced once again once events are added
      clock.advance(Duration.ofNanos(1000))
      tpsCap.addEvent(clock.now, m1, eventSize)
      tpsCap.addEvent(clock.now, m1, eventSize)
      tpsCap.shouldRejectTransaction(m1, thresholdLevel) shouldBe true
    }

    "enforce threshold level caps from TPS increases" in {
      val observationPeriodSeconds = 1
      val config = createIndividualConfig()
      val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
      val tpsCap = new IndividualBlockSequencerThroughputCap(
        observationPeriodSeconds,
        config,
        clock,
        loggerFactory = loggerFactory,
      )
      val eventSize: Long = 128

      // initialize the tps cap logic
      tpsCap.addEvent(clock.now, m1, eventSize)
      clock.advance(
        Duration.ofSeconds(observationPeriodSeconds.toLong).plus(Duration.ofNanos(1000))
      )
      tpsCap.addEvent(clock.now, m1, eventSize)

      // total global utilization should be 10% right now: 1/10 from the above addEvent
      // insertion at any threshold level succeeds during unregulated operation
      tpsCap.shouldRejectTransaction(m2, 3) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 2) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 1) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 0) shouldBe false

      // insertion when utilization is in level 2 (>=70%) only works for levels <= 2
      (1 to 6).foreach(_ => tpsCap.addEvent(clock.now, m1, 1))
      tpsCap.shouldRejectTransaction(m2, 3) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 2) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 1) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 0) shouldBe false

      // insertion when utilization is in level 1 (>=80%) only works for levels <= 1
      tpsCap.addEvent(clock.now, m1, eventSize)
      tpsCap.shouldRejectTransaction(m2, 3) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 2) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 1) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 0) shouldBe false

      // insertion when utilization is in level 0 (>=90%) only works for levels <= 0
      tpsCap.addEvent(clock.now, m1, eventSize)
      tpsCap.shouldRejectTransaction(m2, 3) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 2) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 1) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 0) shouldBe false
    }

    "enforce threshold level caps from KB/s increases" in {
      val observationPeriodSeconds = 1
      val config = createIndividualConfig()
      val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
      val tpsCap = new IndividualBlockSequencerThroughputCap(
        observationPeriodSeconds,
        config,
        clock,
        loggerFactory = loggerFactory,
      )
      val bytesCap = config.globalKbpsCap.value * 1024 * observationPeriodSeconds

      // initialize the tps cap logic
      tpsCap.addEvent(clock.now, m1, 1)
      clock.advance(
        Duration.ofSeconds(observationPeriodSeconds.toLong).plus(Duration.ofNanos(1000))
      )

      // total global utilization should be 60% of Kbps cap from a single large insert
      tpsCap.addEvent(clock.now, m1, (bytesCap * 0.6).toLong)

      // insertion at any threshold level succeeds during unregulated operation
      tpsCap.shouldRejectTransaction(m2, 3) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 2) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 1) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 0) shouldBe false

      // insertion when utilization is in level 2 (>=70%) only works for levels <= 2
      tpsCap.addEvent(clock.now, m1, (bytesCap * 0.1).toLong)
      tpsCap.shouldRejectTransaction(m2, 3) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 2) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 1) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 0) shouldBe false

      // insertion when utilization is in level 1 (>=80%) only works for levels <= 1
      tpsCap.addEvent(clock.now, m1, (bytesCap * 0.1).toLong)
      tpsCap.shouldRejectTransaction(m2, 3) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 2) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 1) shouldBe false
      tpsCap.shouldRejectTransaction(m2, 0) shouldBe false

      // insertion when utilization is in level 0 (>=90%) only works for levels <= 0
      tpsCap.addEvent(clock.now, m1, (bytesCap * 0.1).toLong)
      tpsCap.shouldRejectTransaction(m2, 3) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 2) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 1) shouldBe true
      tpsCap.shouldRejectTransaction(m2, 0) shouldBe false
    }

    "enforce throttling when high TPS reaches highest threshold level" in {
      val observationPeriodSeconds = 1
      val config =
        createIndividualConfig(perClientTpsCap = defaultGlobalTpsCap * 0.4)
      val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
      val tpsCap = new IndividualBlockSequencerThroughputCap(
        observationPeriodSeconds,
        config,
        clock,
        loggerFactory = loggerFactory,
      )
      val eventSize: Long = 128

      // initialize the tps cap logic
      tpsCap.addEvent(clock.now, m1, eventSize)
      clock.advance(
        Duration.ofSeconds(observationPeriodSeconds.toLong).plus(Duration.ofMillis(1))
      )

      // put the cap logic into the highest (enforcement) level threshold (90%)
      val rounds = 3
      (1 to rounds).foreach { _ =>
        tpsCap.addEvent(clock.now, m1, eventSize)
        tpsCap.addEvent(clock.now, m2, eventSize)
        tpsCap.addEvent(clock.now, m3, eventSize)
        clock.advance(Duration.ofMillis(1))
      }
      tpsCap.getMemberUsage(m1).map(_.count) should contain(rounds)
      tpsCap.getMemberUsage(m2).map(_.count) should contain(rounds)
      tpsCap.getMemberUsage(m3).map(_.count) should contain(rounds)

      // w/ globalCap = 10 and vActive = 3, throttled rate per member is: 10 / (1 + 3) = 2.5
      // since m1 already has 3 events in the window, it will be denied service
      tpsCap.shouldRejectTransaction(m1, 0) shouldBe true
      // but m4 (w/ no usage) is allowed to send requests
      tpsCap.shouldRejectTransaction(m4, 0) shouldBe false

      tpsCap.addEvent(clock.now, m4, eventSize) // 100%, and vAct = 4, so fair share is 2.0
      tpsCap.addEvent(clock.now, m5, eventSize) // 110%, and vAct = 5, so fair share is 1.67
      tpsCap.shouldRejectTransaction(m4, 0) shouldBe false
      tpsCap.shouldRejectTransaction(m4, 0) shouldBe false
      tpsCap.addEvent(clock.now, m4, eventSize) // 120%
      tpsCap.getMemberUsage(m4).map(_.count) should contain(2)
      tpsCap.getMemberUsage(m5).map(_.count) should contain(1)
      tpsCap.shouldRejectTransaction(m4, 0) shouldBe true
      tpsCap.shouldRejectTransaction(m5, 0) shouldBe false

      clock.advance(
        Duration.ofSeconds(observationPeriodSeconds.toLong).minus(Duration.ofMillis(rounds.toLong))
      )
      tpsCap.addEvent(clock.now, m5, eventSize) // 120% -> 100% utilization (+1 and -3)
      tpsCap.getMemberUsage(m1).map(_.count) should contain(rounds - 1)
      tpsCap.getMemberUsage(m2).map(_.count) should contain(rounds - 1)
      tpsCap.getMemberUsage(m3).map(_.count) should contain(rounds - 1)
      tpsCap.getMemberUsage(m4).map(_.count) should contain(rounds - 1)
      tpsCap.getMemberUsage(m5).map(_.count) should contain(rounds - 1)
    }

    "enforce throttling when high KB/s reaches highest threshold level" in {
      val observationPeriodSeconds = 1
      val bytesCap = defaultGlobalKbpsCap * 1024 * observationPeriodSeconds
      val config = createIndividualConfig(perClientKbpsCap = bytesCap * 0.4)
      val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
      val tpsCap = new IndividualBlockSequencerThroughputCap(
        observationPeriodSeconds,
        config,
        clock,
        loggerFactory = loggerFactory,
      )
      val eventSize = (bytesCap * 0.1).toLong

      // initialize the tps cap logic
      tpsCap.addEvent(clock.now, m1, 1)
      clock.advance(
        Duration.ofSeconds(observationPeriodSeconds.toLong).plus(Duration.ofMillis(1))
      )

      // put the cap logic into the highest (enforcement) level threshold (90%)
      val rounds = 3
      (1 to rounds).foreach { _ =>
        tpsCap.addEvent(clock.now, m1, eventSize)
        tpsCap.addEvent(clock.now, m2, eventSize)
        tpsCap.addEvent(clock.now, m3, eventSize)
        clock.advance(Duration.ofMillis(1))
      }

      tpsCap.getMemberUsage(m1).map(_.bytes) should contain(3 * eventSize)
      tpsCap.getMemberUsage(m2).map(_.bytes) should contain(3 * eventSize)
      tpsCap.getMemberUsage(m3).map(_.bytes) should contain(3 * eventSize)

      // w/ globalCap = 3000KB/s and vActive = 3, throttled rate per member is: 3000KB/s / (1 + 3) = 768 KB/s
      // since m1 already has 1228 KB/s events in the window, it will be denied service
      tpsCap.shouldRejectTransaction(m1, 0) shouldBe true
      // but m4 (w/ no usage) is allowed to send requests
      tpsCap.shouldRejectTransaction(m4, 0) shouldBe false

      tpsCap.addEvent(clock.now, m4, eventSize) // 100%, and vAct = 4, so fair share is 614 KB/s
      tpsCap.addEvent(clock.now, m5, eventSize) // 110%, and vAct = 5, so fair share is 512 KB/s
      tpsCap.shouldRejectTransaction(m4, 0) shouldBe false
      tpsCap.shouldRejectTransaction(m4, 0) shouldBe false
      tpsCap.addEvent(clock.now, m4, eventSize) // 120%
      tpsCap.getMemberUsage(m4).map(_.bytes) should contain(2 * eventSize)
      tpsCap.getMemberUsage(m5).map(_.bytes) should contain(eventSize)
      tpsCap.shouldRejectTransaction(m4, 0) shouldBe true
      tpsCap.shouldRejectTransaction(m5, 0) shouldBe false

      clock.advance(
        Duration.ofSeconds(observationPeriodSeconds.toLong).minus(Duration.ofMillis(rounds.toLong))
      )
      tpsCap.addEvent(clock.now, m5, eventSize) // 120% -> 100% utilization (+1 and -3)
      tpsCap.getMemberUsage(m1).map(_.bytes) should contain(2 * eventSize)
      tpsCap.getMemberUsage(m2).map(_.bytes) should contain(2 * eventSize)
      tpsCap.getMemberUsage(m3).map(_.bytes) should contain(2 * eventSize)
      tpsCap.getMemberUsage(m4).map(_.bytes) should contain(2 * eventSize)
      tpsCap.getMemberUsage(m5).map(_.bytes) should contain(2 * eventSize)
    }

    "set member usage back to None if no activity within the observation window" in {
      val observationPeriodSeconds = 1
      val config = createIndividualConfig()
      val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
      val tpsCap = new IndividualBlockSequencerThroughputCap(
        observationPeriodSeconds,
        config,
        clock,
        loggerFactory = loggerFactory,
      )
      val eventSize: Long = 128

      tpsCap.addEvent(clock.now, m1, eventSize)
      tpsCap.getMemberUsage(m1).map(_.count) should contain(1)

      clock.advance(Duration.ofSeconds(observationPeriodSeconds.toLong).plus(Duration.ofMillis(1)))
      tpsCap.addEvent(clock.now, m2, eventSize)
      tpsCap.getMemberUsage(m1) shouldBe empty
      tpsCap.getMemberUsage(m2).map(_.count) should contain(1)
    }
  }
}

object BlockSequencerThroughputCapTest {
  private val m1 = DefaultTestIdentities.participant1
  private val m2 = DefaultTestIdentities.participant2
  private val m3 = DefaultTestIdentities.participant3
  private val m4 = DefaultTestIdentities.sequencerId
  private val m5 = DefaultTestIdentities.mediatorId

  private val defaultGlobalTpsCap = 10.0
  private val defaultGlobalKbpsCap = 3000.0
  private val defaultPerClientTpsCap = 1.0
  private val defaultPerClientKbpsCap = 200.0
}
