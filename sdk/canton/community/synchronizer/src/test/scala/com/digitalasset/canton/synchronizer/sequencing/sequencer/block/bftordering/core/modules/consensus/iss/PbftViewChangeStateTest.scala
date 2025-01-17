// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.SegmentState.computeLeaderOfView
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.ViewChange
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.SequencerId
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level.{INFO, WARN}

class PbftViewChangeStateTest extends AsyncWordSpec with BftSequencerBaseTest {

  import SegmentStateTest.*

  private val clock = new SimClock(loggerFactory = loggerFactory)

  private implicit val mc: MetricsContext = MetricsContext.Empty
  private val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering

  "PbftViewChangeState" when {
    "storing messages" should {
      "store only valid View Change messages" in {
        val nextView = ViewNumber(ViewNumber.First + 1L)
        val originalLeaderIndex = allPeers.indexOf(myId)
        val nextLeader = computeLeaderOfView(nextView, originalLeaderIndex, allPeers)

        val vcState = new PbftViewChangeState(
          fullMembership,
          leader = nextLeader,
          epoch = EpochNumber.First,
          view = nextView,
          slotNumbers,
          metrics,
          loggerFactory,
        )

        val vc1 = createViewChange(nextView, myId, myId, slotNumbers.map(_ -> ViewNumber.First))

        // Fresh view change message should process correctly
        assertNoLogs(vcState.processMessage(vc1)) shouldBe true

        // Duplicate should be rejected
        assertLogs(
          vcState.processMessage(vc1),
          log => {
            log.level shouldBe INFO
            log.message should include("already exists")
          },
        ) shouldBe false
      }

      "store only valid New View messages" in {
        val nextView = ViewNumber(ViewNumber.First + 1)
        val originalLeaderIndex = allPeers.indexOf(myId)
        val nextLeader = computeLeaderOfView(nextView, originalLeaderIndex, allPeers)

        val vcState = new PbftViewChangeState(
          fullMembership,
          leader = nextLeader,
          epoch = EpochNumber.First,
          view = nextView,
          slotNumbers,
          metrics,
          loggerFactory,
        )

        // Create strong quorum number of View Change messages, and extract PrePrepares from one of them
        val vcSet = allPeers
          .take(fullMembership.orderingTopology.strongQuorum)
          .map(peer =>
            createViewChange(nextView, peer, myId, slotNumbers.map(_ -> ViewNumber.First))
          )
        val ppSeq = vcSet(0).message.consensusCerts.map(_.prePrepare)

        // Receiving a New View message from the incorrect leader fails
        val wrongNextLeader =
          computeLeaderOfView(ViewNumber(nextView + 1), originalLeaderIndex, allPeers)
        val wrongNewView = createNewView(nextView, wrongNextLeader, myId, vcSet, ppSeq)
        assertLogs(
          vcState.processMessage(wrongNewView),
          log => {
            log.level shouldBe WARN
            log.message should include("but the leader of view")
          },
        ) shouldBe false

        // Receiving a New View message from the correct leader succeeds
        val correctNewView = createNewView(nextView, nextLeader, myId, vcSet, ppSeq)
        assertNoLogs(vcState.processMessage(correctNewView)) shouldBe true

        // Duplicate New View message are rejected
        assertLogs(
          vcState.processMessage(correctNewView),
          log => {
            log.level shouldBe INFO
            log.message should include("already exists; ignoring new")
          },
        ) shouldBe false
      }
    }

    "constructing a New View message (as a non-original leader)" should {
      class SystemState(
          viewNumbersPerPeer: Seq[Map[Long, Long]],
          val nextView: ViewNumber = ViewNumber(ViewNumber.First + 1),
          val originalLeader: SequencerId = otherPeer3,
      ) {
        val originalLeaderIndex: Int = allPeers.indexOf(originalLeader)
        val nextLeader: SequencerId = computeLeaderOfView(nextView, originalLeaderIndex, allPeers)
        val vcState = new PbftViewChangeState(
          fullMembership,
          leader = nextLeader,
          epoch = EpochNumber.First,
          view = nextView,
          slotNumbers,
          metrics,
          loggerFactory,
        )
        val vcSet: IndexedSeq[SignedMessage[ViewChange]] = createViewChangeSet(
          nextView,
          originalLeader,
          viewNumbersPerPeer,
        )
      }

      "produce a New View with no bottom blocks when all slots have an original view prepare cert" in {
        val systemState = new SystemState(
          (0 until fullMembership.orderingTopology.strongQuorum)
            .map(_ => slotNumbers.map(_.toLong -> ViewNumber.First.toLong).toMap.forgetNE)
        )
        import systemState.*

        vcSet.foreach(vcState.processMessage)
        vcState.shouldCreateNewView shouldBe true

        val maybePrePrepares = vcState.constructPrePreparesForNewView(blockMetaData, clock.now)
        val prePrepares = maybePrePrepares.collect { case Right(r) => r }

        prePrepares.size should be(maybePrePrepares.size)

        prePrepares.foreach(_.message.viewNumber shouldBe ViewNumber.First)
        prePrepares.foreach(_.from shouldBe originalLeader)
        prePrepares should have size slotNumbers.size.toLong
      }

      "produce a New View with all bottom blocks when no slots have any prepare cert" in {
        val systemState = new SystemState(
          (0 until fullMembership.orderingTopology.strongQuorum)
            .map(_ => Map.empty[Long, Long])
        )
        import systemState.*

        vcSet.foreach(vcState.processMessage)
        vcState.shouldCreateNewView shouldBe true

        val maybePrePrepares = vcState.constructPrePreparesForNewView(blockMetaData, clock.now)
        val nv = vcState.createNewViewMessage(
          blockMetaData,
          segmentIndex,
          clock.now,
          maybePrePrepares.map {
            case Right(r) => r
            case Left(toSign) => toSign.fakeSign
          },
        )

        nv.prePrepares.size should be(maybePrePrepares.size)

        nv.prePrepares.foreach(_.message.viewNumber shouldBe nextView)
        nv.prePrepares.foreach(_.from shouldBe nextLeader)
        nv.prePrepares should have size slotNumbers.size.toLong
      }

      "produce a New View with the highest-view PrePrepare (w/ valid cert) for each slot" in {
        val systemState = new SystemState(
          Seq(
            Map(BlockNumber.First -> ViewNumber.First, 4L -> 1L, 8L -> 2L),
            Map(BlockNumber.First -> ViewNumber.First, 4L -> 1L),
            Map(BlockNumber.First -> ViewNumber.First, 4L -> ViewNumber.First),
          ).map(_.map { case (k, v) => BlockNumber(k) -> ViewNumber(v) }),
          nextView = ViewNumber(3L),
          originalLeader = otherPeer1,
        )
        import systemState.*

        vcSet.foreach(vcState.processMessage)
        vcState.shouldCreateNewView shouldBe true

        val maybePrePrepares = vcState.constructPrePreparesForNewView(blockMetaData, clock.now)
        val prePrepares = maybePrePrepares.collect { case Right(r) => r }

        prePrepares.size should be(maybePrePrepares.size)
        prePrepares.map(pp =>
          pp.from -> pp.message.viewNumber
        ) should contain theSameElementsInOrderAs Seq(
          originalLeader -> 0,
          otherPeer2 -> 1,
          otherPeer3 -> 2,
        )
        prePrepares should have size slotNumbers.size.toLong
      }

      "produce a New View with highest-view PrePrepare for each slot, including bottom blocks" in {
        val systemState = new SystemState(
          Seq(
            Map(BlockNumber.First -> ViewNumber.First, 4L -> 1),
            Map(BlockNumber.First -> ViewNumber.First, 4L -> 1),
            Map(BlockNumber.First -> ViewNumber.First, 4L -> 0),
          ),
          nextView = ViewNumber(3L),
          originalLeader = otherPeer1,
        )
        import systemState.*

        vcSet.foreach(vcState.processMessage)
        vcState.shouldCreateNewView shouldBe true

        val maybePrePrepares = vcState.constructPrePreparesForNewView(blockMetaData, clock.now)
        val prePrepares = maybePrePrepares.map {
          case Right(r) => r
          case Left(l) => l.fakeSign
        }

        prePrepares.size should be(maybePrePrepares.size)
        prePrepares.map(pp =>
          pp.from -> pp.message.viewNumber
        ) should contain theSameElementsInOrderAs Seq(
          originalLeader -> 0,
          otherPeer2 -> 1,
          myId -> 3,
        )
        prePrepares should have size slotNumbers.size.toLong
      }
    }
  }
}
