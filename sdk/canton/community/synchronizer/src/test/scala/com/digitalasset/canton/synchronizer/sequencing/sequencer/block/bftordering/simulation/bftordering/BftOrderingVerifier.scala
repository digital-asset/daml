// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.simulation.bftordering

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.PeanoQueue
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.output.data.memory.SimulationOutputBlockMetadataStore
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.simulation.{
  SimulationSettings,
  SimulationVerifier,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.simulation.topology.SimulationTopologyHelpers.sequencerBecomeOnlineTime
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.DurationConverters.ScalaDurationOps

import BftOrderingVerifier.LivenessState

class BftOrderingVerifier(
    queue: mutable.Queue[(SequencerId, BlockFormat.Block)],
    stores: Map[SequencerId, SimulationOutputBlockMetadataStore],
    onboardingTimes: Map[SequencerId, TopologyActivationTime],
    simSettings: SimulationSettings,
) extends SimulationVerifier
    with Matchers {

  private val currentLog = ArrayBuffer[BlockFormat.Block]()

  private var previousTimestamp = 0L

  private val peanoQueues = mutable.Map.empty[SequencerId, PeanoQueue[BlockFormat.Block]]

  private val sequencersToOnboard = mutable.Set.from(onboardingTimes.keys)

  private var livenessState: LivenessState = LivenessState.NotChecking

  override def resumeCheckingLiveness(at: CantonTimestamp): Unit =
    livenessState match {
      case LivenessState.NotChecking =>
        livenessState = LivenessState.Checking(
          at,
          currentLog.size,
          peanoQueues.view.mapValues(_.head.unwrap).toMap,
        )
      case _ => // already checking, ignore
    }

  override def checkInvariants(at: CantonTimestamp): Unit = {
    checkLiveness(at)

    onboardSequencers(at)

    checkStores()
  }

  override def aFutureHappened(peer: SequencerId): Unit = ()

  def newStage(
      simulationSettings: SimulationSettings,
      newOnboardingTimes: Map[SequencerId, TopologyActivationTime],
      newStores: Map[SequencerId, SimulationOutputBlockMetadataStore],
  ): BftOrderingVerifier = {
    val newVerifier =
      new BftOrderingVerifier(
        queue,
        stores ++ newStores,
        newOnboardingTimes,
        simulationSettings,
      )
    newVerifier.currentLog ++= currentLog
    newVerifier.previousTimestamp = previousTimestamp
    newVerifier.peanoQueues ++= peanoQueues
    newVerifier.sequencersToOnboard ++= sequencersToOnboard
    newVerifier
  }

  private def checkLiveness(at: CantonTimestamp): Unit =
    livenessState match {
      case LivenessState.NotChecking => ()
      case LivenessState.Checking(startedAt, logSizeAtStart, peanoQueueSnapshots) =>
        val haveAllPeersMadeProgress = peanoQueueSnapshots.forall { case (peerId, peanoQueueHead) =>
          peanoQueues(peerId).head.unwrap > peanoQueueHead
        }
        if (currentLog.sizeIs > logSizeAtStart && haveAllPeersMadeProgress) {
          // There has been progress since the simulation became healthy, so we don't need to check anymore
          livenessState = LivenessState.NotChecking
        } else {
          withClue(
            s"previous log size = $logSizeAtStart, current log size = ${currentLog.size}, " +
              s"previous peano queue heads = $peanoQueueSnapshots, " +
              s"current peano queue heads = ${peanoQueues.view.mapValues(_.head.unwrap).toMap}; " +
              "liveness timeout occurred"
          ) {
            at should be <= startedAt.add(simSettings.livenessCheckInterval.toJava)
          }
        }
    }

  private def onboardSequencers(timestamp: CantonTimestamp): Unit =
    if (sequencersToOnboard.nonEmpty) {
      sequencersToOnboard.toSeq.foreach { sequencer =>
        val onboardingTime = onboardingTimes(sequencer)
        if (sequencerBecomeOnlineTime(onboardingTime, simSettings) < timestamp) {
          implicit val traceContext: TraceContext = TraceContext.empty
          // Conservatively, find the most advanced store to increase certainty that it contains the onboarding block.
          // If the right onboarding block is not found, the simulation is supposed to fail on liveness due to a gap
          //  in the relevant peano queue.
          val store = stores.values.maxBy( // `maxBy` can throw, it's fine for tests
            _.getLastConsecutive
              .resolveValue()
              .toOption
              .flatMap(_.map(_.blockNumber))
              .getOrElse(BlockNumber(0L))
          )
          val startingBlock = store
            .getLatestAtOrBefore(onboardingTime.value)
            .resolveValue()
            .getOrElse(fail(s"failed to get an onboarding block for peer $sequencer"))
            .map(_.blockNumber)
          peanoQueues(sequencer) = new PeanoQueue(startingBlock.getOrElse(BlockNumber(0L)))(fail(_))
          sequencersToOnboard.remove(sequencer)
        }
      }
    }

  private def checkBlockAgainstModel(block: BlockFormat.Block): Unit =
    if (block.blockHeight < currentLog.size) {
      // Block already exists in the model, check it is the same
      currentLog(block.blockHeight.toInt) shouldBe block
    } else {
      // New block
      block.blockHeight shouldBe currentLog.size
      block.requests.foreach { req =>
        val timestamp = req.value.microsecondsSinceEpoch
        timestamp should be > previousTimestamp
        previousTimestamp = timestamp
      }
      currentLog.append(block)
    }

  private def checkStores(): Unit = {
    queue.foreach { case (sequencer, block) =>
      val peanoQueue = peanoQueues.getOrElse(
        sequencer,
        throw new RuntimeException(s"Sequencer $sequencer not onboarded"),
      )
      peanoQueue.insert(BlockNumber(block.blockHeight), block)
      val newBlocks = peanoQueue.pollAvailable()
      newBlocks.foreach { blockToInsert =>
        checkBlockAgainstModel(blockToInsert)
      }
    }
    queue.clear()
  }
}

object BftOrderingVerifier {

  private type PeanoQueueHead = Long

  sealed trait LivenessState

  object LivenessState {
    case object NotChecking extends LivenessState
    final case class Checking(
        startedAt: CantonTimestamp,
        logSizeAtStart: Int,
        peanoQueueSnapshots: Map[SequencerId, PeanoQueueHead],
    ) extends LivenessState
  }
}
