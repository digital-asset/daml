// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.bftordering

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.BlockFormat
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.PeanoQueue
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.memory.SimulationOutputBlockMetadataStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.BlockNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.simulation.SimulationVerifier
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.simulation.bftordering.BftOrderingVerifier.LivenessState
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

class BftOrderingVerifier(
    queue: mutable.Queue[(SequencerId, BlockFormat.Block)],
    stores: Map[SequencerId, SimulationOutputBlockMetadataStore],
    onboardingTimes: Map[SequencerId, EffectiveTime],
    livenessRecoveryTimeout: FiniteDuration,
) extends SimulationVerifier
    with Matchers {
  private val currentLog = ArrayBuffer[BlockFormat.Block]()
  private var previousTimestamp = 0L

  private val peanoQueues = mutable.Map.empty[SequencerId, PeanoQueue[BlockFormat.Block]]

  private val sequencersToOnboard = mutable.Set.from(onboardingTimes.keys)

  private var livenessState: LivenessState = LivenessState.NotChecking

  override def simulationIsGoingHealthy(at: CantonTimestamp): Unit =
    livenessState = LivenessState.BecameHealthy(at, currentLog.size)

  override def checkInvariants(at: CantonTimestamp): Unit = {
    checkLiveness(at)

    onboardSequencers(at)

    checkStores()
  }

  private def checkLiveness(at: CantonTimestamp): Unit =
    livenessState match {
      case LivenessState.NotChecking => ()
      case LivenessState.BecameHealthy(healthyAt, sizeAtTime) =>
        if (sizeAtTime < currentLog.size) {
          // The log have progressed since the simulation became healthy
          // so we don't need to check anymore
          livenessState = LivenessState.NotChecking
        } else {
          withClue("liveness timeout occurred") {
            at should be <= healthyAt.add(livenessRecoveryTimeout.toJava)
          }
        }
    }

  private def onboardSequencers(timestamp: CantonTimestamp): Unit =
    if (sequencersToOnboard.nonEmpty) {
      sequencersToOnboard.toSeq.foreach { sequencer =>
        val onboardingTime = onboardingTimes(sequencer)
        if (onboardingTime.value < timestamp) {
          val store = stores(sequencer)
          val startingBlock = store
            .getLatestAtOrBefore(onboardingTime.value)(TraceContext.empty)
            .resolveValue()
            .getOrElse(fail(s"failed to get an onboarding block for peer $sequencer"))
            .map(_.blockNumber)
            .getOrElse(BlockNumber(0L))
          peanoQueues(sequencer) = new PeanoQueue(startingBlock)
          sequencersToOnboard.remove(sequencer)
        }
      }
    }

  private def checkBlockAgainstModel(block: BlockFormat.Block): Unit =
    if (block.blockHeight < currentLog.size) {
      // Block already exists in the model, check it is the same
      // TODO(#19661): State-transferred blocks might need to propagate this timestamp
      currentLog(block.blockHeight.toInt).copy(tickTopologyAtMicrosFromEpoch = None) shouldBe
        block.copy(tickTopologyAtMicrosFromEpoch = None)
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
      val peanoQueue = peanoQueues(sequencer)
      val newBlocks = peanoQueue.insertAndPoll(BlockNumber(block.blockHeight), block)
      newBlocks.foreach { blockToInsert =>
        checkBlockAgainstModel(blockToInsert)
      }
    }
    queue.clear()
  }

  override def aFutureHappened(peer: SequencerId): Unit =
    ()
}

object BftOrderingVerifier {
  sealed trait LivenessState

  object LivenessState {
    case object NotChecking extends LivenessState
    final case class BecameHealthy(healthyAt: CantonTimestamp, sizeAtTime: Int)
        extends LivenessState
  }
}
