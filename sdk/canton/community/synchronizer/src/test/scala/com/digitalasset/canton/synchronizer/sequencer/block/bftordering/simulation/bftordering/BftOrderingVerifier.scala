// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.PeanoQueue
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  SimulationSettings,
  SimulationVerifier,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.data.StorageHelpers
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.DurationConverters.ScalaDurationOps

import BftOrderingVerifier.LivenessState

final class BftOrderingVerifier(
    queue: mutable.Queue[(BftNodeId, BlockFormat.Block)],
    stores: Map[BftNodeId, OutputMetadataStore[SimulationEnv]],
    onboardingTimes: Map[BftNodeId, TopologyActivationTime],
    initialNodes: Seq[BftNodeId],
    simSettings: SimulationSettings,
    override val loggerFactory: NamedLoggerFactory,
) extends SimulationVerifier
    with Matchers
    with NamedLogging {

  private val currentLog = ArrayBuffer[BlockFormat.Block]()

  private var previousTimestamp = 0L

  private val peanoQueues = mutable.Map.empty[BftNodeId, PeanoQueue[BlockNumber, BlockFormat.Block]]

  initialNodes.foreach { node =>
    peanoQueues(node) = new PeanoQueue(BlockNumber(0L))(fail(_))
  }

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

    checkStores()
  }

  override def nodeStarted(at: CantonTimestamp, node: BftNodeId): Unit = {
    implicit val traceContext: TraceContext = TraceContext.empty
    // Conservatively, find the most advanced store to increase certainty that it contains the onboarding block.
    // If the right onboarding block is not found, the simulation is supposed to fail on liveness due to a gap
    //  in the relevant peano queue.
    val store = StorageHelpers.findMostAdvancedOutputStore(stores)._2
    val onboardingTime = onboardingTimes(node)
    val startingBlock = store
      .getLatestBlockAtOrBefore(onboardingTime.value)
      .resolveValue()
      .getOrElse(fail(s"failed to get an onboarding block for peer $node"))
      .map(_.blockNumber)
    peanoQueues(node) = new PeanoQueue(startingBlock.getOrElse(BlockNumber(0L)))(fail(_))
  }

  override def aFutureHappened(node: BftNodeId): Unit = ()

  def lastSequencerAcknowledgedBlock(node: BftNodeId): Option[BlockNumber] =
    peanoQueues
      .get(node)
      .flatMap { queue =>
        val head = queue.head.unwrap
        if (head == 0L) {
          None
        } else {
          Some(BlockNumber(head - 1L)) // head is what we are waiting for
        }
      }

  def newStage(
      simulationSettings: SimulationSettings,
      newOnboardingTimes: Map[BftNodeId, TopologyActivationTime],
      newStores: Map[BftNodeId, OutputMetadataStore[SimulationEnv]],
  ): BftOrderingVerifier = {
    val newVerifier =
      new BftOrderingVerifier(
        queue,
        stores ++ newStores,
        newOnboardingTimes,
        Seq.empty,
        simulationSettings,
        loggerFactory,
      )
    newVerifier.currentLog ++= currentLog
    newVerifier.previousTimestamp = previousTimestamp
    newVerifier.peanoQueues ++= peanoQueues
    newVerifier
  }

  private def checkLiveness(at: CantonTimestamp): Unit =
    livenessState match {
      case LivenessState.NotChecking => ()
      case LivenessState.Checking(startedAt, logSizeAtStart, peanoQueueSnapshots) =>
        val hasEveryoneMadeProgress = peanoQueueSnapshots.forall { case (node, peanoQueueHead) =>
          peanoQueues(node).head.unwrap > peanoQueueHead
        }
        if (currentLog.sizeIs > logSizeAtStart && hasEveryoneMadeProgress) {
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
    implicit val traceContext: TraceContext = TraceContext.empty
    queue.foreach { case (sequencer, block) =>
      logger.debug(s"Simulation verifier: got block ${block.blockHeight} from sequencer $sequencer")
      val peanoQueue = peanoQueues.getOrElse(
        sequencer,
        throw new RuntimeException(s"Sequencer $sequencer not onboarded"),
      )
      peanoQueue.insert(BlockNumber(block.blockHeight), block)
      val newBlocks = peanoQueue.pollAvailable()
      newBlocks.foreach { blockToInsert =>
        logger.debug(
          s"Simulation verifier: checking block ${blockToInsert.blockHeight} in order from sequencer $sequencer"
        )
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
        peanoQueueSnapshots: Map[BftNodeId, PeanoQueueHead],
    ) extends LivenessState
  }
}
