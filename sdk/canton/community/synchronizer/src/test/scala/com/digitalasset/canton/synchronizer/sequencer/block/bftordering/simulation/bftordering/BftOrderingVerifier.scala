// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.bftordering

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.PeanoQueue
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  SimulationSettings,
  SimulationVerifier,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.data.StorageHelpers
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.DurationConverters.ScalaDurationOps

import BftOrderingVerifier.{LivenessState, OffboardingStatus}

final class BftOrderingVerifier(
    queue: mutable.Queue[(BftNodeId, Traced[BlockFormat.Block])],
    stores: Map[BftNodeId, OutputMetadataStore[SimulationEnv]],
    onboardingTimes: Map[BftNodeId, TopologyActivationTime],
    offboardingTimes: Map[BftNodeId, CantonTimestamp],
    initialNodes: Seq[BftNodeId],
    simSettings: SimulationSettings,
    epochLength: EpochLength,
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

  private val offboardingProgress = mutable.Map.empty[BftNodeId, OffboardingStatus]

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
      newOffboardingTimes: Map[BftNodeId, CantonTimestamp],
      newStores: Map[BftNodeId, OutputMetadataStore[SimulationEnv]],
  ): BftOrderingVerifier = {
    val newVerifier =
      new BftOrderingVerifier(
        queue,
        stores ++ newStores,
        newOnboardingTimes,
        newOffboardingTimes,
        Seq.empty,
        simulationSettings,
        epochLength,
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
        val (nodesInTopology, nodesNotInTopology) = peanoQueueSnapshots
          .partition(x => offboardingTimes.get(x._1).forall(at <= _))
        val hasEveryoneMadeProgress = nodesInTopology
          .forall { case (node, peanoQueueHead) =>
            peanoQueues(node).head.unwrap > peanoQueueHead
          }
        if (currentLog.sizeIs > logSizeAtStart && hasEveryoneMadeProgress) {
          // There has been progress since the simulation became healthy, so we don't need to check anymore
          livenessState = LivenessState.NotChecking
        } else {
          withClue(
            s"previous log size = $logSizeAtStart, current log size = ${currentLog.size}\n" +
              s"previous peano queue heads (onboarded)  = $nodesInTopology\n" +
              s"current  peano queue heads (onboarded)  = ${peanoQueues.view.filterKeys(nodesInTopology.contains).mapValues(_.head.unwrap).toMap}\n" +
              s"previous peano queue heads (offboarded) = $nodesNotInTopology\n" +
              s"current  peano queue heads (offboarded) = ${peanoQueues.view.filterKeys(nodesNotInTopology.contains).mapValues(_.head.unwrap).toMap}\n" +
              (if (nodesNotInTopology.nonEmpty) {
                 s"offboarding times = $offboardingTimes\n" +
                   s"offboardingProgress = $offboardingProgress\n"
               } else "") +
              "liveness timeout occurred"
          ) {
            at should be <= startedAt.add(simSettings.livenessCheckInterval.toJava)
          }
        }
    }

  private def checkBlockAgainstModel(block: BlockFormat.Block): Unit =
    if (block.blockHeight < currentLog.size) {
      // Block already exists in the model, check it is the same
      block shouldBe currentLog(block.blockHeight.toInt)
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

  private def checkOffboardingStatus(node: BftNodeId, block: BlockFormat.Block): Unit =
    offboardingTimes.get(node) match {
      case Some(offboardingTime) =>
        val blockNumber = BlockNumber(block.blockHeight)
        offboardingProgress.get(node) match {
          case Some(OffboardingStatus.FinishedOffboarding) =>
            throw new RuntimeException(
              s"Sequencer $node published block $blockNumber after finished offboarding"
            )
          case Some(OffboardingStatus.StartedOffboarding(finalBlock)) =>
            if (blockNumber == finalBlock) {
              offboardingProgress(node) = OffboardingStatus.FinishedOffboarding
            }
          case None =>
            val blockPartOfLastEpoch =
              block.requests.exists(_.value.microsecondsSinceEpoch >= offboardingTime.toMicros)

            if (blockPartOfLastEpoch) {
              val lastBlockInEpoch = computeLastBlockInEpoch(blockNumber)
              if (blockNumber == lastBlockInEpoch) {
                offboardingProgress(node) = OffboardingStatus.FinishedOffboarding
              } else {
                offboardingProgress(node) = OffboardingStatus.StartedOffboarding(lastBlockInEpoch)
              }
            }
        }

      case None =>
    }

  // TODO(#19289) this assumes an ever static epoch length
  private def computeLastBlockInEpoch(blockNumber: BlockNumber): BlockNumber = BlockNumber(
    epochLength * ((blockNumber / epochLength) + 1) - 1
  )

  private def checkStores(): Unit = {
    implicit val traceContext: TraceContext = TraceContext.empty
    queue.foreach { case (sequencer, block) =>
      logger.debug(
        s"Simulation verifier: got block ${block.value.blockHeight} from sequencer $sequencer"
      )
      val peanoQueue = peanoQueues.getOrElse(
        sequencer,
        throw new RuntimeException(s"Sequencer $sequencer not onboarded"),
      )
      peanoQueue.insert(BlockNumber(block.value.blockHeight), block.value)
      val newBlocks = peanoQueue.pollAvailable()
      newBlocks.foreach { blockToInsert =>
        logger.debug(
          s"Simulation verifier: checking block ${blockToInsert.blockHeight} in order from sequencer $sequencer"
        )
        checkBlockAgainstModel(blockToInsert)
        checkOffboardingStatus(sequencer, blockToInsert)
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

  sealed trait OffboardingStatus

  object OffboardingStatus {
    final case object FinishedOffboarding extends OffboardingStatus
    final case class StartedOffboarding(finalBlock: BlockNumber) extends OffboardingStatus
  }
}
