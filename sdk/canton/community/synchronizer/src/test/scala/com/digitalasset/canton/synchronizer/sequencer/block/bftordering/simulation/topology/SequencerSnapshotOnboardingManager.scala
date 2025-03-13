// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.ModuleRef
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.snapshot.SequencerSnapshotAdditionalInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.SequencerNode.SnapshotMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Output,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.SimulationModuleSystem.SimulationEnv
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingManager
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.{
  AddEndpoint,
  Command,
  EventOriginator,
  InjectedSend,
  ModuleAddress,
  PrepareOnboarding,
  SimulationSettings,
  StartMachine,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.data.StorageHelpers
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.simulation.topology.SequencerSnapshotOnboardingManager.DefaultEpsilonForSchedulingCommand
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.ScalaDurationOps

class SequencerSnapshotOnboardingManager(
    newlyOnboardingNodesToTimes: Map[BftNodeId, TopologyActivationTime],
    initialNodes: Seq[BftNodeId],
    nodeToEndpoint: Map[BftNodeId, P2PEndpoint],
    stores: Map[BftNodeId, BftOrderingStores[SimulationEnv]],
    simulationSettings: SimulationSettings,
) extends OnboardingManager[Option[SequencerSnapshotAdditionalInfo]] {

  implicit private val traceContext: TraceContext = TraceContext.empty

  private var onboardedNodes = initialNodes

  private val nodeToSequencerSnapshotAdditionalInfo =
    mutable.Map.empty[BftNodeId, SequencerSnapshotAdditionalInfo]

  private var nodesToStart = Seq.empty[BftNodeId]

  private var nodesToRetry = Seq.empty[BftNodeId]

  override def provide(forNode: BftNodeId): Option[SequencerSnapshotAdditionalInfo] =
    nodeToSequencerSnapshotAdditionalInfo.get(forNode)

  override def commandsToSchedule(timestamp: CantonTimestamp): Seq[(Command, FiniteDuration)] = {
    val commandsToStartNodes: Seq[(Command, FiniteDuration)] = nodesToStart.map { sequencerId =>
      val myEndpoint = nodeToEndpoint(sequencerId)

      StartMachine(myEndpoint) -> DefaultEpsilonForSchedulingCommand
    }
    nodesToStart = Seq.empty

    val commandsToRetryNodes: Seq[(Command, FiniteDuration)] = nodesToRetry.map { sequencerId =>
      PrepareOnboarding(
        sequencerId
      ) -> simulationSettings.retryBecomingOnlineInterval
    }
    nodesToRetry = Seq.empty

    commandsToStartNodes ++ commandsToRetryNodes
  }

  override def machineStarted(
      timestamp: CantonTimestamp,
      myEndpoint: P2PEndpoint,
      node: BftNodeId,
  ): Seq[(Command, FiniteDuration)] = {
    val oneWayEndpointAdditions: Seq[(Command, FiniteDuration)] = onboardedNodes.map { otherNode =>
      AddEndpoint(myEndpoint, otherNode) -> 1.milliseconds
    }

    val otherWayEndpointAdditions: Seq[(Command, FiniteDuration)] = onboardedNodes.map {
      otherNode =>
        AddEndpoint(nodeToEndpoint(otherNode), node) -> 1.milliseconds
    }

    onboardedNodes = node +: onboardedNodes

    oneWayEndpointAdditions ++ otherWayEndpointAdditions
  }

  def newStage(
      newlyOnboardedNodesToOnboardingTimes: Map[BftNodeId, TopologyActivationTime],
      newNodesToEndpoint: Map[BftNodeId, P2PEndpoint],
      simulationSettings: SimulationSettings,
  ): SequencerSnapshotOnboardingManager = new SequencerSnapshotOnboardingManager(
    newlyOnboardedNodesToOnboardingTimes,
    onboardedNodes,
    newNodesToEndpoint,
    stores,
    simulationSettings,
  )

  override def initCommands: Seq[(Command, CantonTimestamp)] = newlyOnboardingNodesToTimes.map {
    case (node, timestamp) =>
      PrepareOnboarding(node) -> timestamp.value.add(
        simulationSettings.becomingOnlineAfterOnboardingDelay.toJava
      )
  }.toSeq

  override def prepareOnboardingFor(
      at: CantonTimestamp,
      node: BftNodeId,
  ): Seq[(Command, FiniteDuration)] = {
    // Conservatively, find the most advanced store to increase certainty that it contains the right onboarding data.
    val nodeToAsk =
      StorageHelpers.findMostAdvancedOutputStore(stores.view.mapValues(_.outputStore).toMap)._1

    val sequencerActivationTime = newlyOnboardingNodesToTimes(node)

    Seq(
      InjectedSend(
        nodeToAsk,
        ModuleAddress.Output,
        EventOriginator.FromClient,
        Output.SequencerSnapshotMessage.GetAdditionalInfo(
          sequencerActivationTime.value,
          new ModuleRef[SequencerNode.SnapshotMessage] {
            override def asyncSendTraced(msg: SequencerNode.SnapshotMessage)(implicit
                traceContext: TraceContext
            ): Unit =
              msg match {
                case SnapshotMessage.AdditionalInfo(info) =>
                  val snapshot = SequencerSnapshotAdditionalInfo
                    .fromProto(info.toByteString) // silly to convert to ByteString
                    .getOrElse(sys.error(s"Can't parse sequencerSnapshot $info"))

                  if (snapshot.nodeActiveAt.contains(node)) {
                    nodesToStart = node +: nodesToStart
                    nodeToSequencerSnapshotAdditionalInfo(node) = snapshot
                  } else {
                    nodesToRetry = node +: nodesToRetry
                  }

                case SnapshotMessage.AdditionalInfoRetrievalError(errorMessage) =>
                  sys.error(s"Couldn't retrieve SequencerSnapshot: $errorMessage")
              }
          },
        ),
      ) -> DefaultEpsilonForSchedulingCommand
    )
  }
}

object SequencerSnapshotOnboardingManager {
  // We make the commands take some time to go into effect, this amount is completely arbitrary
  private val DefaultEpsilonForSchedulingCommand = 1.milliseconds
}
