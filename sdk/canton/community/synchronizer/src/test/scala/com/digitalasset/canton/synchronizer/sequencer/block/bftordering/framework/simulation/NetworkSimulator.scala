// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  PlainTextP2PEndpoint,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId

import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Random

private sealed trait NetworkSimulatorState {

  def tick(
      nodes: Set[SequencerId],
      settings: NetworkSettings,
      clock: Clock,
      random: Random,
  ): NetworkSimulatorState

  def partitionExists(peer1: SequencerId, peer2: SequencerId): Boolean
}

private object NetworkSimulatorState {

  final case class NoCurrentPartition(started: CantonTimestamp) extends NetworkSimulatorState {
    private def shouldCreatePartition(
        settings: NetworkSettings,
        clock: Clock,
        random: Random,
    ): Boolean =
      clock.now.isAfter(
        started.plus(settings.unPartitionStability.toJava)
      ) && settings.partitionProbability.flipCoin(random)

    private def makePartition(
        nodes: Set[SequencerId],
        settings: NetworkSettings,
        random: Random,
    ): Set[BrokenLink] = {
      val selectedSet: Set[SequencerId] = settings.partitionMode.selectSet(nodes, random)
      val otherSet = nodes.removedAll(selectedSet)
      nodes.flatMap { peer1 =>
        otherSet.flatMap { peer2 =>
          BrokenLink(settings, peer1, peer2)
        }
      }
    }

    override def tick(
        nodes: Set[SequencerId],
        settings: NetworkSettings,
        clock: Clock,
        random: Random,
    ): NetworkSimulatorState =
      if (shouldCreatePartition(settings, clock, random)) {
        NetworkSimulatorState.ActivePartition(
          makePartition(nodes, settings, random),
          clock.now,
        )
      } else { this }

    override def partitionExists(peer1: SequencerId, peer2: SequencerId): Boolean = false
  }

  private final case class ActivePartition(partition: Set[BrokenLink], started: CantonTimestamp)
      extends NetworkSimulatorState {

    private def shouldRemovePartition(
        settings: NetworkSettings,
        clock: Clock,
        random: Random,
    ): Boolean =
      clock.now.isAfter(
        started.plus(settings.partitionStability.toJava)
      ) && settings.unPartitionProbability.flipCoin(random)

    override def tick(
        nodes: Set[SequencerId],
        settings: NetworkSettings,
        clock: Clock,
        random: Random,
    ): NetworkSimulatorState =
      if (shouldRemovePartition(settings, clock, random)) {
        NetworkSimulatorState.NoCurrentPartition(clock.now)
      } else { this }

    override def partitionExists(peer1: SequencerId, peer2: SequencerId): Boolean =
      partition.contains(BrokenLink(peer1, peer2))
  }
}

private final case class BrokenLink(from: SequencerId, to: SequencerId)

private object BrokenLink {

  def apply(settings: NetworkSettings, peer1: SequencerId, peer2: SequencerId): Set[BrokenLink] =
    settings.partitionSymmetry match {
      case PartitionSymmetry.Symmetric => Set(BrokenLink(peer1, peer2), BrokenLink(peer2, peer1))
      case PartitionSymmetry.ASymmetric => Set(BrokenLink(peer1, peer2))
    }
}

class NetworkSimulator(
    settings: NetworkSettings,
    topology: Topology[?, ?, ?, ?],
    agenda: Agenda,
    clock: Clock,
) {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private val random = new Random(settings.randomSeed)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var partitionState: NetworkSimulatorState =
    NetworkSimulatorState.NoCurrentPartition(clock.now)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var canUseFaults = true

  def tick(): Unit = if (canUseFaults) {
    partitionState = partitionState.tick(nodes, settings, clock, random)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  def scheduleNetworkEvent(
      fromPeer: SequencerId,
      toPeer: SequencerId,
      msg: Any,
  ): Unit = {

    if (canUseFaults && settings.packetLoss.flipCoin(random)) {
      // this got dropped
      return
    }

    if (canUseFaults && partitionState.partitionExists(fromPeer, toPeer)) {
      // there is a current partition, so drop
      return
    }

    val sendCount = 1 + (if (canUseFaults && settings.packetReplay.flipCoin(random)) 1 else 0)

    1 to sendCount foreach { _ =>
      val delay = settings.oneWayDelay
        .generateRandomDuration(random)
      agenda.addOne(
        ReceiveNetworkMessage(toPeer, msg),
        delay,
      )
    }
  }

  def scheduleEstablishConnection(
      fromPeer: SequencerId,
      toPeer: SequencerId,
      endpoint: PlainTextP2PEndpoint,
      continuation: (P2PEndpoint.Id, SequencerId) => Unit,
  ): Unit = {
    val delay = settings.establishConnectionDelay.generateRandomDuration(random)
    agenda.addOne(
      EstablishConnection(fromPeer, toPeer, endpoint, continuation),
      delay,
    )
  }

  def makeHealthy(): Unit = {
    canUseFaults = false
    partitionState = NetworkSimulatorState.NoCurrentPartition(clock.now)
  }

  private def nodes = topology.activeSequencersToMachines.view.keySet.toSet
}
