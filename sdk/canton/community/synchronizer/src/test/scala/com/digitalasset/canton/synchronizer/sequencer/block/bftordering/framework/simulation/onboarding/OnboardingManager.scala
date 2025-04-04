// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.Command
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.onboarding.OnboardingManager.ReasonForProvide

import scala.concurrent.duration.FiniteDuration

trait OnboardingManager[OnboardingDataT] {

  /** This is called before the simulator is about to start a node, the [[OnboardingDataT]] is used
    * to start the node
    */
  def provide(reasonForProvide: ReasonForProvide, forNode: BftNodeId): OnboardingDataT

  /** This is called at the beginning of simulation and can be used by the manager to schedule tasks
    */
  def initCommands: Seq[(Command, CantonTimestamp)]

  /** This is called when the simulator reaches a
    * [[com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.simulation.PrepareOnboarding]]
    * command
    */
  def prepareOnboardingFor(
      at: CantonTimestamp,
      node: BftNodeId,
  ): Seq[(Command, FiniteDuration)]

  /** This is called after each step in the simulator
    */
  def commandsToSchedule(timestamp: CantonTimestamp): Seq[(Command, FiniteDuration)]

  /** This is called after the simulator have started a new node
    */
  def machineStarted(
      timestamp: CantonTimestamp,
      endpoint: P2PEndpoint,
      node: BftNodeId,
  ): Seq[(Command, FiniteDuration)]
}

object OnboardingManager {
  sealed trait ReasonForProvide

  object ReasonForProvide {
    final case object ProvideForInit extends ReasonForProvide
    final case object ProvideForRestart extends ReasonForProvide
  }
}

object EmptyOnboardingManager extends OnboardingManager[Unit] {
  override def provide(reasonForProvide: ReasonForProvide, forNode: BftNodeId): Unit = ()

  override def commandsToSchedule(timestamp: CantonTimestamp): Seq[(Command, FiniteDuration)] =
    Seq.empty

  override def machineStarted(
      timestamp: CantonTimestamp,
      endpoint: P2PEndpoint,
      node: BftNodeId,
  ): Seq[(Command, FiniteDuration)] = Seq.empty

  override def initCommands: Seq[(Command, CantonTimestamp)] = Seq.empty

  override def prepareOnboardingFor(
      at: CantonTimestamp,
      node: BftNodeId,
  ): Seq[(Command, FiniteDuration)] =
    Seq.empty
}
