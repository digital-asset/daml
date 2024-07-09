// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.EnterpriseSequencerAdminCommands
import com.digitalasset.canton.admin.api.client.commands.EnterpriseSequencerAdminCommands.{
  InitializeFromGenesisState,
  InitializeFromOnboardingState,
}
import com.digitalasset.canton.admin.api.client.data.StaticDomainParameters
import com.digitalasset.canton.config.{ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.console.{Help, SequencerReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.admin.grpc.InitializeSequencerResponse
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerSnapshot
import com.digitalasset.canton.grpc.ByteStringStreamObserver
import com.digitalasset.canton.sequencer.admin.v30.OnboardingStateResponse
import com.digitalasset.canton.topology.SequencerId
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

class SequencerAdministration(node: SequencerReference) extends ConsoleCommandGroup.Impl(node) {
  private def timeouts: ConsoleCommandTimeout = consoleEnvironment.commandTimeouts
  private implicit val ec: ExecutionContext = consoleEnvironment.environment.executionContext
  @Help.Summary(
    "Download sequencer snapshot at given point in time to bootstrap another sequencer"
  )
  def snapshot(timestamp: CantonTimestamp): SequencerSnapshot = {
    // TODO(#14074) add something like "snapshot for sequencer-id", rather than timestamp based
    //      we still need to keep the timestamp based such that we can provide recovery for corrupted sequencers
    consoleEnvironment.run {
      runner.adminCommand(EnterpriseSequencerAdminCommands.Snapshot(timestamp))
    }
  }

  @Help.Summary(
    "Download the onboarding state at a given point in time to bootstrap another sequencer"
  )
  def onboarding_state_at_timestamp(
      timestamp: CantonTimestamp,
      timeout: NonNegativeDuration = timeouts.unbounded,
  ): ByteString = {
    consoleEnvironment.run {
      val responseObserver =
        new ByteStringStreamObserver[OnboardingStateResponse](_.onboardingStateForSequencer)

      def call =
        runner.adminCommand(
          EnterpriseSequencerAdminCommands.OnboardingState(
            observer = responseObserver,
            sequencerOrTimestamp = Right(timestamp),
          )
        )

      processResult(call, responseObserver.resultBytes, timeout, "Downloading onboarding state")
    }
  }

  @Help.Summary(
    "Download the onboarding state for a given sequencer"
  )
  def onboarding_state_for_sequencer(
      sequencerId: SequencerId,
      timeout: NonNegativeDuration = timeouts.unbounded,
  ): ByteString = {
    consoleEnvironment.run {
      val responseObserver =
        new ByteStringStreamObserver[OnboardingStateResponse](_.onboardingStateForSequencer)

      def call =
        runner.adminCommand(
          EnterpriseSequencerAdminCommands.OnboardingState(
            observer = responseObserver,
            sequencerOrTimestamp = Left(sequencerId),
          )
        )
      processResult(call, responseObserver.resultBytes, timeout, "Downloading onboarding state")
    }
  }

  @Help.Summary(
    "Initialize a sequencer from the beginning of the event stream. This should only be called for " +
      "sequencer nodes being initialized at the same time as the corresponding domain node. " +
      "This is called as part of the domain.setup.bootstrap command, so you are unlikely to need to call this directly."
  )
  def assign_from_genesis_state(
      genesisState: ByteString,
      domainParameters: StaticDomainParameters,
      waitForReady: Boolean = true,
  ): InitializeSequencerResponse = {
    if (waitForReady) node.health.wait_for_ready_for_initialization()

    consoleEnvironment.run {
      runner.adminCommand(
        InitializeFromGenesisState(
          genesisState,
          domainParameters.toInternal,
        )
      )
    }
  }

  @Help.Summary(
    "Dynamically initialize a sequencer from a point later than the beginning of the event stream." +
      "This is called as part of the sequencer.setup.onboard_new_sequencer command, so you are unlikely to need to call this directly."
  )
  def assign_from_onboarding_state(
      onboardingState: ByteString,
      waitForReady: Boolean = true,
  ): InitializeSequencerResponse = {
    if (waitForReady) node.health.wait_for_ready_for_initialization()

    consoleEnvironment.run {
      runner.adminCommand(
        InitializeFromOnboardingState(onboardingState)
      )
    }
  }

}
