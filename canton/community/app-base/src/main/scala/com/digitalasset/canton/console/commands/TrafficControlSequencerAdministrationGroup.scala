// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.SequencerAdminCommands
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReference,
}
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficStatus
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.*

class TrafficControlSequencerAdministrationGroup(
    instance: InstanceReference,
    topology: TopologyAdministrationGroupX,
    runner: AdminCommandRunner,
    override val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends TrafficControlAdministrationGroup(
      instance,
      topology,
      runner,
      consoleEnvironment,
      loggerFactory,
    )
    with Helpful
    with FeatureFlagFilter {

  @Help.Summary("Return the traffic state of the given members")
  @Help.Description(
    """Use this command to get the traffic state of a list of members."""
  )
  def traffic_state_of_members(
      members: Seq[Member]
  ): SequencerTrafficStatus = {
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(members)
      )
    )
  }

  @Help.Summary("Return the traffic state of the all members")
  @Help.Description(
    """Use this command to get the traffic state of all members."""
  )
  def traffic_state_of_all_members: SequencerTrafficStatus = {
    check(FeatureFlag.Preview)(
      consoleEnvironment.run(
        runner.adminCommand(
          SequencerAdminCommands.GetTrafficControlState(Seq.empty)
        )
      )
    )
  }
}
