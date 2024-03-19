// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.SequencerAdminCommands
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlag,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReference,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerTrafficStatus
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.*

class TrafficControlSequencerAdministrationGroup(
    instance: InstanceReference,
    topology: TopologyAdministrationGroup,
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
  @Help.Summary("Set the traffic balance of a member")
  @Help.Description(
    """Use this command to set the new traffic balance of a member.
      | member: member for which the traffic balance is to be set
      | serial: serial number of the request, must be strictly greater than the latest update made for that member
      | newBalance: new traffic balance to be set
      |
      | returns: the max sequencing time used for the update
      | After and only after that time, if the new balance still does not appear in the traffic state,
      |  the update can be considered failed and should be retried.
      """
  )
  def set_traffic_balance(
      member: Member,
      serial: PositiveInt,
      newBalance: NonNegativeLong,
  ): Option[CantonTimestamp] = {
    check(FeatureFlag.Preview)(
      consoleEnvironment.run(
        runner.adminCommand(
          SequencerAdminCommands.SetTrafficBalance(member, serial, newBalance)
        )
      )
    )
  }
}
