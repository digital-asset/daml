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
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerTrafficStatus,
  TimestampSelector,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.*

class TrafficControlSequencerAdministrationGroup(
    runner: AdminCommandRunner,
    override val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends TrafficControlAdministrationGroup(
      runner,
      consoleEnvironment,
      loggerFactory,
    )
    with Helpful
    with FeatureFlagFilter {

  @Help.Summary("Return the traffic state of the given members")
  @Help.Description(
    """Use this command to get the traffic state of a list of members at the latest safe timestamp."""
  )
  def traffic_state_of_members(
      members: Seq[Member]
  ): SequencerTrafficStatus = {
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(members, TimestampSelector.LatestSafe)
      )
    )
  }

  @Help.Summary("Return the traffic state of the given members at the latest approximate time")
  @Help.Description(
    """Use this command to get the traffic state of a list of members using the latest possible time the sequencer can
      estimate the state.
      CAREFUL: The returned state is only an approximation in the future and might not be the actual correct state
      by the time this timestamp is reached by the domain."""
  )
  def traffic_state_of_members_approximate(
      members: Seq[Member]
  ): SequencerTrafficStatus = {
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(members, TimestampSelector.LatestApproximate)
      )
    )
  }

  @Help.Summary("Return the last traffic state update of the given members, per member")
  @Help.Description(
    """Use this command to get the last traffic state update of each member. It will be last updated when a member consumed traffic."""
  )
  def last_traffic_state_update_of_members(
      members: Seq[Member]
  ): SequencerTrafficStatus = {
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          members,
          TimestampSelector.LastUpdatePerMember,
        )
      )
    )
  }

  @Help.Summary("Return the traffic state of the given members at a specific timestamp")
  @Help.Description(
    """Use this command to get the traffic state of specified members at a given timestamp."""
  )
  def traffic_state_of_members_at_timestamp(
      members: Seq[Member],
      timestamp: CantonTimestamp,
  ): SequencerTrafficStatus = {
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          members,
          TimestampSelector.ExactTimestamp(timestamp),
        )
      )
    )
  }

  @Help.Summary("Return the traffic state of the all members.")
  @Help.Description(
    """Use this command to get the traffic state of all members.
      Set latestApproximate to true to get an approximation of the traffic state (including base traffic)
      at the latest possible timestamp the sequencer can calculate it. This an approximation only because the sequencer
      may use its wall clock which could be beyond the domain time.
      """
  )
  def traffic_state_of_all_members(
      latestApproximate: Boolean = false
  ): SequencerTrafficStatus = {
    check(FeatureFlag.Preview)(
      consoleEnvironment.run(
        runner.adminCommand(
          SequencerAdminCommands.GetTrafficControlState(
            Seq.empty,
            if (latestApproximate) TimestampSelector.LatestApproximate
            else TimestampSelector.LatestSafe,
          )
        )
      )
    )
  }
  @Help.Summary("Set the traffic purchased entry of a member")
  @Help.Description(
    """Use this command to set the new traffic purchased entry of a member.
      | member: member for which the traffic purchased entry is to be set
      | serial: serial number of the request, must be strictly greater than the latest update made for that member
      | newBalance: new traffic purchased entry to be set
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
          SequencerAdminCommands.SetTrafficPurchased(member, serial, newBalance)
        )
      )
    )
  }
}
