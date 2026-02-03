// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.SequencerAdminCommands
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.sequencer.traffic.{
  SequencerTrafficStatus,
  TimestampSelector,
}
import com.digitalasset.canton.topology.*
import com.google.protobuf.ByteString

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
    """Use this command to get the traffic state of a list of members at the latest safe
      |timestamp.
      """
  )
  def traffic_state_of_members(
      members: Seq[Member]
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(members, TimestampSelector.LatestSafe)
      )
    )

  @Help.Summary("Return the traffic state of the given members at the latest approximate time")
  @Help.Description(
    """Use this command to get the traffic state of a list of members using the latest possible
      |time the sequencer can estimate the state.
      |
      |CAREFUL: The returned state is only an approximation in the future and might not be the
      |actual correct state by the time this timestamp is reached by the synchronizer.
      """
  )
  def traffic_state_of_members_approximate(
      members: Seq[Member]
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          members,
          TimestampSelector.LatestApproximate,
        )
      )
    )

  @Help.Summary("Return the last traffic state update of the given members, per member")
  @Help.Description(
    """Use this command to get the last traffic state update of each member. It will be last
      |updated when a member consumed traffic.
      """
  )
  def last_traffic_state_update_of_members(
      members: Seq[Member]
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          members,
          TimestampSelector.LastUpdatePerMember,
        )
      )
    )

  @Help.Summary("Return the traffic state of the given members at a specific timestamp")
  @Help.Description(
    """Use this command to get the traffic state of specified members at a given timestamp."""
  )
  // TODO(#25930): Make this command return what its name suggests, i.e., the traffic state exactly at the given timestamp.
  def traffic_state_of_members_at_timestamp(
      members: Seq[Member],
      timestamp: CantonTimestamp,
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          members,
          TimestampSelector.ExactTimestamp(timestamp),
        )
      )
    )

  @Help.Summary("Return the traffic state of the all members")
  @Help.Description(
    """Use this command to get the traffic state of all members.
      |Set latestApproximate to true to get an approximation of the traffic state (including
      |base traffic) at the latest possible timestamp the sequencer can calculate it. This an
      |approximation only because the sequencer may use its wall clock which could be beyond
      |the synchronizer time.
      """
  )
  def traffic_state_of_all_members(
      latestApproximate: Boolean = false
  ): SequencerTrafficStatus =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.GetTrafficControlState(
          Seq.empty,
          if (latestApproximate) TimestampSelector.LatestApproximate
          else TimestampSelector.LatestSafe,
        )
      )
    )

  @Help.Summary("Set the traffic purchased entry of a member")
  @Help.Description(
    """Use this command to set the new traffic purchased entry of a member.
      |
      |Parameters:
      |- member: Member for which the traffic purchased entry is to be set.
      |- serial: Serial number of the request, must be strictly greater than the latest update
      |  made for that member.
      |- newBalance: New traffic purchased entry to be set.
      |
      |Returns: The max sequencing time used for the update. After and only after that time,
      |if the new balance still does not appear in the traffic state, the update can be
      |considered failed and should be retried.
      """
  )
  def set_traffic_balance(
      member: Member,
      serial: PositiveInt,
      newBalance: NonNegativeLong,
  ): Unit =
    consoleEnvironment.run(
      runner.adminCommand(
        SequencerAdminCommands.SetTrafficPurchased(member, serial, newBalance)
      )
    )

  // TODO(#30533): Make sure the naming is consistent with other LSU commands
  @Help.Summary(
    "Get the traffic control state to be transferred to a new sequencer during a logical synchronizer upgrade"
  )
  @Help.Description(
    """Use this command on the old synchronizer to get the input for `set_lsu_state`
      | to be run on the new synchronizer.
      | A logical synchronizer upgrade must be ongoing and sequencer must have reached
      | the upgrade time for this call to succeed.
      """
  )
  def get_lsu_state(): ByteString =
    consoleEnvironment.run(
      runner.adminCommand(SequencerAdminCommands.GetLSUTrafficControlState)
    )

  // TODO(#30533): Make sure the naming is consistent with other LSU commands
  @Help.Summary(
    "Set the traffic control state on a fresh sequencer during a logical synchronizer upgrade"
  )
  @Help.Description(
    """Use this command on the new synchronizer with an output of
      | `get_lsu_traffic_control_state` from the old synchronizer.
      | A logical synchronizer upgrade must be ongoing and the command must be called only once
      | on the successor synchronizer sequencer.
      | Parameters:
      | - memberTraffic: ByteString produced by `get_lsu_state`.
      """
  )
  def set_lsu_state(
      memberTraffic: ByteString
  ): Unit =
    consoleEnvironment.run(
      runner.adminCommand(SequencerAdminCommands.SetLSUTrafficControlState(memberTraffic))
    )

}
