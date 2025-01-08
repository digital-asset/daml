// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlagFilter,
  Help,
  Helpful,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.sequencing.protocol.TrafficState
import com.digitalasset.canton.topology.*

class TrafficControlAdministrationGroup(
    runner: AdminCommandRunner,
    override val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends Helpful
    with FeatureFlagFilter {

  @Help.Summary("Return the traffic state of the node")
  @Help.Description(
    """Use this command to get the traffic state of the node at a given time for a specific synchronizer id."""
  )
  def traffic_state(
      synchronizerId: SynchronizerId
  ): TrafficState =
    consoleEnvironment.run(
      runner.adminCommand(
        ParticipantAdminCommands.TrafficControl
          .GetTrafficControlState(synchronizerId)
      )
    )
}
