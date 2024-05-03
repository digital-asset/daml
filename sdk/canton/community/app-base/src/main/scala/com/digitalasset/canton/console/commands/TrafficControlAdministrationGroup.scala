// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.traffic.MemberTrafficStatus

class TrafficControlAdministrationGroup(
    runner: AdminCommandRunner,
    override val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends Helpful
    with FeatureFlagFilter {

  @Help.Summary("Return the traffic state of the node")
  @Help.Description(
    """Use this command to get the traffic state of the node at a given time for a specific domain ID."""
  )
  def traffic_state(
      domainId: DomainId
  ): MemberTrafficStatus = {
    consoleEnvironment.run(
      runner.adminCommand(
        ParticipantAdminCommands.TrafficControl
          .GetTrafficControlState(domainId)
      )
    )
  }
}
