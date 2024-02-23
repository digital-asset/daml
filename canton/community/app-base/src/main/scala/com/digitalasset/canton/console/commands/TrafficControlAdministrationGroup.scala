// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveLong}
import com.digitalasset.canton.console.{
  AdminCommandRunner,
  ConsoleEnvironment,
  FeatureFlagFilter,
  Help,
  Helpful,
  InstanceReference,
}
import com.digitalasset.canton.crypto.Fingerprint
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransactionX,
  TopologyChangeOpX,
  TrafficControlStateX,
}
import com.digitalasset.canton.traffic.MemberTrafficStatus

class TrafficControlAdministrationGroup(
    instance: InstanceReference,
    topology: TopologyAdministrationGroup,
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

  @Help.Summary("Top up traffic for this node")
  @Help.Description(
    """Use this command to update the new total traffic limit for the node."""
  )
  def top_up(
      domainId: DomainId,
      newTotalTrafficAmount: PositiveLong,
      member: Member = instance.id.member,
      serial: Option[PositiveInt] = None,
      signedBy: Option[Fingerprint] = Some(instance.id.uid.namespace.fingerprint),
      synchronize: Option[config.NonNegativeDuration] = Some(
        consoleEnvironment.commandTimeouts.bounded
      ),
  ): SignedTopologyTransactionX[TopologyChangeOpX, TrafficControlStateX] = {
    topology.traffic_control.top_up(
      domainId,
      newTotalTrafficAmount,
      member,
      serial,
      signedBy,
      synchronize,
    )
  }

}
