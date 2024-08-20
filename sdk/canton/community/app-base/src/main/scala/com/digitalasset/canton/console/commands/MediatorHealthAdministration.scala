// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.admin.api.client.commands.{
  MediatorAdminCommands,
  StatusAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.MediatorStatus
import com.digitalasset.canton.console.{AdminCommandRunner, ConsoleEnvironment, FeatureFlagFilter}
import com.digitalasset.canton.logging.NamedLoggerFactory

class MediatorHealthAdministration(
    val runner: AdminCommandRunner,
    val consoleEnvironment: ConsoleEnvironment,
    override val loggerFactory: NamedLoggerFactory,
) extends HealthAdministration[MediatorStatus](
      runner,
      consoleEnvironment,
    )
    with FeatureFlagFilter {
  override protected def nodeStatusCommand
      : StatusAdminCommands.NodeStatusCommand[MediatorStatus, _, _] =
    MediatorAdminCommands.Health.MediatorStatusCommand()
}
