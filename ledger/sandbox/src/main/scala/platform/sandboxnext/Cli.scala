// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.sandbox.cli.CommonCli.SandboxConfigSetters
import com.daml.platform.sandbox.cli.{CommonCli, SandboxCli}
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.services.time.TimeProviderType
import scopt.OptionParser

private[sandboxnext] object Cli extends SandboxCli {

  override def defaultConfig: SandboxConfig = SandboxConfig.defaultConfig

  override protected val parser: OptionParser[SandboxConfig] = {
    val parser = new CommonCli(Name)
      .withContractIdSeeding(
        defaultConfig,
        Some(Seeding.Strong),
        Some(Seeding.Weak),
        Some(Seeding.Static),
      )
      .parser
    parser
      .opt[Unit]('s', "static-time")
      .optional()
      .action((_, c) => c.setTimeProviderType(TimeProviderType.Static))
      .text("Use static time. When not specified, wall-clock-time is used.")
    parser
      .opt[Unit]('w', "wall-clock-time")
      .optional()
      .action((_, c) => c.setTimeProviderType(TimeProviderType.WallClock))
      .text("Use wall clock time (UTC). This is the default.")
    parser
      .opt[Boolean](name = "implicit-party-allocation")
      .optional()
      .action((x, c) => c.copy(implicitPartyAllocation = x))
      .text(
        s"When referring to a party that doesn't yet exist on the ledger, $Name will implicitly allocate that party."
          + s" You can optionally disable this behavior to bring $Name into line with other ledgers."
      )
    parser
  }

}
