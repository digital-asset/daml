// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.sandbox.cli.{CommonCli, SandboxCli}
import com.daml.platform.sandbox.config.SandboxConfig
import scopt.OptionParser

private[sandbox] object Cli extends SandboxCli {

  override val defaultConfig: SandboxConfig = DefaultConfig

  override protected val parser: OptionParser[SandboxConfig] = {
    val parser = new CommonCli(Name)
      .withContractIdSeeding(
        defaultConfig,
        None,
        Some(Seeding.Strong),
        Some(Seeding.Weak),
        Some(Seeding.Static),
      )
      .parser
    parser
      .opt[String](name = "scenario")
      .optional()
      .action((x, c) => c.copy(scenario = Some(x)))
      .text(
        s"If set, $Name will execute the given scenario on startup and store all the contracts created by it.  (deprecated)" +
          " Note that when using --sql-backend-jdbcurl the scenario will be ran only if starting from a fresh database, _not_ when resuming from an existing one." +
          " Two identifier formats are supported: Module.Name:Entity.Name (preferred) and Module.Name.Entity.Name (deprecated, will print a warning when used)." +
          s" Also note that instructing $Name to load a scenario will have the side effect of loading _all_ the .dar files provided eagerly (see --eager-package-loading)."
      )
    parser
      .opt[String]("sql-backend-jdbcurl")
      .optional()
      .text(
        s"Deprecated: Use the DAML Driver for PostgreSQL if you need persistence.\nThe JDBC connection URL to a Postgres database containing the username and password as well. If present, $Name will use the database to persist its data.")
      .action((url, config) => config.copy(jdbcUrl = Some(url)))
    parser
  }

}
