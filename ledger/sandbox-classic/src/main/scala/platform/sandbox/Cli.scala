// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.sandbox.cli.{CommonCli, SandboxCli}
import com.daml.platform.sandbox.config.SandboxConfig
import scopt.OptionParser

private[sandbox] object Cli extends SandboxCli {

  override val defaultConfig: SandboxConfig = DefaultConfig

  override protected val parser: OptionParser[SandboxConfig] = {
    val parser = new CommonCli(Name).withEarlyAccess.withDevEngine
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
      .opt[Unit]("eager-package-loading")
      .optional()
      .text(
        "Whether to load all the packages in the .dar files provided eagerly, rather than when needed as the commands come."
      )
      .action((_, config) => config.copy(eagerPackageLoading = true))
    parser
      .opt[String]("sql-backend-jdbcurl")
      .optional()
      .text(
        s"Deprecated: Use the Daml Driver for PostgreSQL if you need persistence.\nThe JDBC connection URL to a Postgres database containing the username and password as well. If present, $Name will use the database to persist its data."
      )
      .action((url, config) => config.copy(jdbcUrl = Some(url)))
    parser
      .opt[Int]("max-parallel-submissions")
      .optional()
      .action((value, config) => config.copy(maxParallelSubmissions = value))
      .text(
        s"Maximum number of successfully interpreted commands waiting to be sequenced. The threshold is shared across all parties. Overflowing it will cause back-pressure, signaled by a `RESOURCE_EXHAUSTED` error code. Default is ${defaultConfig.maxParallelSubmissions}."
      )
    parser
  }

}
