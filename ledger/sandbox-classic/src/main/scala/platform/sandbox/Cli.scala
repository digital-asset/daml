// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox

import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.sandbox.cli.{CommonCli, SandboxCli}
import com.daml.platform.sandbox.config.SandboxConfig
import scopt.OptionParser

private[sandbox] object Cli extends SandboxCli {

  private val seedingMap = Map[String, Option[Seeding]](
    "no" -> None,
    "strong" -> Some(Seeding.Strong),
    "testing-weak" -> Some(Seeding.Weak),
    "testing-static" -> Some(Seeding.Static),
  )

  override val defaultConfig: SandboxConfig = DefaultConfig

  override protected val parser: OptionParser[SandboxConfig] = {
    val parser = new CommonCli(Name).parser
    parser
      .opt[String]("contract-id-seeding")
      .optional()
      .text(s"""Set the seeding of contract IDs. Possible values are ${seedingMap.keys
        .mkString(",")}. Default is "no".""")
      .validate(
        v =>
          Either.cond(
            seedingMap.contains(v.toLowerCase),
            (),
            s"seeding must be ${seedingMap.keys.mkString(",")}"))
      .action((text, config) => config.copy(seeding = seedingMap(text)))
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
  }

}
