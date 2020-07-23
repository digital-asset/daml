// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext

import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.sandbox.cli.{CommonCli, SandboxCli}
import com.daml.platform.sandbox.config.SandboxConfig
import scopt.OptionParser

private[sandboxnext] object Cli extends SandboxCli {

  private val seedingMap = Map[String, Option[Seeding]](
    "strong" -> Some(Seeding.Strong),
    "testing-weak" -> Some(Seeding.Weak),
    "testing-static" -> Some(Seeding.Static),
  )

  override def defaultConfig: SandboxConfig = SandboxConfig.defaultConfig

  override protected val parser: OptionParser[SandboxConfig] = {
    val parser = new CommonCli(Name).parser
    parser
      .opt[String]("contract-id-seeding")
      .optional()
      .text(s"""Set the seeding of contract IDs. Possible values are ${seedingMap.keys
        .mkString(",")}. Default is "strong".""")
      .validate(
        v =>
          Either.cond(
            seedingMap.contains(v.toLowerCase),
            (),
            s"seeding must be ${seedingMap.keys.mkString(",")}"))
      .action((text, config) => config.copy(seeding = seedingMap(text)))
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
