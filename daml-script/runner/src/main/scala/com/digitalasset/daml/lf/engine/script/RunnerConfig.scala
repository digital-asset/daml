// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.engine.script

import java.io.File
import java.time.Duration

import com.digitalasset.platform.services.time.TimeProviderType

case class RunnerConfig(
    darPath: File,
    scriptIdentifier: String,
    ledgerHost: String,
    ledgerPort: Int,
    timeProviderType: TimeProviderType,
    commandTtl: Duration,
)

object RunnerConfig {
  private val parser = new scopt.OptionParser[RunnerConfig]("script-runner") {
    head("script-runner")

    opt[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the script")

    opt[String]("script-name")
      .required()
      .action((t, c) => c.copy(scriptIdentifier = t))
      .text("Identifier of the script that should be run in the format Module.Name:Entity.Name")

    opt[String]("ledger-host")
      .required()
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .required()
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port")

    opt[Unit]('w', "wall-clock-time")
      .action { (t, c) =>
        c.copy(timeProviderType = TimeProviderType.WallClock)
      }
      .text("Use wall clock time (UTC). When not provided, static time is used.")

    opt[Long]("ttl")
      .action { (t, c) =>
        c.copy(commandTtl = Duration.ofSeconds(t))
      }
      .text("TTL in seconds used for commands emitted by the trigger. Defaults to 30s.")
  }
  def parse(args: Array[String]): Option[RunnerConfig] =
    parser.parse(
      args,
      RunnerConfig(
        darPath = null,
        scriptIdentifier = null,
        ledgerHost = "",
        ledgerPort = 0,
        timeProviderType = TimeProviderType.Static,
        commandTtl = Duration.ofSeconds(30L),
      )
    )
}
