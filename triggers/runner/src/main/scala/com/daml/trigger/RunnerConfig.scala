// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.trigger

import java.io.File
import java.time.Duration

import com.digitalasset.platform.services.time.TimeProviderType

case class RunnerConfig(
    darPath: File,
    // If true, we will only list the triggers in the DAR and exit.
    listTriggers: Boolean,
    triggerIdentifier: String,
    ledgerHost: String,
    ledgerPort: Int,
    ledgerParty: String,
    timeProviderType: TimeProviderType,
    commandTtl: Duration,
)

object RunnerConfig {
  private val parser = new scopt.OptionParser[RunnerConfig]("trigger-runner") {
    head("trigger-runner")

    opt[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the trigger")

    opt[String]("trigger-name")
      .action((t, c) => c.copy(triggerIdentifier = t))
      .text("Identifier of the trigger that should be run in the format Module.Name:Entity.Name")

    opt[String]("ledger-host")
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port")

    opt[String]("ledger-party")
      .action((t, c) => c.copy(ledgerParty = t))
      .text("Ledger party")

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

    cmd("list")
      .action((_, c) => c.copy(listTriggers = true))
      .text("List the triggers in the DAR.")

    checkConfig(c =>
      if (c.listTriggers) {
        // I do not want to break the trigger CLI and require a
        // "run" command so I can’t make these options required
        // in general. Therefore, we do this check in checkConfig.
        success
      } else {
        if (c.triggerIdentifier == null) {
          failure("Missing option --trigger-name")
        } else if (c.ledgerHost == null) {
          failure("Missing option --ledger-host")
        } else if (c.ledgerPort == 0) {
          failure("Missing option --ledger-port")
        } else if (c.ledgerParty == null) {
          failure("Missing option --ledger-pary")
        } else {
          success
        }
    })
  }
  def parse(args: Array[String]): Option[RunnerConfig] =
    parser.parse(
      args,
      RunnerConfig(
        darPath = null,
        listTriggers = false,
        triggerIdentifier = null,
        ledgerHost = null,
        ledgerPort = 0,
        ledgerParty = null,
        timeProviderType = TimeProviderType.Static,
        commandTtl = Duration.ofSeconds(30L),
      )
    )
}
