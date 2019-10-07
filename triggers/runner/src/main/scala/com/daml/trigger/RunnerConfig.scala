// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.trigger

import java.io.File

case class RunnerConfig(
    darPath: File,
    triggerIdentifier: String,
    ledgerHost: String,
    ledgerPort: Int,
    ledgerParty: String)

object RunnerConfig {
  private val parser = new scopt.OptionParser[RunnerConfig]("trigger-runner") {
    head("trigger-runner")

    opt[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the trigger")

    opt[String]("trigger-name")
      .required()
      .action((t, c) => c.copy(triggerIdentifier = t))
      .text("Identifier of the trigger that should be run in the format Module.Name:Entity.Name")

    opt[String]("ledger-host")
      .required()
      .action((t, c) => c.copy(ledgerHost = t))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .required()
      .action((t, c) => c.copy(ledgerPort = t))
      .text("Ledger port")

    opt[String]("ledger-party")
      .required()
      .action((t, c) => c.copy(ledgerParty = t))
      .text("Ledger party")
  }
  def parse(args: Array[String]): Option[RunnerConfig] =
    parser.parse(
      args,
      RunnerConfig(
        darPath = null,
        triggerIdentifier = null,
        ledgerHost = "",
        ledgerPort = 0,
        ledgerParty = ""))
}
