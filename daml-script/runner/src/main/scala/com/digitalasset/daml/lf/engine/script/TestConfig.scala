// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.script

import java.io.File
import java.time.Duration

import com.daml.platform.services.time.TimeProviderType

case class TestConfig(
    darPath: File,
    ledgerHost: Option[String],
    ledgerPort: Option[Int],
    participantConfig: Option[File],
    timeProviderType: TimeProviderType,
    commandTtl: Duration,
    maxInboundMessageSize: Int,
)

object TestConfig {
  private val parser = new scopt.OptionParser[TestConfig]("test-script") {
    head("test-script")

    opt[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the script")

    opt[String]("ledger-host")
      .optional()
      .action((t, c) => c.copy(ledgerHost = Some(t)))
      .text("Ledger hostname")

    opt[Int]("ledger-port")
      .optional()
      .action((t, c) => c.copy(ledgerPort = Some(t)))
      .text("Ledger port")

    opt[File]("participant-config")
      .optional()
      .action((t, c) => c.copy(participantConfig = Some(t)))
      .text("File containing the participant configuration in JSON format")

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

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${RunnerConfig.DefaultMaxInboundMessageSize}")

    help("help").text("Print this usage text")

    checkConfig(c => {
      if (c.ledgerHost.isDefined != c.ledgerPort.isDefined) {
        failure("Must specify both --ledger-host and --ledger-port")
      } else if (c.ledgerHost.isDefined && c.participantConfig.isDefined) {
        failure("Cannot specify both --ledger-host and --participant-config")
      } else {
        success
      }
    })
  }
  def parse(args: Array[String]): Option[TestConfig] =
    parser.parse(
      args,
      TestConfig(
        darPath = null,
        ledgerHost = None,
        ledgerPort = None,
        participantConfig = None,
        timeProviderType = TimeProviderType.Static,
        commandTtl = Duration.ofSeconds(30L),
        maxInboundMessageSize = RunnerConfig.DefaultMaxInboundMessageSize,
      )
    )
}
