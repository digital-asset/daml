// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.runner.common.CliConfig
import pureconfig.generic.semiauto.deriveConvert
import pureconfig.ConfigConvert
import com.daml.ledger.runner.common.PureConfigReaderWriter.Secure._
import com.daml.ledger.sandbox.BridgeConfig.DefaultMaximumDeduplicationDuration
import scopt.OParser

import java.time.Duration

case class BridgeConfig(
    conflictCheckingEnabled: Boolean = true,
    submissionBufferSize: Int = 500,
    maxDeduplicationDuration: Duration = DefaultMaximumDeduplicationDuration,
)

object BridgeConfig {
  val DefaultMaximumDeduplicationDuration: Duration = Duration.ofMinutes(30L)
  val Default: BridgeConfig = BridgeConfig()

  implicit val Convert: ConfigConvert[BridgeConfig] = deriveConvert[BridgeConfig]

  val Parser: OParser[_, CliConfig[BridgeConfig]] = {
    val builder = OParser.builder[CliConfig[BridgeConfig]]
    OParser.sequence(
      builder
        .opt[Int]("bridge-submission-buffer-size")
        .text("Submission buffer size. Defaults to 500.")
        .action((p, c) => c.copy(extra = c.extra.copy(submissionBufferSize = p))),
      builder
        .opt[Unit]("disable-conflict-checking")
        .hidden()
        .text("Disable ledger-side submission conflict checking.")
        .action((_, c) => c.copy(extra = c.extra.copy(conflictCheckingEnabled = false))),
      builder
        .opt[Boolean](name = "implicit-party-allocation")
        .optional()
        .action((x, c) => c.copy(implicitPartyAllocation = x))
        .text(
          s"When referring to a party that doesn't yet exist on the ledger, the participant will implicitly allocate that party."
            + s" You can optionally disable this behavior to bring participant into line with other ledgers."
        ),
      builder.checkConfig(c =>
        Either.cond(
          c.maxDeduplicationDuration.forall(_.compareTo(Duration.ofHours(1L)) <= 0),
          (),
          "Maximum supported deduplication duration is one hour",
        )
      ),
    )
  }
}
