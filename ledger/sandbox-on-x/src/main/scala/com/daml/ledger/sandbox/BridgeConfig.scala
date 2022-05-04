// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.runner.common.{ConfigProvider, LegacyCliConfig}
import com.daml.ledger.sandbox.BridgeConfigProvider.DefaultMaximumDeduplicationDuration
import com.daml.platform.configuration.InitialLedgerConfiguration
import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}
import scopt.OptionParser

import java.time.Duration

case class BridgeConfig(
    conflictCheckingEnabled: Boolean,
    submissionBufferSize: Int,
)

object BridgeConfig {
  implicit val bridgeConfigReader: ConfigReader[BridgeConfig] = deriveReader[BridgeConfig]
  implicit val bridgeConfigWriter: ConfigWriter[BridgeConfig] = deriveWriter[BridgeConfig]
}

class BridgeConfigProvider extends ConfigProvider[BridgeConfig] {

  override def extraConfigParser(parser: OptionParser[LegacyCliConfig[BridgeConfig]]): Unit = {
    parser
      .opt[Int]("bridge-submission-buffer-size")
      .text("Submission buffer size. Defaults to 500.")
      .action((p, c) => c.copy(extra = c.extra.copy(submissionBufferSize = p)))

    parser
      .opt[Unit]("disable-conflict-checking")
      .hidden()
      .text("Disable ledger-side submission conflict checking.")
      .action((_, c) => c.copy(extra = c.extra.copy(conflictCheckingEnabled = false)))

    parser
      .opt[Boolean](name = "implicit-party-allocation")
      .optional()
      .action((x, c) => c.copy(implicitPartyAllocation = x))
      .text(
        s"When referring to a party that doesn't yet exist on the ledger, the participant will implicitly allocate that party."
          + s" You can optionally disable this behavior to bring participant into line with other ledgers."
      )

    parser.checkConfig(c =>
      Either.cond(
        c.maxDeduplicationDuration.forall(_.compareTo(Duration.ofHours(1L)) <= 0),
        (),
        "Maximum supported deduplication duration is one hour",
      )
    )
    ()
  }

  override def initialLedgerConfig(
      maxDeduplicationDuration: Option[Duration]
  ): InitialLedgerConfiguration = {
    val superConfig = super.initialLedgerConfig(maxDeduplicationDuration)
    superConfig.copy(maxDeduplicationDuration = DefaultMaximumDeduplicationDuration)
  }

  override val defaultExtraConfig: BridgeConfig = BridgeConfig(
    conflictCheckingEnabled = true,
    submissionBufferSize = 500,
  )

}

object BridgeConfigProvider {
  val DefaultMaximumDeduplicationDuration: Duration = Duration.ofMinutes(30L)
}
