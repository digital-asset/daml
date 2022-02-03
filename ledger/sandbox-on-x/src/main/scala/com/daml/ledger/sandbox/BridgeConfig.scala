// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.jwt.JwtVerifierConfigurationCli
import com.daml.ledger.api.auth.{AuthService, AuthServiceJWT, AuthServiceWildcard}
import com.daml.ledger.participant.state.kvutils.app.{Config, ConfigProvider}
import com.daml.platform.apiserver.TimeServiceBackend
import com.daml.platform.configuration.InitialLedgerConfiguration
import com.daml.platform.services.time.TimeProviderType
import scopt.OptionParser

import java.io.File
import java.nio.file.Path
import java.time.{Duration, Instant}

// TODO SoX: Keep only ledger-bridge-related configurations in this class
//           and extract the participant-specific configs in the main config file.
case class BridgeConfig(
    conflictCheckingEnabled: Boolean,
    submissionBufferSize: Int,
    implicitPartyAllocation: Boolean,
    timeProviderType: TimeProviderType,
    authService: AuthService,
    profileDir: Option[Path],
    stackTraces: Boolean,
)

object BridgeConfigProvider extends ConfigProvider[BridgeConfig] {
  override def extraConfigParser(parser: OptionParser[Config[BridgeConfig]]): Unit = {
    parser
      .opt[Int]("bridge-submission-buffer-size")
      .text("Submission buffer size. Defaults to 500.")
      .action((p, c) => c.copy(extra = c.extra.copy(submissionBufferSize = p)))

    parser
      .opt[Unit]("enable-conflict-checking")
      .text("Enables the ledger-side submission conflict checking.")
      .action((_, c) => c.copy(extra = c.extra.copy(conflictCheckingEnabled = true)))

    parser
      .opt[Boolean](name = "implicit-party-allocation")
      .optional()
      .action((x, c) => c.copy(extra = c.extra.copy(implicitPartyAllocation = x)))
      .text(
        s"When referring to a party that doesn't yet exist on the ledger, the participant will implicitly allocate that party."
          + s" You can optionally disable this behavior to bring participant into line with other ledgers."
      )

    parser
      .opt[Unit]('s', "static-time")
      .optional()
      .action((_, c) => c.copy(extra = c.extra.copy(timeProviderType = TimeProviderType.Static)))
      .text("Use static time. When not specified, wall-clock-time is used.")

    parser
      .opt[File]("profile-dir")
      .optional()
      .action((dir, config) =>
        config.copy(extra = config.extra.copy(profileDir = Some(dir.toPath)))
      )
      .text("Enable profiling and write the profiles into the given directory.")

    parser
      .opt[Boolean]("stack-traces")
      .hidden()
      .optional()
      .action((enabled, config) => config.copy(extra = config.extra.copy(stackTraces = enabled)))
      .text(
        "Enable/disable stack traces. Default is to disable them. " +
          "Enabling stack traces may have a significant performance impact."
      )

    JwtVerifierConfigurationCli.parse(parser)((v, c) =>
      c.copy(extra = c.extra.copy(authService = AuthServiceJWT(v)))
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

  override def timeServiceBackend(config: Config[BridgeConfig]): Option[TimeServiceBackend] =
    config.extra.timeProviderType match {
      case TimeProviderType.Static => Some(TimeServiceBackend.simple(Instant.EPOCH))
      case TimeProviderType.WallClock => None
    }

  override def initialLedgerConfig(config: Config[BridgeConfig]): InitialLedgerConfiguration = {
    val superConfig = super.initialLedgerConfig(config)
    superConfig.copy(configuration =
      superConfig.configuration.copy(maxDeduplicationTime = DefaultMaximumDeduplicationTime)
    )
  }

  override val defaultExtraConfig: BridgeConfig = BridgeConfig(
    // TODO SoX: Enabled by default
    conflictCheckingEnabled = false,
    submissionBufferSize = 500,
    implicitPartyAllocation = false,
    timeProviderType = TimeProviderType.WallClock,
    authService = AuthServiceWildcard,
    profileDir = None,
    stackTraces = false,
  )

  val DefaultMaximumDeduplicationTime: Duration = Duration.ofMinutes(30L)
}
