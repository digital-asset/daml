// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.jwt.JwtVerifierConfigurationCli
import com.daml.ledger.api.auth.{AuthService, AuthServiceJWT, AuthServiceWildcard}
import com.daml.ledger.participant.state.kvutils.app.{Config, ConfigProvider}
import com.daml.platform.apiserver.TimeServiceBackend
import com.daml.platform.services.time.TimeProviderType
import scopt.OptionParser

import java.io.File
import java.nio.file.Path
import java.time.Instant

// TODO SoX: Keep only ledger-bridge-related configurations in this class
//           and extract the participant-specific configs in the main config file.
case class BridgeConfig(
    conflictCheckingEnabled: Boolean,
    maxDedupSeconds: Int,
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
      .opt[Int]("bridge-max-dedup-seconds")
      .text("Maximum deduplication time in seconds. Defaults to 30.")
      .action((p, c) => c.copy(extra = c.extra.copy(maxDedupSeconds = p)))

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
      .text("Enable/disable stack traces. Default is to enable them.")

    JwtVerifierConfigurationCli.parse(parser)((v, c) =>
      c.copy(extra = c.extra.copy(authService = AuthServiceJWT(v)))
    )
    ()
  }

  override def timeServiceBackend(config: Config[BridgeConfig]): Option[TimeServiceBackend] =
    config.extra.timeProviderType match {
      case TimeProviderType.Static => Some(TimeServiceBackend.simple(Instant.EPOCH))
      case TimeProviderType.WallClock => None
    }

  override val defaultExtraConfig: BridgeConfig = BridgeConfig(
    // TODO SoX: Enabled by default
    conflictCheckingEnabled = false,
    maxDedupSeconds = 30,
    submissionBufferSize = 500,
    implicitPartyAllocation = false,
    timeProviderType = TimeProviderType.WallClock,
    authService = AuthServiceWildcard,
    profileDir = None,
    stackTraces = true,
  )
}
