// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import java.io.File
import java.nio.file.Path

import ch.qos.logback.classic.Level
import com.daml.ledger.participant.state.v1.TimeModel
import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.configuration.CommandConfiguration
import com.digitalasset.platform.services.time.TimeProviderType

/**
  * Defines the basic configuration for running sandbox
  */
final case class SandboxConfig(
    address: Option[String],
    port: Int,
    portFile: Option[Path],
    damlPackages: List[File],
    timeProviderType: TimeProviderType,
    timeModel: TimeModel,
    commandConfig: CommandConfiguration, //TODO: this should go to the file config
    tlsConfig: Option[TlsConfiguration],
    scenario: Option[String],
    ledgerIdMode: LedgerIdMode,
    maxInboundMessageSize: Int,
    jdbcUrl: Option[String],
    eagerPackageLoading: Boolean,
    logLevel: Level,
    authService: Option[AuthService],
    useSortableCid: Boolean
)

object SandboxConfig {
  val DefaultPort = 6865

  val DefaultMaxInboundMessageSize = 4194304

  lazy val default =
    SandboxConfig(
      None,
      DefaultPort,
      None,
      Nil,
      TimeProviderType.Static,
      TimeModel.reasonableDefault,
      CommandConfiguration.default,
      tlsConfig = None,
      scenario = None,
      ledgerIdMode = LedgerIdMode.Dynamic(),
      jdbcUrl = None,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      eagerPackageLoading = false,
      logLevel = Level.INFO,
      authService = None,
      useSortableCid = false
    )
}
