// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.config

import java.io.File
import java.nio.file.Path

import ch.qos.logback.classic.Level
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.ledger.participant.state.v1.TimeModel
import com.digitalasset.ledger.api.auth.AuthService
import com.digitalasset.ledger.api.tls.TlsConfiguration
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.configuration.{CommandConfiguration, SubmissionConfiguration}
import com.digitalasset.platform.services.time.TimeProviderType
import com.digitalasset.ports.Port

/**
  * Defines the basic configuration for running sandbox
  */
final case class SandboxConfig(
    address: Option[String],
    port: Port,
    portFile: Option[Path],
    damlPackages: List[File],
    timeProviderType: Option[TimeProviderType],
    timeModel: TimeModel,
    commandConfig: CommandConfiguration, //TODO: this should go to the file config
    submissionConfig: SubmissionConfiguration,
    tlsConfig: Option[TlsConfiguration],
    scenario: Option[String],
    ledgerIdMode: LedgerIdMode,
    maxInboundMessageSize: Int,
    jdbcUrl: Option[String],
    eagerPackageLoading: Boolean,
    logLevel: Option[Level],
    authService: Option[AuthService],
    seeding: Option[Seeding],
)

object SandboxConfig {
  val DefaultPort: Port = Port(6865)

  val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024

  lazy val nextDefault: SandboxConfig =
    SandboxConfig(
      address = None,
      port = DefaultPort,
      portFile = None,
      damlPackages = Nil,
      timeProviderType = None,
      timeModel = TimeModel.reasonableDefault,
      commandConfig = CommandConfiguration.default,
      submissionConfig = SubmissionConfiguration.default,
      tlsConfig = None,
      scenario = None,
      ledgerIdMode = LedgerIdMode.Dynamic,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      jdbcUrl = None,
      eagerPackageLoading = false,
      logLevel = None, // the default is in logback.xml
      authService = None,
      seeding = Some(Seeding.Strong),
    )

  lazy val default: SandboxConfig =
    nextDefault.copy(seeding = None)
}
