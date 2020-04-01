// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.config

import java.io.File
import java.nio.file.Path
import java.time.Duration

import ch.qos.logback.classic.Level
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.ledger.participant.state.v1.TimeModel
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.{
  CommandConfiguration,
  MetricsReporter,
  PartyConfiguration,
  SubmissionConfiguration
}
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port

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
    partyConfig: PartyConfiguration,
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
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: Duration,
    eventsPageSize: Int,
)

object SandboxConfig {
  val DefaultPort: Port = Port(6865)

  val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024

  val DefaultEventsPageSize: Int = 1000

  lazy val nextDefault: SandboxConfig =
    SandboxConfig(
      address = None,
      port = DefaultPort,
      portFile = None,
      damlPackages = Nil,
      timeProviderType = None,
      timeModel = TimeModel.reasonableDefault,
      commandConfig = CommandConfiguration.default,
      partyConfig = PartyConfiguration.default.copy(
        implicitPartyAllocation = true,
      ),
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
      metricsReporter = None,
      metricsReportingInterval = Duration.ofSeconds(10),
      eventsPageSize = DefaultEventsPageSize,
    )

  lazy val default: SandboxConfig =
    nextDefault.copy(
      partyConfig = nextDefault.partyConfig.copy(
        // In Sandbox, parties are always allocated implicitly. Enabling this would result in an
        // extra `writeService.allocateParty` call, which is unnecessary and bad for performance.
        implicitPartyAllocation = false,
      ),
      seeding = None,
    )
}
