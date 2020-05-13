// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.config

import java.io.File
import java.nio.file.Path
import java.time.Duration

import ch.qos.logback.classic.Level
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.{CommandConfiguration, LedgerConfiguration, MetricsReporter}
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
    commandConfig: CommandConfiguration,
    ledgerConfig: LedgerConfiguration,
    tlsConfig: Option[TlsConfiguration],
    scenario: Option[String],
    implicitPartyAllocation: Boolean,
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

  val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock

  lazy val nextDefault: SandboxConfig =
    SandboxConfig(
      address = None,
      port = DefaultPort,
      portFile = None,
      damlPackages = Nil,
      timeProviderType = None,
      commandConfig = CommandConfiguration.default,
      ledgerConfig = LedgerConfiguration.defaultLocalLedger,
      tlsConfig = None,
      scenario = None,
      implicitPartyAllocation = true,
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
      seeding = None,
      ledgerConfig = LedgerConfiguration.defaultLedgerBackedIndex,
    )
}
