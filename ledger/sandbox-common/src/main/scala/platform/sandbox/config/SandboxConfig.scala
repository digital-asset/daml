// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.config

import java.io.File
import java.nio.file.Path
import java.time.Duration

import ch.qos.logback.classic.Level
import com.daml.caching.SizedCache
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.participant.state.v1
import com.daml.ledger.participant.state.v1.SeedService.Seeding
import com.daml.metrics.MetricsReporter
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.{CommandConfiguration, LedgerConfiguration}
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port
import scala.concurrent.duration._

/** Defines the basic configuration for running sandbox
  */
final case class SandboxConfig(
    address: Option[String],
    port: Port,
    portFile: Option[Path],
    ledgerIdMode: LedgerIdMode,
    participantId: v1.ParticipantId,
    damlPackages: List[File],
    timeProviderType: Option[TimeProviderType],
    commandConfig: CommandConfiguration,
    ledgerConfig: LedgerConfiguration,
    tlsConfig: Option[TlsConfiguration],
    scenario: Option[String],
    implicitPartyAllocation: Boolean,
    maxInboundMessageSize: Int,
    jdbcUrl: Option[String],
    databaseConnectionPoolSize: Int,
    eagerPackageLoading: Boolean,
    logLevel: Option[Level],
    authService: Option[AuthService],
    seeding: Option[Seeding],
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: FiniteDuration,
    eventsPageSize: Int,
    lfValueTranslationEventCacheConfiguration: SizedCache.Configuration,
    lfValueTranslationContractCacheConfiguration: SizedCache.Configuration,
    profileDir: Option[Path],
    stackTraces: Boolean,
    engineMode: SandboxConfig.EngineMode,
    managementServiceTimeout: Duration,
    sqlStartMode: Option[PostgresStartupMode],
)

object SandboxConfig {
  val DefaultPort: Port = Port(6865)

  val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024

  val DefaultDatabaseConnectionPoolSize: Int = 16

  val DefaultEventsPageSize: Int = 1000

  val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock

  val DefaultLfValueTranslationCacheConfiguration: SizedCache.Configuration =
    SizedCache.Configuration.none

  val DefaultParticipantId: v1.ParticipantId =
    v1.ParticipantId.assertFromString("sandbox-participant")

  val DefaultManagementServiceTimeout: Duration = Duration.ofMinutes(2)

  val DefaultSqlStartupMode: PostgresStartupMode = PostgresStartupMode.MigrateAndStart

  lazy val defaultConfig: SandboxConfig =
    SandboxConfig(
      address = None,
      port = DefaultPort,
      portFile = None,
      ledgerIdMode = LedgerIdMode.Dynamic,
      participantId = DefaultParticipantId,
      damlPackages = Nil,
      timeProviderType = None,
      commandConfig = CommandConfiguration.default,
      ledgerConfig = LedgerConfiguration.defaultLocalLedger,
      tlsConfig = None,
      scenario = None,
      implicitPartyAllocation = true,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      jdbcUrl = None,
      databaseConnectionPoolSize = DefaultDatabaseConnectionPoolSize,
      eagerPackageLoading = false,
      logLevel = None, // the default is in logback.xml
      authService = None,
      seeding = Some(Seeding.Strong),
      metricsReporter = None,
      metricsReportingInterval = 10.seconds,
      eventsPageSize = DefaultEventsPageSize,
      lfValueTranslationEventCacheConfiguration = DefaultLfValueTranslationCacheConfiguration,
      lfValueTranslationContractCacheConfiguration = DefaultLfValueTranslationCacheConfiguration,
      profileDir = None,
      stackTraces = true,
      engineMode = EngineMode.Stable,
      managementServiceTimeout = DefaultManagementServiceTimeout,
      sqlStartMode = Some(DefaultSqlStartupMode),
    )

  sealed abstract class EngineMode extends Product with Serializable

  object EngineMode {
    final case object Stable extends EngineMode
    final case object EarlyAccess extends EngineMode
    final case object Dev extends EngineMode
  }

}
