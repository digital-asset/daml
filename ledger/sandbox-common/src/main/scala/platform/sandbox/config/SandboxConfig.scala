// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.config

import ch.qos.logback.classic.Level
import com.daml.caching.SizedCache
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}
import com.daml.lf.data.Ref
import com.daml.metrics.MetricsReporter
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.{
  CommandConfiguration,
  InitialLedgerConfiguration,
  SubmissionConfiguration,
}
import com.daml.platform.services.time.TimeProviderType
import com.daml.ports.Port

import java.io.File
import java.nio.file.Path
import java.time.Duration
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** Defines the basic configuration for running sandbox
  */
final case class SandboxConfig(
    address: Option[String],
    port: Port,
    portFile: Option[Path],
    ledgerIdMode: LedgerIdMode,
    participantId: Ref.ParticipantId,
    damlPackages: List[File],
    timeProviderType: Option[TimeProviderType],
    configurationLoadTimeout: Duration,
    delayBeforeSubmittingLedgerConfiguration: Duration,
    timeModel: LedgerTimeModel,
    commandConfig: CommandConfiguration,
    submissionConfig: SubmissionConfiguration,
    tlsConfig: Option[TlsConfiguration],
    scenario: Option[String],
    implicitPartyAllocation: Boolean,
    maxInboundMessageSize: Int,
    jdbcUrl: Option[String],
    databaseConnectionPoolSize: Int,
    databaseConnectionTimeout: FiniteDuration,
    eagerPackageLoading: Boolean,
    logLevel: Option[Level],
    authService: Option[AuthService],
    seeding: Option[Seeding],
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: FiniteDuration,
    eventsPageSize: Int,
    eventsProcessingParallelism: Int,
    lfValueTranslationEventCacheConfiguration: SizedCache.Configuration,
    lfValueTranslationContractCacheConfiguration: SizedCache.Configuration,
    profileDir: Option[Path],
    stackTraces: Boolean,
    engineMode: SandboxConfig.EngineMode,
    managementServiceTimeout: Duration,
    sqlStartMode: Option[PostgresStartupMode],
    enableAppendOnlySchema: Boolean,
    enableCompression: Boolean,
) {

  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): SandboxConfig =
    copy(tlsConfig = Some(modify(tlsConfig.getOrElse(TlsConfiguration.Empty))))

  lazy val initialLedgerConfiguration: InitialLedgerConfiguration =
    InitialLedgerConfiguration(
      Configuration.reasonableInitialConfiguration.copy(timeModel = timeModel),
      delayBeforeSubmittingLedgerConfiguration,
    )
}

object SandboxConfig {
  val DefaultPort: Port = Port(6865)

  val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024

  val DefaultDatabaseConnectionPoolSize: Int = 16
  val DefaultDatabaseConnectionTimeout: FiniteDuration = 250.millis

  val DefaultEventsPageSize: Int = 1000
  val DefaultEventsProcessingParallelism: Int = 8

  val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock

  val DefaultLfValueTranslationCacheConfiguration: SizedCache.Configuration =
    SizedCache.Configuration.none

  val DefaultParticipantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("sandbox-participant")

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
      configurationLoadTimeout = Duration.ofSeconds(10),
      delayBeforeSubmittingLedgerConfiguration = Duration.ofSeconds(1),
      timeModel = LedgerTimeModel.reasonableDefault,
      commandConfig = CommandConfiguration.default,
      submissionConfig = SubmissionConfiguration.default,
      tlsConfig = None,
      scenario = None,
      implicitPartyAllocation = true,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      jdbcUrl = None,
      databaseConnectionPoolSize = DefaultDatabaseConnectionPoolSize,
      databaseConnectionTimeout = DefaultDatabaseConnectionTimeout,
      eagerPackageLoading = false,
      logLevel = None, // the default is in logback.xml
      authService = None,
      seeding = Some(Seeding.Strong),
      metricsReporter = None,
      metricsReportingInterval = 10.seconds,
      eventsPageSize = DefaultEventsPageSize,
      eventsProcessingParallelism = DefaultEventsProcessingParallelism,
      lfValueTranslationEventCacheConfiguration = DefaultLfValueTranslationCacheConfiguration,
      lfValueTranslationContractCacheConfiguration = DefaultLfValueTranslationCacheConfiguration,
      profileDir = None,
      stackTraces = true,
      engineMode = EngineMode.Stable,
      managementServiceTimeout = DefaultManagementServiceTimeout,
      sqlStartMode = Some(DefaultSqlStartupMode),
      enableAppendOnlySchema = false,
      enableCompression = false,
    )

  sealed abstract class EngineMode extends Product with Serializable

  object EngineMode {
    final case object Stable extends EngineMode
    final case object EarlyAccess extends EngineMode
    final case object Dev extends EngineMode
  }

}
