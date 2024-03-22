// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.config

import ch.qos.logback.classic.Level
import com.daml.caching.SizedCache
import com.daml.ledger.api.auth.AuthService
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.lf.data.Ref
import com.daml.metrics.MetricsReporter
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.CommandConfiguration
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.usermanagement.UserManagementConfig
import com.daml.ports.Port

import java.io.File
import java.nio.file.Path
import java.time.Duration
import scala.concurrent.duration._

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
    maxDeduplicationDuration: Option[Duration],
    // TODO Consider removing once sandbox-next is gone
    delayBeforeSubmittingLedgerConfiguration: Duration,
    timeModel: LedgerTimeModel,
    commandConfig: CommandConfiguration,
    tlsConfig: Option[TlsConfiguration],
    implicitPartyAllocation: Boolean,
    maxInboundMessageSize: Int,
    jdbcUrl: Option[String],
    databaseConnectionPoolSize: Int,
    logLevel: Option[Level],
    authService: Option[AuthService],
    seeding: Seeding,
    metricsReporter: Option[MetricsReporter],
    metricsReportingInterval: FiniteDuration,
    maxParallelSubmissions: Int, // only used by Sandbox Classic
    eventsPageSize: Int,
    eventsProcessingParallelism: Int,
    acsIdPageSize: Int,
    acsIdFetchingParallelism: Int,
    acsContractFetchingParallelism: Int,
    acsGlobalParallelism: Int,
    lfValueTranslationEventCacheConfiguration: SizedCache.Configuration,
    lfValueTranslationContractCacheConfiguration: SizedCache.Configuration,
    profileDir: Option[Path],
    stackTraces: Boolean,
    engineMode: SandboxConfig.EngineMode,
    managementServiceTimeout: Duration,
    enableCompression: Boolean,
    userManagementConfig: UserManagementConfig,
) {

  def withTlsConfig(modify: TlsConfiguration => TlsConfiguration): SandboxConfig =
    copy(tlsConfig = Some(modify(tlsConfig.getOrElse(TlsConfiguration.Empty))))

  def withUserManagementConfig(
      modify: UserManagementConfig => UserManagementConfig
  ): SandboxConfig =
    copy(userManagementConfig = modify(userManagementConfig))

}

object SandboxConfig {
  val DefaultPort: Port = Port(6865)

  val DefaultMaxInboundMessageSize: Int = 4 * 1024 * 1024

  val DefaultDatabaseConnectionPoolSize: Int = 16

  val DefaultEventsPageSize: Int = 1000
  val DefaultEventsProcessingParallelism: Int = 8
  val DefaultAcsIdPageSize: Int = 20000
  val DefaultAcsIdFetchingParallelism: Int = 2
  val DefaultAcsContractFetchingParallelism: Int = 2
  val DefaultAcsGlobalParallelism: Int = 10

  val DefaultTimeProviderType: TimeProviderType = TimeProviderType.WallClock

  val DefaultLfValueTranslationCacheConfiguration: SizedCache.Configuration =
    SizedCache.Configuration.none

  val DefaultParticipantId: Ref.ParticipantId =
    Ref.ParticipantId.assertFromString("sandbox-participant")

  val DefaultManagementServiceTimeout: Duration = Duration.ofMinutes(2)

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
      maxDeduplicationDuration = None,
      delayBeforeSubmittingLedgerConfiguration = Duration.ofSeconds(1),
      // Sandbox is a slow ledger, use a big skew to avoid failing commands under load.
      // If sandbox ever gets fast enough not to cause flakes in our tests,
      // this can be reverted to LedgerTimeModel.reasonableDefault.
      timeModel = LedgerTimeModel(
        avgTransactionLatency = Duration.ofSeconds(0L),
        minSkew = Duration.ofSeconds(120L),
        maxSkew = Duration.ofSeconds(120L),
      ).get,
      commandConfig = CommandConfiguration.Default,
      tlsConfig = None,
      implicitPartyAllocation = true,
      maxInboundMessageSize = DefaultMaxInboundMessageSize,
      jdbcUrl = None,
      databaseConnectionPoolSize = DefaultDatabaseConnectionPoolSize,
      logLevel = None, // the default is in logback.xml
      authService = None,
      seeding = Seeding.Strong,
      metricsReporter = None,
      metricsReportingInterval = 10.seconds,
      maxParallelSubmissions = 512,
      eventsPageSize = DefaultEventsPageSize,
      eventsProcessingParallelism = DefaultEventsProcessingParallelism,
      acsIdPageSize = DefaultAcsIdPageSize,
      acsIdFetchingParallelism = DefaultAcsIdFetchingParallelism,
      acsContractFetchingParallelism = DefaultAcsContractFetchingParallelism,
      acsGlobalParallelism = DefaultAcsGlobalParallelism,
      lfValueTranslationEventCacheConfiguration = DefaultLfValueTranslationCacheConfiguration,
      lfValueTranslationContractCacheConfiguration = DefaultLfValueTranslationCacheConfiguration,
      profileDir = None,
      stackTraces = true,
      engineMode = EngineMode.Stable,
      managementServiceTimeout = DefaultManagementServiceTimeout,
      enableCompression = false,
      userManagementConfig = UserManagementConfig.default(true),
    )

  sealed abstract class EngineMode extends Product with Serializable

  object EngineMode {
    final case object Stable extends EngineMode
    final case object EarlyAccess extends EngineMode
    final case object Dev extends EngineMode
  }

}
