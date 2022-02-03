// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.caching
import com.daml.ledger.participant.state.kvutils.app._
import com.daml.lf.language.LanguageVersion
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.config.SandboxConfig.EngineMode
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import scalaz.syntax.tag._
import scala.jdk.DurationConverters._

object ConfigConverter {
  private val DefaultH2SandboxJdbcUrl = "jdbc:h2:mem:sandbox;db_close_delay=-1"

  private[sandbox] def toSandboxOnXConfig(
      sandboxConfig: SandboxConfig,
      maybeLedgerId: Option[String],
      ledgerName: LedgerName,
  ): Config[BridgeConfig] = {
    val singleCombinedParticipant = ParticipantConfig(
      mode = ParticipantRunMode.Combined,
      participantId = sandboxConfig.participantId,
      shardName = None,
      address = sandboxConfig.address,
      port = sandboxConfig.port,
      portFile = sandboxConfig.portFile,
      // When missing, sandbox-classic used an in-memory ledger.
      // For Sandbox-on-X we don't offer that, so default to H2
      serverJdbcUrl = sandboxConfig.jdbcUrl.getOrElse(DefaultH2SandboxJdbcUrl),
      managementServiceTimeout = sandboxConfig.managementServiceTimeout,
      // TODO SoX-to-sandbox-classic: Wire up all indexer configurations
      indexerConfig = ParticipantIndexerConfig(
        allowExistingSchema = true,
        inputMappingParallelism = sandboxConfig.maxParallelSubmissions,
        enableCompression = sandboxConfig.enableCompression,
        databaseConnectionTimeout = sandboxConfig.databaseConnectionTimeout,
      ),
      apiServerDatabaseConnectionPoolSize = sandboxConfig.databaseConnectionPoolSize,
    )

    val extraBridgeConfig = BridgeConfig(
      conflictCheckingEnabled = true,
      implicitPartyAllocation = sandboxConfig.implicitPartyAllocation,
      authService =
        sandboxConfig.authService.getOrElse(BridgeConfigProvider.defaultExtraConfig.authService),
      timeProviderType =
        sandboxConfig.timeProviderType.getOrElse(SandboxConfig.DefaultTimeProviderType),
      // TODO SoX-to-sandbox-classic: Dedicated submissionBufferSize CLI param for sanbox-classic
      submissionBufferSize = sandboxConfig.maxParallelSubmissions,
      // TODO SoX-to-sandbox-classic: Dedicated submissionBufferSize CLI param for sanbox-classic
      profileDir = sandboxConfig.profileDir,
      stackTraces = sandboxConfig.stackTraces,
    )

    val allowedLanguageVersions = sandboxConfig.engineMode match {
      case EngineMode.Stable => LanguageVersion.StableVersions
      case EngineMode.EarlyAccess => LanguageVersion.EarlyAccessVersions
      case EngineMode.Dev => LanguageVersion.DevVersions
    }

    Config[BridgeConfig](
      mode = Mode.Run,
      ledgerId = sandboxConfig.ledgerIdMode match {
        case LedgerIdMode.Static(ledgerId) => ledgerId.unwrap
        case LedgerIdMode.Dynamic =>
          maybeLedgerId.getOrElse(LedgerIdGenerator.generateRandomId(ledgerName).unwrap)
      },
      commandConfig = sandboxConfig.commandConfig,
      tlsConfig = sandboxConfig.tlsConfig,
      participants = Seq(
        singleCombinedParticipant
      ),
      maxInboundMessageSize = sandboxConfig.maxInboundMessageSize,
      configurationLoadTimeout = sandboxConfig.configurationLoadTimeout,
      eventsPageSize = sandboxConfig.eventsPageSize,
      eventsProcessingParallelism = sandboxConfig.eventsProcessingParallelism,
      acsIdPageSize = sandboxConfig.acsIdPageSize,
      acsIdFetchingParallelism = sandboxConfig.acsIdFetchingParallelism,
      acsContractFetchingParallelism = sandboxConfig.acsContractFetchingParallelism,
      acsGlobalParallelism = sandboxConfig.acsGlobalParallelism,
      acsIdQueueLimit = sandboxConfig.acsIdQueueLimit,
      stateValueCache = caching.WeightedCache.Configuration.none,
      lfValueTranslationEventCache = sandboxConfig.lfValueTranslationEventCacheConfiguration,
      lfValueTranslationContractCache = sandboxConfig.lfValueTranslationContractCacheConfiguration,
      seeding = sandboxConfig.seeding,
      metricsReporter = sandboxConfig.metricsReporter,
      metricsReportingInterval = sandboxConfig.metricsReportingInterval.toJava,
      allowedLanguageVersions = allowedLanguageVersions,
      // TODO SoX-to-sandbox-classic: Add configurable flag for sandbox-classic
      enableInMemoryFanOutForLedgerApi = false,
      maxDeduplicationDuration = sandboxConfig.maxDeduplicationDuration,
      extra = extraBridgeConfig,
      enableSelfServiceErrorCodes = sandboxConfig.enableSelfServiceErrorCodes,
      userManagementConfig = sandboxConfig.userManagementConfig,
    )
  }
}
