// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.caching
import com.daml.ledger.api.auth.AuthServiceWildcard
import com.daml.ledger.runner.common._
import com.daml.lf.language.LanguageVersion
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.config.SandboxConfig.{DefaultTimeProviderType, EngineMode}
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import scalaz.syntax.tag._

import java.util.UUID
import scala.jdk.DurationConverters._

object ConfigConverter {
  private def defaultH2SandboxJdbcUrl() =
    s"jdbc:h2:mem:sandbox-${UUID.randomUUID().toString};db_close_delay=-1"

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
      serverJdbcUrl = sandboxConfig.jdbcUrl.getOrElse(defaultH2SandboxJdbcUrl()),
      managementServiceTimeout = sandboxConfig.managementServiceTimeout,
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
      submissionBufferSize = sandboxConfig.maxParallelSubmissions,
    )

    val allowedLanguageVersions = sandboxConfig.engineMode match {
      case EngineMode.Stable => LanguageVersion.StableVersions
      case EngineMode.EarlyAccess => LanguageVersion.EarlyAccessVersions
      case EngineMode.Dev => LanguageVersion.DevVersions
    }

    Config[BridgeConfig](
      allowedLanguageVersions = allowedLanguageVersions,
      authService = sandboxConfig.authService.getOrElse(AuthServiceWildcard),
      acsContractFetchingParallelism = sandboxConfig.acsContractFetchingParallelism,
      acsGlobalParallelism = sandboxConfig.acsGlobalParallelism,
      acsIdFetchingParallelism = sandboxConfig.acsIdFetchingParallelism,
      acsIdPageSize = sandboxConfig.acsIdPageSize,
      acsIdQueueLimit = sandboxConfig.acsIdQueueLimit,
      configurationLoadTimeout = sandboxConfig.configurationLoadTimeout,
      commandConfig = sandboxConfig.commandConfig,
      enableInMemoryFanOutForLedgerApi = false,
      eventsPageSize = sandboxConfig.eventsPageSize,
      eventsProcessingParallelism = sandboxConfig.eventsProcessingParallelism,
      extra = extraBridgeConfig,
      ledgerId = sandboxConfig.ledgerIdMode match {
        case LedgerIdMode.Static(ledgerId) => ledgerId.unwrap
        case LedgerIdMode.Dynamic =>
          maybeLedgerId.getOrElse(LedgerIdGenerator.generateRandomId(ledgerName).unwrap)
      },
      lfValueTranslationContractCache = sandboxConfig.lfValueTranslationContractCacheConfiguration,
      lfValueTranslationEventCache = sandboxConfig.lfValueTranslationEventCacheConfiguration,
      maxDeduplicationDuration = sandboxConfig.maxDeduplicationDuration,
      maxInboundMessageSize = sandboxConfig.maxInboundMessageSize,
      metricsReporter = sandboxConfig.metricsReporter,
      metricsReportingInterval = sandboxConfig.metricsReportingInterval.toJava,
      mode = Mode.Run,
      participants = Seq(
        singleCombinedParticipant
      ),
      profileDir = sandboxConfig.profileDir,
      seeding = sandboxConfig.seeding,
      stackTraces = sandboxConfig.stackTraces,
      stateValueCache = caching.WeightedCache.Configuration.none,
      timeProviderType = sandboxConfig.timeProviderType.getOrElse(DefaultTimeProviderType),
      tlsConfig = sandboxConfig.tlsConfig,
      userManagementConfig = sandboxConfig.userManagementConfig,
    )
  }
}
