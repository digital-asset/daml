// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.runner.common._
import com.daml.lf.engine.EngineConfig
import com.daml.lf.language.LanguageVersion
import com.daml.platform.apiserver.AuthServiceConfig
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.configuration.IndexServiceConfig
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
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
  ): CliConfig[BridgeConfig] = {
    // When missing, sandbox-classic used an in-memory ledger.
    // For Sandbox-on-X we don't offer that, so default to H2
    val serverJdbcUrl = sandboxConfig.jdbcUrl.getOrElse(defaultH2SandboxJdbcUrl())
    val singleCombinedParticipant = CliParticipantConfig(
      mode = ParticipantRunMode.Combined,
      participantId = sandboxConfig.participantId,
      address = sandboxConfig.address,
      port = sandboxConfig.port,
      portFile = sandboxConfig.portFile,
      serverJdbcUrl = serverJdbcUrl,
      managementServiceTimeout = sandboxConfig.managementServiceTimeout.toScala,
      indexerConfig = IndexerConfig(
        startupMode = IndexerStartupMode.MigrateAndStart(allowExistingSchema = false),
        inputMappingParallelism = sandboxConfig.maxParallelSubmissions,
        enableCompression = sandboxConfig.enableCompression,
      ),
      apiServerDatabaseConnectionPoolSize = sandboxConfig.databaseConnectionPoolSize,
    )

    val extraBridgeConfig = BridgeConfig(
      conflictCheckingEnabled = true,
      submissionBufferSize = sandboxConfig.maxParallelSubmissions,
      maxDeduplicationDuration = sandboxConfig.maxDeduplicationDuration.getOrElse(
        BridgeConfig.DefaultMaximumDeduplicationDuration
      ),
    )

    val allowedLanguageVersions = sandboxConfig.engineMode match {
      case EngineMode.Stable => LanguageVersion.StableVersions
      case EngineMode.EarlyAccess => LanguageVersion.EarlyAccessVersions
      case EngineMode.Dev => LanguageVersion.DevVersions
    }

    CliConfig[BridgeConfig](
      engineConfig = EngineConfig(
        allowedLanguageVersions = allowedLanguageVersions,
        profileDir = sandboxConfig.profileDir,
        stackTraceMode = sandboxConfig.stackTraces,
        forbidV0ContractId = true,
      ),
      authService = AuthServiceConfig.Wildcard,
      acsContractFetchingParallelism = sandboxConfig.acsContractFetchingParallelism,
      acsGlobalParallelism = sandboxConfig.acsGlobalParallelism,
      acsIdFetchingParallelism = sandboxConfig.acsIdFetchingParallelism,
      acsIdPageSize = sandboxConfig.acsIdPageSize,
      configurationLoadTimeout = sandboxConfig.configurationLoadTimeout,
      commandConfig = sandboxConfig.commandConfig,
      enableInMemoryFanOutForLedgerApi = false,
      eventsPageSize = sandboxConfig.eventsPageSize,
      bufferedStreamsPageSize = 100,
      eventsProcessingParallelism = sandboxConfig.eventsProcessingParallelism,
      extra = extraBridgeConfig,
      implicitPartyAllocation = sandboxConfig.implicitPartyAllocation,
      ledgerId = sandboxConfig.ledgerIdMode match {
        case LedgerIdMode.Static(ledgerId) => ledgerId.unwrap
        case LedgerIdMode.Dynamic =>
          maybeLedgerId.getOrElse(LedgerIdGenerator.generateRandomId(ledgerName).unwrap)
      },
      lfValueTranslationContractCache = sandboxConfig.lfValueTranslationContractCacheConfiguration,
      lfValueTranslationEventCache = sandboxConfig.lfValueTranslationEventCacheConfiguration,
      maxDeduplicationDuration = sandboxConfig.maxDeduplicationDuration,
      maxInboundMessageSize = sandboxConfig.maxInboundMessageSize,
      maxTransactionsInMemoryFanOutBufferSize =
        IndexServiceConfig.DefaultMaxTransactionsInMemoryFanOutBufferSize,
      metricsReporter = sandboxConfig.metricsReporter,
      metricsReportingInterval = sandboxConfig.metricsReportingInterval.toJava,
      mode = Mode.Run,
      participants = Seq(
        singleCombinedParticipant
      ),
      seeding = sandboxConfig.seeding,
      timeProviderType = sandboxConfig.timeProviderType.getOrElse(DefaultTimeProviderType),
      tlsConfig = sandboxConfig.tlsConfig,
      userManagementConfig = sandboxConfig.userManagementConfig,
    )
  }
}
