// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.runner.common

import com.daml.jwt.JwtTimestampLeeway
import com.daml.lf.engine.EngineConfig
import com.daml.lf.interpretation.Limits
import com.daml.lf.language.LanguageVersion
import com.daml.lf.transaction.ContractKeyUniquenessMode
import com.daml.lf.VersionRange
import org.scalacheck.Gen
import com.daml.ledger.api.tls.{TlsConfiguration, TlsVersion}
import com.daml.lf.data.Ref
import com.daml.platform.apiserver.{ApiServerConfig, AuthServiceConfig}
import com.daml.platform.apiserver.SeedService.Seeding
import com.daml.platform.apiserver.configuration.RateLimitingConfig
import com.daml.platform.config.{MetricsConfig, ParticipantConfig}
import com.daml.platform.config.MetricsConfig.MetricRegistryType
import com.daml.platform.configuration.{
  CommandConfiguration,
  IndexServiceConfig,
  InitialLedgerConfiguration,
}
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode, PackageMetadataViewConfig}
import com.daml.platform.indexer.ha.HaConfig
import com.daml.platform.localstore.UserManagementConfig
import com.daml.platform.services.time.TimeProviderType
import com.daml.platform.store.DbSupport
import com.daml.platform.store.DbSupport.DataSourceProperties
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue
import com.daml.ports.Port
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Paths
import java.time.Duration
import java.time.temporal.ChronoUnit
import com.daml.metrics.api.reporters.MetricsReporter

object ArbitraryConfig {
  val duration: Gen[Duration] = for {
    value <- Gen.chooseNum(0, Int.MaxValue)
    unit <- Gen.oneOf(
      List(
        ChronoUnit.NANOS,
        ChronoUnit.MICROS,
        ChronoUnit.MILLIS,
        ChronoUnit.SECONDS,
      )
    )
  } yield Duration.of(value.toLong, unit)

  val versionRange: Gen[VersionRange[LanguageVersion]] = for {
    min <- Gen.oneOf(LanguageVersion.All)
    max <- Gen.oneOf(LanguageVersion.All)
    if LanguageVersion.Ordering.compare(max, min) >= 0
  } yield VersionRange[LanguageVersion](min, max)

  val limits: Gen[Limits] = for {
    contractSignatories <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    contractObservers <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    choiceControllers <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    choiceObservers <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    transactionInputContracts <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
  } yield Limits(
    contractSignatories,
    contractObservers,
    choiceControllers,
    choiceObservers,
    transactionInputContracts,
  )

  val contractKeyUniquenessMode: Gen[ContractKeyUniquenessMode] =
    Gen.oneOf(ContractKeyUniquenessMode.Strict, ContractKeyUniquenessMode.Off)

  val engineConfig: Gen[EngineConfig] = for {
    allowedLanguageVersions <- versionRange
    packageValidation <- Gen.oneOf(true, false)
    stackTraceMode <- Gen.oneOf(true, false)
    forbidV0ContractId <- Gen.oneOf(true, false)
    requireSuffixedGlobalContractId <- Gen.oneOf(true, false)
    contractKeyUniqueness <- contractKeyUniquenessMode
    limits <- limits
  } yield EngineConfig(
    allowedLanguageVersions = allowedLanguageVersions,
    packageValidation = packageValidation,
    stackTraceMode = stackTraceMode,
    profileDir = None,
    contractKeyUniqueness = contractKeyUniqueness,
    forbidV0ContractId = forbidV0ContractId,
    requireSuffixedGlobalContractId = requireSuffixedGlobalContractId,
    limits = limits,
  )

  val inetSocketAddress = for {
    host <- Gen.alphaStr
    port <- Gen.chooseNum(1, 65535)
  } yield new InetSocketAddress(host, port)

  val graphiteReporter: Gen[MetricsReporter] = for {
    address <- inetSocketAddress
    prefixStr <- Gen.alphaStr if prefixStr.nonEmpty
    prefix <- Gen.option(prefixStr)
  } yield MetricsReporter.Graphite(address, prefix)

  val prometheusReporter: Gen[MetricsReporter] = for {
    address <- inetSocketAddress
  } yield MetricsReporter.Prometheus(address)

  val csvReporter: Gen[MetricsReporter] = for {
    path <- Gen.alphaStr
  } yield MetricsReporter.Csv(Paths.get(path).toAbsolutePath)

  val metricsReporter: Gen[MetricsReporter] =
    Gen.oneOf(graphiteReporter, prometheusReporter, csvReporter, Gen.const(MetricsReporter.Console))

  val metricRegistryType: Gen[MetricRegistryType] =
    Gen.oneOf[MetricRegistryType](MetricRegistryType.JvmShared, MetricRegistryType.New)

  val metricConfig = for {
    enabled <- Gen.oneOf(true, false)
    reporter <- metricsReporter
    reportingInterval <- Gen.finiteDuration
    registryType <- metricRegistryType
  } yield MetricsConfig(enabled, reporter, reportingInterval, registryType)

  val clientAuth = Gen.oneOf(ClientAuth.values().toList)

  val tlsVersion = Gen.oneOf(TlsVersion.allVersions)

  val tlsConfiguration = for {
    enabled <- Gen.oneOf(true, false)
    keyCertChainFile <- Gen.option(Gen.alphaStr)
    keyFile <- Gen.option(Gen.alphaStr)
    trustCertCollectionFile <- Gen.option(Gen.alphaStr)
    clientAuth <- clientAuth
    enableCertRevocationChecking <- Gen.oneOf(true, false)
    minimumServerProtocolVersion <- Gen.option(tlsVersion)
  } yield TlsConfiguration(
    enabled,
    keyCertChainFile.map(fileName => new File(fileName)),
    keyFile.map(fileName => new File(fileName)),
    trustCertCollectionFile.map(fileName => new File(fileName)),
    None,
    clientAuth,
    enableCertRevocationChecking,
    minimumServerProtocolVersion,
  )

  val port = Gen.choose(0, 65535).map(p => Port(p))

  val initialLedgerConfiguration = for {
    maxDeduplicationDuration <- duration
    avgTransactionLatency <- duration
    minSkew <- duration
    maxSkew <- duration
    delayBeforeSubmitting <- duration
    config = InitialLedgerConfiguration(
      maxDeduplicationDuration,
      avgTransactionLatency,
      minSkew,
      maxSkew,
      delayBeforeSubmitting,
    )
    optConfig <- Gen.option(config)
  } yield optConfig

  val seeding = Gen.oneOf(Seeding.Weak, Seeding.Strong, Seeding.Static)

  val userManagementConfig = for {
    enabled <- Gen.oneOf(true, false)
    maxCacheSize <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    cacheExpiryAfterWriteInSeconds <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    maxUsersPageSize <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
  } yield UserManagementConfig(
    enabled = enabled,
    maxCacheSize = maxCacheSize,
    cacheExpiryAfterWriteInSeconds = cacheExpiryAfterWriteInSeconds,
    maxUsersPageSize = maxUsersPageSize,
  )

  def jwtTimestampLeewayGen: Gen[JwtTimestampLeeway] = {
    for {
      default <- Gen.option(Gen.posNum[Long])
      expiresAt <- Gen.option(Gen.posNum[Long])
      issuedAt <- Gen.option(Gen.posNum[Long])
      notBefore <- Gen.option(Gen.posNum[Long])
    } yield JwtTimestampLeeway(
      default = default,
      expiresAt = expiresAt,
      issuedAt = issuedAt,
      notBefore = notBefore,
    )
  }

  val UnsafeJwtHmac256 = for {
    secret <- Gen.alphaStr
  } yield AuthServiceConfig.UnsafeJwtHmac256(secret)

  val JwtRs256Crt = for {
    certificate <- Gen.alphaStr
  } yield AuthServiceConfig.JwtRs256(certificate)

  val JwtEs256Crt = for {
    certificate <- Gen.alphaStr
  } yield AuthServiceConfig.JwtEs256(certificate)

  val JwtEs512Crt = for {
    certificate <- Gen.alphaStr
  } yield AuthServiceConfig.JwtEs512(certificate)

  val JwtRs256Jwks = for {
    url <- Gen.alphaStr
  } yield AuthServiceConfig.JwtRs256Jwks(url)

  val authServiceConfig = Gen.oneOf(
    Gen.const(AuthServiceConfig.Wildcard),
    UnsafeJwtHmac256,
    JwtRs256Crt,
    JwtEs256Crt,
    JwtEs512Crt,
    JwtRs256Jwks,
  )

  val commandConfiguration = for {
    inputBufferSize <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    maxCommandsInFlight <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    trackerRetentionPeriod <- duration
  } yield CommandConfiguration(
    inputBufferSize,
    maxCommandsInFlight,
    trackerRetentionPeriod,
  )

  val timeProviderType = Gen.oneOf(TimeProviderType.Static, TimeProviderType.WallClock)

  val connectionPoolConfig = for {
    connectionPoolSize <- Gen.chooseNum(0, Int.MaxValue)
    connectionTimeout <- Gen.finiteDuration
  } yield DbSupport.ConnectionPoolConfig(
    connectionPoolSize,
    connectionTimeout,
  )

  val postgresDataSourceConfig = for {
    synchronousCommit <- Gen.option(Gen.oneOf(SynchronousCommitValue.All))
    tcpKeepalivesIdle <- Gen.option(Gen.chooseNum(0, Int.MaxValue))
    tcpKeepalivesInterval <- Gen.option(Gen.chooseNum(0, Int.MaxValue))
    tcpKeepalivesCount <- Gen.option(Gen.chooseNum(0, Int.MaxValue))
  } yield PostgresDataSourceConfig(
    synchronousCommit,
    tcpKeepalivesIdle,
    tcpKeepalivesInterval,
    tcpKeepalivesCount,
  )

  val dataSourceProperties = for {
    connectionPool <- connectionPoolConfig
    postgres <- postgresDataSourceConfig
  } yield DataSourceProperties(connectionPool = connectionPool, postgres = postgres)

  val rateLimitingConfig = for {
    maxApiServicesQueueSize <- Gen.chooseNum(0, Int.MaxValue)
    maxApiServicesIndexDbQueueSize <- Gen.chooseNum(0, Int.MaxValue)
    maxUsedHeapSpacePercentage <- Gen.chooseNum(0, Int.MaxValue)
    minFreeHeapSpaceBytes <- Gen.long
    element = RateLimitingConfig(
      maxApiServicesQueueSize,
      maxApiServicesIndexDbQueueSize,
      maxUsedHeapSpacePercentage,
      minFreeHeapSpaceBytes,
    )
    optElement <- Gen.option(element)
  } yield optElement

  val apiServerConfig = for {
    address <- Gen.option(Gen.alphaStr)
    apiStreamShutdownTimeout <- Gen.finiteDuration
    command <- commandConfiguration
    configurationLoadTimeout <- Gen.finiteDuration
    initialLedgerConfiguration <- initialLedgerConfiguration
    managementServiceTimeout <- Gen.finiteDuration
    maxInboundMessageSize <- Gen.chooseNum(0, Int.MaxValue)
    port <- port
    portFile <- Gen.option(Gen.alphaStr.map(p => Paths.get(p)))
    rateLimit <- rateLimitingConfig
    seeding <- seeding
    timeProviderType <- timeProviderType
    tls <- Gen.option(tlsConfiguration)
    userManagement <- userManagementConfig
  } yield ApiServerConfig(
    address = address,
    apiStreamShutdownTimeout = apiStreamShutdownTimeout,
    command = command,
    configurationLoadTimeout = configurationLoadTimeout,
    initialLedgerConfiguration = initialLedgerConfiguration,
    managementServiceTimeout = managementServiceTimeout,
    maxInboundMessageSize = maxInboundMessageSize,
    port = port,
    portFile = portFile,
    rateLimit = rateLimit,
    seeding = seeding,
    timeProviderType = timeProviderType,
    tls = tls,
    userManagement = userManagement,
  )

  val indexerStartupMode: Gen[IndexerStartupMode] = for {
    allowExistingSchema <- Gen.oneOf(true, false)
    schemaMigrationAttempts <- Gen.chooseNum(0, Int.MaxValue)
    schemaMigrationAttemptBackoff <- Gen.finiteDuration
    value <- Gen.oneOf[IndexerStartupMode](
      IndexerStartupMode.ValidateAndStart,
      IndexerStartupMode
        .ValidateAndWaitOnly(schemaMigrationAttempts, schemaMigrationAttemptBackoff),
      IndexerStartupMode.MigrateOnEmptySchemaAndStart,
      IndexerStartupMode.MigrateAndStart(allowExistingSchema),
    )
  } yield value

  val haConfig = for {
    mainLockAcquireRetryMillis <- Gen.long
    workerLockAcquireRetryMillis <- Gen.long
    workerLockAcquireMaxRetry <- Gen.long
    mainLockCheckerPeriodMillis <- Gen.long
    indexerLockId <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    indexerWorkerLockId <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
  } yield HaConfig(
    mainLockAcquireRetryMillis,
    workerLockAcquireRetryMillis,
    workerLockAcquireMaxRetry,
    mainLockCheckerPeriodMillis,
    indexerLockId,
    indexerWorkerLockId,
  )

  val packageMetadataViewConfig = for {
    initLoadParallelism <- Gen.chooseNum(0, Int.MaxValue)
    initProcessParallelism <- Gen.chooseNum(0, Int.MaxValue)
  } yield PackageMetadataViewConfig(initLoadParallelism, initProcessParallelism)

  val indexerConfig = for {
    batchingParallelism <- Gen.chooseNum(0, Int.MaxValue)
    dataSourceProperties <- Gen.option(dataSourceProperties)
    enableCompression <- Gen.oneOf(true, false)
    highAvailability <- haConfig
    ingestionParallelism <- Gen.chooseNum(0, Int.MaxValue)
    inputMappingParallelism <- Gen.chooseNum(0, Int.MaxValue)
    maxInputBufferSize <- Gen.chooseNum(0, Int.MaxValue)
    restartDelay <- Gen.finiteDuration
    startupMode <- indexerStartupMode
    submissionBatchSize <- Gen.long
    packageMetadataViewConfig <- packageMetadataViewConfig
  } yield IndexerConfig(
    batchingParallelism = batchingParallelism,
    dataSourceProperties = dataSourceProperties,
    enableCompression = enableCompression,
    highAvailability = highAvailability,
    ingestionParallelism = ingestionParallelism,
    inputMappingParallelism = inputMappingParallelism,
    maxInputBufferSize = maxInputBufferSize,
    restartDelay = restartDelay,
    startupMode = startupMode,
    submissionBatchSize = submissionBatchSize,
    packageMetadataView = packageMetadataViewConfig,
  )

  val indexServiceConfig = for {
    eventsPageSize <- Gen.chooseNum(0, Int.MaxValue)
    eventsProcessingParallelism <- Gen.chooseNum(0, Int.MaxValue)
    bufferedStreamsPageSize <- Gen.chooseNum(0, Int.MaxValue)
    acsIdPageSize <- Gen.chooseNum(0, Int.MaxValue)
    acsIdPageBufferSize <- Gen.chooseNum(0, Int.MaxValue)
    acsIdPageWorkingMemoryBytes <- Gen.chooseNum(0, Int.MaxValue)
    acsIdFetchingParallelism <- Gen.chooseNum(0, Int.MaxValue)
    acsContractFetchingParallelism <- Gen.chooseNum(0, Int.MaxValue)
    acsGlobalParallelism <- Gen.chooseNum(0, Int.MaxValue)
    maxContractStateCacheSize <- Gen.long
    maxContractKeyStateCacheSize <- Gen.long
    maxTransactionsInMemoryFanOutBufferSize <- Gen.chooseNum(0, Int.MaxValue)
    apiStreamShutdownTimeout <- Gen.finiteDuration
  } yield IndexServiceConfig(
    eventsPageSize,
    eventsProcessingParallelism,
    bufferedStreamsPageSize,
    acsIdPageSize,
    acsIdPageBufferSize,
    acsIdPageWorkingMemoryBytes,
    acsIdFetchingParallelism,
    acsContractFetchingParallelism,
    acsGlobalParallelism,
    maxContractStateCacheSize,
    maxContractKeyStateCacheSize,
    maxTransactionsInMemoryFanOutBufferSize,
    apiStreamShutdownTimeout,
  )

  val participantConfig = for {
    apiServer <- apiServerConfig
    dataSourceProperties <- dataSourceProperties
    indexService <- indexServiceConfig
    indexer <- indexerConfig
    jwtTimestampLeeway <- Gen.option(jwtTimestampLeewayGen)
  } yield ParticipantConfig(
    apiServer = apiServer,
    authentication = AuthServiceConfig.Wildcard, // hardcoded to wildcard, as otherwise it
    // will be redacted and cannot be checked for isomorphism
    jwtTimestampLeeway = jwtTimestampLeeway,
    dataSourceProperties = dataSourceProperties,
    indexService = indexService,
    indexer = indexer,
  )

  val config = for {
    engine <- engineConfig
    ledgerId <- Gen.alphaStr
    metrics <- metricConfig
    participant <- participantConfig
  } yield Config(
    engine = engine,
    ledgerId = ledgerId,
    metrics = metrics,
    dataSource = Map.empty, // hardcoded to wildcard, as otherwise it
    participants = Map(
      Ref.ParticipantId.fromString("default").toOption.get -> participant
    ), // will be redacted and cannot be checked for isomorphism
  )

}
