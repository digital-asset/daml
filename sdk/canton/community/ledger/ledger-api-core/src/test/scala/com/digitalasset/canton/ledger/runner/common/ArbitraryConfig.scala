// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import com.daml.jwt.JwtTimestampLeeway
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.platform.apiserver.configuration.RateLimitingConfig
import com.digitalasset.canton.platform.config.{IdentityProviderManagementConfig, *}
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.DbSupport.DataSourceProperties
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.digitalasset.canton.platform.store.backend.postgresql.PostgresDataSourceConfig.SynchronousCommitValue
import com.digitalasset.daml.lf.VersionRange
import com.digitalasset.daml.lf.interpretation.Limits
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.ContractKeyUniquenessMode
import io.netty.handler.ssl.ClientAuth
import org.scalacheck.Gen

import java.net.InetSocketAddress
import java.time.Duration
import java.time.temporal.ChronoUnit

object ArbitraryConfig {

  val nonNegativeIntGen: Gen[NonNegativeInt] =
    Gen.chooseNum(0, Int.MaxValue).map(NonNegativeInt.tryCreate)

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

  val nonNegativeFiniteDurationGen: Gen[NonNegativeFiniteDuration] =
    duration.map(NonNegativeFiniteDuration.tryFromJavaDuration)

  val versionRange: Gen[VersionRange[LanguageVersion]] = for {
    min <- Gen.oneOf(LanguageVersion.AllV2)
    max <- Gen.oneOf(LanguageVersion.AllV2)
    if LanguageVersion.Ordering.compare(max, min) >= 0
  } yield VersionRange[LanguageVersion](min, max)

  val limits: Gen[Limits] = for {
    contractSignatories <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    contractObservers <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    choiceControllers <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    choiceObservers <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    choiceAuthorizers <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    transactionInputContracts <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
  } yield Limits(
    contractSignatories,
    contractObservers,
    choiceControllers,
    choiceObservers,
    choiceAuthorizers,
    transactionInputContracts,
  )

  val contractKeyUniquenessMode: Gen[ContractKeyUniquenessMode] =
    Gen.oneOf(ContractKeyUniquenessMode.Strict, ContractKeyUniquenessMode.Off)

  val inetSocketAddress = for {
    host <- Gen.alphaStr
    port <- Gen.chooseNum(1, 65535)
  } yield new InetSocketAddress(host, port)

  val clientAuth = Gen.oneOf(ClientAuth.values().toList)

  val port = Gen.choose(0, 65535).map(p => Port.tryCreate(p))

  val userManagementServiceConfig = for {
    enabled <- Gen.oneOf(true, false)
    maxCacheSize <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    cacheExpiryAfterWriteInSeconds <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    maxUsersPageSize <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
  } yield UserManagementServiceConfig(
    enabled = enabled,
    maxCacheSize = maxCacheSize,
    cacheExpiryAfterWriteInSeconds = cacheExpiryAfterWriteInSeconds,
    maxUsersPageSize = maxUsersPageSize,
  )

  val identityProviderManagementConfig = for {
    cacheExpiryAfterWrite <- nonNegativeFiniteDurationGen
  } yield IdentityProviderManagementConfig(
    cacheExpiryAfterWrite = cacheExpiryAfterWrite
  )

  def jwtTimestampLeewayGen: Gen[JwtTimestampLeeway] =
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

  val commandServiceConfig = for {
    maxCommandsInFlight <- Gen.chooseNum(Int.MinValue, Int.MaxValue)
    maxTrackingTimeout <- duration
  } yield CommandServiceConfig(
    NonNegativeFiniteDuration(maxTrackingTimeout),
    maxCommandsInFlight,
  )

  val connectionPoolConfig = for {
    connectionPoolSize <- Gen.chooseNum(0, Int.MaxValue)
    connectionTimeout <- Gen.finiteDuration
  } yield DbSupport.ConnectionPoolConfig(
    connectionPoolSize,
    connectionTimeout,
  )

  val postgresDataSourceConfig = for {
    synchronousCommit <- Gen.option(Gen.oneOf(SynchronousCommitValue.All))
    tcpKeepalivesIdle <- Gen.chooseNum(0, Int.MaxValue)
    tcpKeepalivesInterval <- Gen.chooseNum(0, Int.MaxValue)
    tcpKeepalivesCount <- Gen.chooseNum(0, Int.MaxValue)
  } yield PostgresDataSourceConfig(
    synchronousCommit,
    Some(tcpKeepalivesIdle),
    Some(tcpKeepalivesInterval),
    Some(tcpKeepalivesCount),
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

  val indexerConfig = for {
    batchingParallelism <- nonNegativeIntGen
    enableCompression <- Gen.oneOf(true, false)
    ingestionParallelism <- nonNegativeIntGen
    inputMappingParallelism <- nonNegativeIntGen
    maxInputBufferSize <- nonNegativeIntGen
    restartDelay <- nonNegativeFiniteDurationGen
    submissionBatchSize <- Gen.long
  } yield IndexerConfig(
    batchingParallelism = batchingParallelism,
    enableCompression = enableCompression,
    ingestionParallelism = ingestionParallelism,
    inputMappingParallelism = inputMappingParallelism,
    maxInputBufferSize = maxInputBufferSize,
    restartDelay = restartDelay,
    submissionBatchSize = submissionBatchSize,
  )

  def genActiveContractsServiceStreamConfig: Gen[ActiveContractsServiceStreamsConfig] =
    for {
      eventsPageSize <- Gen.chooseNum(0, Int.MaxValue)
      acsIdPageSize <- Gen.chooseNum(0, Int.MaxValue)
      acsIdPageBufferSize <- Gen.chooseNum(0, Int.MaxValue)
      acsIdPageWorkingMemoryBytes <- Gen.chooseNum(0, Int.MaxValue)
      acsIdFetchingParallelism <- Gen.chooseNum(0, Int.MaxValue)
      acsContractFetchingParallelism <- Gen.chooseNum(0, Int.MaxValue)
    } yield ActiveContractsServiceStreamsConfig(
      maxIdsPerIdPage = acsIdPageSize,
      maxPayloadsPerPayloadsPage = eventsPageSize,
      maxPagesPerIdPagesBuffer = acsIdPageBufferSize,
      maxWorkingMemoryInBytesForIdPages = acsIdPageWorkingMemoryBytes,
      maxParallelIdCreateQueries = acsIdFetchingParallelism,
      maxParallelPayloadCreateQueries = acsContractFetchingParallelism,
    )

  def genTransactionFlatStreams: Gen[UpdatesStreamsConfig] =
    for {
      maxIdsPerIdPage <- Gen.chooseNum(0, Int.MaxValue)
      maxPayloadsPerPayloadsPage <- Gen.chooseNum(0, Int.MaxValue)
      maxPagesPerIdPagesBuffer <- Gen.chooseNum(0, Int.MaxValue)
      maxWorkingMemoryInBytesForIdPages <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelIdCreateQueries <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelPayloadCreateQueries <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelIdConsumingQueries <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelPayloadConsumingQueries <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelPayloadQueries <- Gen.chooseNum(0, Int.MaxValue)
      transactionsProcessingParallelism <- Gen.chooseNum(0, Int.MaxValue)
    } yield UpdatesStreamsConfig(
      maxIdsPerIdPage = maxIdsPerIdPage,
      maxPagesPerIdPagesBuffer = maxPayloadsPerPayloadsPage,
      maxWorkingMemoryInBytesForIdPages = maxPagesPerIdPagesBuffer,
      maxPayloadsPerPayloadsPage = maxWorkingMemoryInBytesForIdPages,
      maxParallelIdCreateQueries = maxParallelIdCreateQueries,
      maxParallelIdConsumingQueries = maxParallelPayloadCreateQueries,
      maxParallelPayloadCreateQueries = maxParallelIdConsumingQueries,
      maxParallelPayloadConsumingQueries = maxParallelPayloadConsumingQueries,
      maxParallelPayloadQueries = maxParallelPayloadQueries,
      transactionsProcessingParallelism = transactionsProcessingParallelism,
    )

  def genTransactionTreeStreams: Gen[TransactionTreeStreamsConfig] =
    for {
      maxIdsPerIdPage <- Gen.chooseNum(0, Int.MaxValue)
      maxPayloadsPerPayloadsPage <- Gen.chooseNum(0, Int.MaxValue)
      maxPagesPerIdPagesBuffer <- Gen.chooseNum(0, Int.MaxValue)
      maxWorkingMemoryInBytesForIdPages <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelIdCreateQueries <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelPayloadCreateQueries <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelIdConsumingQueries <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelPayloadConsumingQueries <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelPayloadQueries <- Gen.chooseNum(0, Int.MaxValue)
      transactionsProcessingParallelism <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelIdNonConsumingQueries <- Gen.chooseNum(0, Int.MaxValue)
      maxParallelPayloadNonConsumingQueries <- Gen.chooseNum(0, Int.MaxValue)
    } yield TransactionTreeStreamsConfig(
      maxIdsPerIdPage = maxIdsPerIdPage,
      maxPagesPerIdPagesBuffer = maxPayloadsPerPayloadsPage,
      maxWorkingMemoryInBytesForIdPages = maxPagesPerIdPagesBuffer,
      maxPayloadsPerPayloadsPage = maxWorkingMemoryInBytesForIdPages,
      maxParallelIdCreateQueries = maxParallelIdCreateQueries,
      maxParallelIdConsumingQueries = maxParallelPayloadCreateQueries,
      maxParallelPayloadCreateQueries = maxParallelIdConsumingQueries,
      maxParallelPayloadConsumingQueries = maxParallelPayloadConsumingQueries,
      maxParallelPayloadQueries = maxParallelPayloadQueries,
      transactionsProcessingParallelism = transactionsProcessingParallelism,
      maxParallelIdNonConsumingQueries = maxParallelIdNonConsumingQueries,
      maxParallelPayloadNonConsumingQueries = maxParallelPayloadNonConsumingQueries,
    )

  val indexServiceConfig: Gen[IndexServiceConfig] = for {
    activeContractsServiceStreamsConfig <- genActiveContractsServiceStreamConfig
    transactionFlatStreams <- genTransactionFlatStreams
    transactionTreeStreams <- genTransactionTreeStreams
    eventsProcessingParallelism <- Gen.chooseNum(0, Int.MaxValue)
    bufferedStreamsPageSize <- Gen.chooseNum(0, Int.MaxValue)
    maxContractStateCacheSize <- Gen.long
    maxContractKeyStateCacheSize <- Gen.long
    maxTransactionsInMemoryFanOutBufferSize <- Gen.chooseNum(0, Int.MaxValue)
    apiStreamShutdownTimeout <- Gen.finiteDuration
  } yield IndexServiceConfig(
    eventsProcessingParallelism,
    bufferedStreamsPageSize,
    maxContractStateCacheSize,
    maxContractKeyStateCacheSize,
    maxTransactionsInMemoryFanOutBufferSize,
    apiStreamShutdownTimeout,
    activeContractsServiceStreams = activeContractsServiceStreamsConfig,
    updatesStreams = transactionFlatStreams,
    transactionTreeStreams = transactionTreeStreams,
  )

}
