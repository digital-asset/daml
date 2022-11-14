// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import scala.concurrent.duration.{Duration, FiniteDuration}

final case class IndexServiceConfig(
    eventsPageSize: Int = IndexServiceConfig.DefaultEventsPageSize,
    eventsProcessingParallelism: Int = IndexServiceConfig.DefaultEventsProcessingParallelism,
    bufferedStreamsPageSize: Int = IndexServiceConfig.DefaultBufferedStreamsPageSize,
    acsIdPageSize: Int = IndexServiceConfig.DefaultAcsIdPageSize,
    acsIdPageBufferSize: Int = IndexServiceConfig.DefaultAcsIdPageBufferSize,
    acsIdPageWorkingMemoryBytes: Int = IndexServiceConfig.DefaultAcsIdPageWorkingMemoryBytes,
    acsIdFetchingParallelism: Int = IndexServiceConfig.DefaultAcsIdFetchingParallelism,
    acsContractFetchingParallelism: Int = IndexServiceConfig.DefaultAcsContractFetchingParallelism,
    acsGlobalParallelism: Int = IndexServiceConfig.DefaultAcsGlobalParallelism,
    maxContractStateCacheSize: Long = IndexServiceConfig.DefaultMaxContractStateCacheSize,
    maxContractKeyStateCacheSize: Long = IndexServiceConfig.DefaultMaxContractKeyStateCacheSize,
    maxTransactionsInMemoryFanOutBufferSize: Int =
      IndexServiceConfig.DefaultMaxTransactionsInMemoryFanOutBufferSize,
    apiStreamShutdownTimeout: Duration = IndexServiceConfig.DefaultApiStreamShutdownTimeout,
    inMemoryStateUpdaterParallelism: Int =
      IndexServiceConfig.DefaultInMemoryStateUpdaterParallelism,
    inMemoryFanOutThreadPoolSize: Int = IndexServiceConfig.DefaultInMemoryFanOutThreadPoolSize,
    preparePackageMetadataTimeOutWarning: FiniteDuration =
      IndexServiceConfig.PreparePackageMetadataTimeOutWarning,
    completionsPageSize: Int = IndexServiceConfig.DefaultCompletionsPageSize,
    transactionsFlatStreamReaderConfig: TransactionsFlatStreamReaderConfig =
      TransactionsFlatStreamReaderConfig.default,
    transactionsTreeStreamReaderConfig: TransactionsTreeStreamReaderConfig =
      TransactionsTreeStreamReaderConfig.default,
    // TODO etq: Take care what config key names will get exposed in the user-facing participant config
    globalMaxIdQueries: Int = 20,
    globalMaxPayloadQueries: Int = 10,
)

object IndexServiceConfig {
  val DefaultEventsPageSize: Int = 1000
  val DefaultEventsProcessingParallelism: Int = 8
  val DefaultBufferedStreamsPageSize: Int = 100
  val DefaultAcsIdPageSize: Int = 20000
  val DefaultAcsIdPageBufferSize: Int = 1
  val DefaultAcsIdPageWorkingMemoryBytes: Int = 100 * 1024 * 1024
  // TODO pbatko: Must be power of 2
  val DefaultAcsIdFetchingParallelism: Int = 2
  val DefaultAcsContractFetchingParallelism: Int = 2
  val DefaultAcsGlobalParallelism: Int = 10
  val DefaultMaxContractStateCacheSize: Long = 100000L
  val DefaultMaxContractKeyStateCacheSize: Long = 100000L
  val DefaultMaxTransactionsInMemoryFanOutBufferSize: Int = 10000
  val DefaultApiStreamShutdownTimeout: Duration = FiniteDuration(5, "seconds")
  val DefaultInMemoryStateUpdaterParallelism: Int = 2
  val DefaultInMemoryFanOutThreadPoolSize: Int = 16
  val PreparePackageMetadataTimeOutWarning: FiniteDuration = FiniteDuration(1, "second")
  val DefaultCompletionsPageSize: Int = 1000
}

// TODO etq: Take care what config key names will get exposed in the user-facing participant config
case class TransactionsFlatStreamReaderConfig(
    maxIdsPerIdPage: Int = 20000,
    maxPagesPerIdPagesBuffer: Int = 1,
    maxWorkingMemoryInBytesForIdPages: Int = 100 * 1024 * 1024,
    maxPayloadsPerPayloadsPage: Int = 1000, // eventsPageSize
    // TODO pbatko: Check if these really must be powers of 2
    // BEGIN
    maxParallelIdCreateQueries: Int = 4,
    maxParallelIdConsumingQueries: Int = 4,
    maxParallelPayloadCreateQueries: Int = 2,
    maxParallelPayloadConsumingQueries: Int = 2,
    // END
    maxParallelPayloadQueries: Int = 2,
    payloadProcessingParallelism: Int = 8,
)
object TransactionsFlatStreamReaderConfig {
  def default: TransactionsFlatStreamReaderConfig = TransactionsFlatStreamReaderConfig()
}

// TODO etq: Take care what config key names will get exposed in the user-facing participant config
case class TransactionsTreeStreamReaderConfig(
    maxIdsPerIdPage: Int = 20000,
    maxPagesPerIdPagesBuffer: Int = 1,
    maxWorkingMemoryInBytesForIdPages: Int = 100 * 1024 * 1024,
    maxPayloadsPerPayloadsPage: Int = 1000,
    maxParallelIdCreateQueries: Int = 8,
    maxParallelIdConsumingQueries: Int = 8,
    maxParallelIdNonConsumingQueries: Int = 4,
    maxParallelPayloadCreateQueries: Int = 2,
    maxParallelPayloadConsumingQueries: Int = 2,
    maxParallelPayloadNonConsumingQueries: Int = 2,
    maxParallelPayloadQueries: Int = 2,
    payloadProcessingParallelism: Int = 8,
)
object TransactionsTreeStreamReaderConfig {
  def default: TransactionsTreeStreamReaderConfig = TransactionsTreeStreamReaderConfig()
}
