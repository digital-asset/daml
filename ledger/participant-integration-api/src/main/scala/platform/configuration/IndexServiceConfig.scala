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
    // Must be a power of 2
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
    completionsMaxPayloadsPerPayloadsPage: Int = 1000,
    transactionsFlatStreams: TransactionsFlatStreamsConfig = TransactionsFlatStreamsConfig.default,
    transactionsTreeStreams: TransactionsTreeStreamsConfig = TransactionsTreeStreamsConfig.default,
    globalMaxEventIdQueries: Int = 20,
    globalMaxEventPayloadQueries: Int = 10,
)

object IndexServiceConfig {
  val DefaultEventsPageSize: Int = 1000
  val DefaultEventsProcessingParallelism: Int = 8
  val DefaultBufferedStreamsPageSize: Int = 100
  val DefaultAcsIdPageSize: Int = 20000
  val DefaultAcsIdPageBufferSize: Int = 1
  val DefaultAcsIdPageWorkingMemoryBytes: Int = 100 * 1024 * 1024
  val DefaultAcsIdFetchingParallelism: Int = 2
  // Must be a power of 2
  val DefaultAcsContractFetchingParallelism: Int = 2
  val DefaultAcsGlobalParallelism: Int = 10
  val DefaultMaxContractStateCacheSize: Long = 100000L
  val DefaultMaxContractKeyStateCacheSize: Long = 100000L
  val DefaultMaxTransactionsInMemoryFanOutBufferSize: Int = 10000
  val DefaultApiStreamShutdownTimeout: Duration = FiniteDuration(5, "seconds")
  val DefaultInMemoryStateUpdaterParallelism: Int = 2
  val DefaultInMemoryFanOutThreadPoolSize: Int = 16
  val PreparePackageMetadataTimeOutWarning: FiniteDuration = FiniteDuration(1, "second")
}

case class TransactionsFlatStreamsConfig(
    maxIdsPerIdPage: Int = 20000,
    maxPagesPerIdPagesBuffer: Int = 1,
    maxWorkingMemoryInBytesForIdPages: Int = 100 * 1024 * 1024,
    maxPayloadsPerPayloadsPage: Int = 1000,
    maxParallelIdCreateQueries: Int = 4,
    maxParallelIdConsumingQueries: Int = 4,
    // Must be a power of 2
    maxParallelPayloadCreateQueries: Int = 2,
    // Must be a power of 2
    maxParallelPayloadConsumingQueries: Int = 2,
    maxParallelPayloadQueries: Int = 2,
    transactionsProcessingParallelism: Int = 8,
)
object TransactionsFlatStreamsConfig {
  val default: TransactionsFlatStreamsConfig = TransactionsFlatStreamsConfig()
}

case class TransactionsTreeStreamsConfig(
    maxIdsPerIdPage: Int = 20000,
    maxPagesPerIdPagesBuffer: Int = 1,
    maxWorkingMemoryInBytesForIdPages: Int = 100 * 1024 * 1024,
    maxPayloadsPerPayloadsPage: Int = 1000,
    maxParallelIdCreateQueries: Int = 8,
    maxParallelIdConsumingQueries: Int = 8,
    maxParallelIdNonConsumingQueries: Int = 4,
    // Must be a power of 2
    maxParallelPayloadCreateQueries: Int = 2,
    // Must be a power of 2
    maxParallelPayloadConsumingQueries: Int = 2,
    // Must be a power of 2
    maxParallelPayloadNonConsumingQueries: Int = 2,
    maxParallelPayloadQueries: Int = 2,
    transactionsProcessingParallelism: Int = 8,
)
object TransactionsTreeStreamsConfig {
  val default: TransactionsTreeStreamsConfig = TransactionsTreeStreamsConfig()
}
