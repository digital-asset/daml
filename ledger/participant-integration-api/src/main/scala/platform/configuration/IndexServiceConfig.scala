// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import scala.concurrent.duration.{Duration, FiniteDuration}

final case class IndexServiceConfig(
    eventsProcessingParallelism: Int = IndexServiceConfig.DefaultEventsProcessingParallelism,
    bufferedStreamsPageSize: Int = IndexServiceConfig.DefaultBufferedStreamsPageSize,
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
    completionsPageSize: Int = 1000,
    acsStreams: AcsStreamsConfig = AcsStreamsConfig.default,
    transactionFlatStreams: TransactionFlatStreamsConfig = TransactionFlatStreamsConfig.default,
    transactionTreeStreams: TransactionTreeStreamsConfig = TransactionTreeStreamsConfig.default,
    globalMaxEventIdQueries: Int = 20,
    globalMaxEventPayloadQueries: Int = 10,
)

object IndexServiceConfig {
  val DefaultEventsProcessingParallelism: Int = 8
  val DefaultBufferedStreamsPageSize: Int = 100
  val DefaultMaxContractStateCacheSize: Long = 100000L
  val DefaultMaxContractKeyStateCacheSize: Long = 100000L
  val DefaultMaxTransactionsInMemoryFanOutBufferSize: Int = 10000
  val DefaultApiStreamShutdownTimeout: Duration = FiniteDuration(5, "seconds")
  val DefaultInMemoryStateUpdaterParallelism: Int = 2
  val DefaultInMemoryFanOutThreadPoolSize: Int = 16
  val PreparePackageMetadataTimeOutWarning: FiniteDuration = FiniteDuration(5, "second")
}

case class AcsStreamsConfig(
    maxIdsPerIdPage: Int = AcsStreamsConfig.DefaultAcsIdPageSize,
    maxPagesPerIdPagesBuffer: Int = AcsStreamsConfig.DefaultAcsIdPageBufferSize,
    maxWorkingMemoryInBytesForIdPages: Int = AcsStreamsConfig.DefaultAcsIdPageWorkingMemoryBytes,
    maxPayloadsPerPayloadsPage: Int = AcsStreamsConfig.DefaultEventsPageSize,
    maxParallelIdCreateQueries: Int = AcsStreamsConfig.DefaultAcsIdFetchingParallelism,
    // Must be a power of 2
    maxParallelPayloadCreateQueries: Int = AcsStreamsConfig.DefaultAcsContractFetchingParallelism,
)

object AcsStreamsConfig {
  val DefaultEventsPageSize: Int = 1000
  val DefaultAcsIdPageSize: Int = 20000
  val DefaultAcsIdPageBufferSize: Int = 1
  val DefaultAcsIdPageWorkingMemoryBytes: Int = 100 * 1024 * 1024
  val DefaultAcsIdFetchingParallelism: Int = 2
  // Must be a power of 2
  val DefaultAcsContractFetchingParallelism: Int = 2

  val default: AcsStreamsConfig = AcsStreamsConfig()
}

case class TransactionFlatStreamsConfig(
    maxIdsPerIdPage: Int = 20000,
    maxPagesPerIdPagesBuffer: Int = 1,
    maxWorkingMemoryInBytesForIdPages: Int = 100 * 1024 * 1024,
    maxPayloadsPerPayloadsPage: Int = 1000,
    maxParallelIdCreateQueries: Int = 4,
    maxParallelIdConsumingQueries: Int = 4,
    maxParallelPayloadCreateQueries: Int = 2,
    maxParallelPayloadConsumingQueries: Int = 2,
    maxParallelPayloadQueries: Int = 2,
    transactionsProcessingParallelism: Int = 8,
)
object TransactionFlatStreamsConfig {
  val default: TransactionFlatStreamsConfig = TransactionFlatStreamsConfig()
}

case class TransactionTreeStreamsConfig(
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
    transactionsProcessingParallelism: Int = 8,
)
object TransactionTreeStreamsConfig {
  val default: TransactionTreeStreamsConfig = TransactionTreeStreamsConfig()
}
