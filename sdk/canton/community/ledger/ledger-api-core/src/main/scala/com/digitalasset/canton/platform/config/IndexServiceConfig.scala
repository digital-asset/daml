// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

/** Ledger api index service specific configurations
  *
  * @param bufferedEventsProcessingParallelism
  *   parallelism for loading and decoding ledger events for populating ledger api server's internal
  *   buffers
  * @param bufferedStreamsPageSize
  *   the page size for streams created from ledger api server's in-memory buffers
  * @param maxContractStateCacheSize
  *   maximum caffeine cache size of mutable state cache of contracts
  * @param maxContractKeyStateCacheSize
  *   maximum caffeine cache size of mutable state cache of contract keys
  * @param maxTransactionsInMemoryFanOutBufferSize
  *   maximum number of transactions to hold in the "in-memory fanout" (if enabled)
  * @param apiStreamShutdownTimeout
  *   shutdown timeout for a graceful completion of ledger api server's streams
  * @param inMemoryStateUpdaterParallelism
  *   the processing parallelism of the Ledger API server in-memory state updater
  * @param apiQueryServicesThreadPoolSize
  *   size of the thread-pool backing the Ledger API query-services (fe not the command
  *   submission/interpretation). If not set, defaults to ((number of thread)/4 + 1)
  * @param preparePackageMetadataTimeOutWarning
  *   timeout for package metadata preparation after which a warning will be logged
  * @param completionsPageSize
  *   database / pekko page size for batching of ledger api server index ledger completion queries
  * @param activeContractsServiceStreams
  *   configurations pertaining to the ledger api server's "active contracts service"
  * @param updatesStreams
  *   configurations pertaining to the ledger api server's streams of updates
  * @param transactionTreeStreams
  *   configurations pertaining to the ledger api server's streams of transaction trees
  * @param globalMaxEventIdQueries
  *   maximum number of concurrent event id queries across all stream types
  * @param globalMaxEventPayloadQueries
  *   maximum number of concurrent event payload queries across all stream types
  * @param offsetCheckpointCacheUpdateInterval
  *   the interval duration for OffsetCheckpoint cache updates
  * @param idleStreamOffsetCheckpointTimeout
  *   the timeout duration for checking if a new OffsetCheckpoint is created
  */
final case class IndexServiceConfig(
    bufferedEventsProcessingParallelism: Int =
      IndexServiceConfig.DefaultBufferedEventsProcessingParallelism,
    bufferedStreamsPageSize: Int = IndexServiceConfig.DefaultBufferedStreamsPageSize,
    maxContractStateCacheSize: Long = IndexServiceConfig.DefaultMaxContractStateCacheSize,
    maxContractKeyStateCacheSize: Long = IndexServiceConfig.DefaultMaxContractKeyStateCacheSize,
    maxTransactionsInMemoryFanOutBufferSize: Int =
      IndexServiceConfig.DefaultMaxTransactionsInMemoryFanOutBufferSize,
    apiStreamShutdownTimeout: Duration = IndexServiceConfig.DefaultApiStreamShutdownTimeout,
    inMemoryStateUpdaterParallelism: Int =
      IndexServiceConfig.DefaultInMemoryStateUpdaterParallelism,
    apiQueryServicesThreadPoolSize: Option[Int] = None,
    preparePackageMetadataTimeOutWarning: NonNegativeFiniteDuration =
      IndexServiceConfig.PreparePackageMetadataTimeOutWarning,
    completionsPageSize: Int = IndexServiceConfig.DefaultCompletionsPageSize,
    activeContractsServiceStreams: ActiveContractsServiceStreamsConfig =
      ActiveContractsServiceStreamsConfig.default,
    updatesStreams: UpdatesStreamsConfig = UpdatesStreamsConfig.default,
    globalMaxEventIdQueries: Int = 20,
    globalMaxEventPayloadQueries: Int = 10,
    offsetCheckpointCacheUpdateInterval: NonNegativeFiniteDuration =
      IndexServiceConfig.OffsetCheckpointCacheUpdateInterval,
    idleStreamOffsetCheckpointTimeout: NonNegativeFiniteDuration =
      IndexServiceConfig.IdleStreamOffsetCheckpointTimeout,
)

object IndexServiceConfig {
  val DefaultBufferedEventsProcessingParallelism: Int = 8
  val DefaultBufferedStreamsPageSize: Int = 100
  val DefaultMaxContractStateCacheSize: Long = 10000L
  val DefaultMaxContractKeyStateCacheSize: Long = 10000L
  val DefaultMaxTransactionsInMemoryFanOutBufferSize: Int = 1000
  val DefaultApiStreamShutdownTimeout: Duration = FiniteDuration(5, "seconds")
  val DefaultInMemoryStateUpdaterParallelism: Int = 2
  val PreparePackageMetadataTimeOutWarning: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(5)
  val DefaultCompletionsPageSize = 1000
  val OffsetCheckpointCacheUpdateInterval: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(15)
  val IdleStreamOffsetCheckpointTimeout: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofMinutes(1)

  def DefaultQueryServicesThreadPoolSize(logger: Logger): Int = {
    val numberOfThreads = Threading.detectNumberOfThreads(logger).value
    numberOfThreads / 4 + 1
  }
}

/** Ledger api active contracts service specific configurations
  *
  * @param maxIdsPerIdPage
  *   Number of event ids to retrieve in a single query (a page of event ids).
  * @param maxPagesPerIdPagesBuffer
  *   Number of id pages to store in a buffer. There is a buffer for each decomposed filtering
  *   constraint.
  * @param maxWorkingMemoryInBytesForIdPages
  *   Memory for storing id pages across all id pages buffers. Per single stream.
  * @param maxPayloadsPerPayloadsPage
  *   Number of parallel queries that fetch payloads of create events. Per single stream.
  * @param maxParallelActiveIdQueries
  *   Number of parallel queries that fetch ids of create events. Per single stream.
  * @param maxParallelPayloadCreateQueries
  *   Number of event payloads to retrieve in a single query (a page of event payloads).
  * @param contractProcessingParallelism
  *   The parallelism for contract processing.
  * @param maxIncompletePageSize
  *   The maximum size of an incomplete page. Stream is chunked up this into groups of this size.
  */
final case class ActiveContractsServiceStreamsConfig(
    maxIdsPerIdPage: Int = ActiveContractsServiceStreamsConfig.DefaultAcsIdPageSize,
    maxPagesPerIdPagesBuffer: Int = ActiveContractsServiceStreamsConfig.DefaultAcsIdPageBufferSize,
    maxWorkingMemoryInBytesForIdPages: Int =
      ActiveContractsServiceStreamsConfig.DefaultAcsIdPageWorkingMemoryBytes,
    maxPayloadsPerPayloadsPage: Int = ActiveContractsServiceStreamsConfig.DefaultEventsPageSize,
    maxParallelActiveIdQueries: Int =
      ActiveContractsServiceStreamsConfig.DefaultAcsIdFetchingParallelism,
    idFilterQueryParallelism: Int =
      ActiveContractsServiceStreamsConfig.DefaultAcsIdFilterQueryParallelism,
    // Must be a power of 2
    maxParallelPayloadCreateQueries: Int =
      ActiveContractsServiceStreamsConfig.DefaultAcsContractFetchingParallelism,
    contractProcessingParallelism: Int =
      ActiveContractsServiceStreamsConfig.DefaultContractProcessingParallelism,
    // Temporary incomplete population parameters
    maxIncompletePageSize: Int = 20,
)

object ActiveContractsServiceStreamsConfig {
  val DefaultEventsPageSize: Int = 1000
  val DefaultAcsIdPageSize: Int = 20000
  val DefaultAcsIdPageBufferSize: Int = 1
  val DefaultAcsIdPageWorkingMemoryBytes: Int = 100 * 1024 * 1024
  val DefaultAcsIdFetchingParallelism: Int = 4
  val DefaultAcsIdFilterQueryParallelism: Int = 2
  // Must be a power of 2
  val DefaultAcsContractFetchingParallelism: Int = 2
  val DefaultContractProcessingParallelism: Int = 8

  val default: ActiveContractsServiceStreamsConfig = ActiveContractsServiceStreamsConfig()

}

/** Updates stream configuration.
  *
  * @param maxIdsPerIdPage
  *   Number of event ids to retrieve in a single query (a page of event ids).
  * @param maxPagesPerIdPagesBuffer
  *   Number of id pages to store in a buffer. There is a buffer for each decomposed filtering
  *   constraint.
  * @param maxWorkingMemoryInBytesForIdPages
  *   Memory for storing id pages across all id pages buffers. Per single stream.
  * @param maxPayloadsPerPayloadsPage
  *   Number of event payloads to retrieve in a single query (a page of event payloads).
  * @param maxParallelIdActivateQueries
  *   Number of parallel queries that fetch ids of create events. Per single stream.
  * @param maxParallelIdDeactivateQueries
  *   Number of parallel queries that fetch ids of consuming events. Per single stream.
  * @param maxParallelIdTopologyEventsQueries
  *   Number of parallel queries that fetch payloads of topology events. Per single stream.
  * @param maxParallelPayloadActivateQueries
  *   Number of parallel queries that fetch payloads of create events. Per single stream.
  * @param maxParallelPayloadDeactivateQueries
  *   Number of parallel queries that fetch payloads of consuming events. Per single stream.
  * @param maxParallelPayloadTopologyEventsQueries
  *   Number of parallel queries that fetch ids of topology events. Per single stream.
  * @param maxParallelPayloadQueries
  *   Upper bound on the number of parallel queries that fetch payloads. Per single stream.
  * @param transactionsProcessingParallelism
  *   Number of transactions to process in parallel. Per single stream.
  */

final case class UpdatesStreamsConfig(
    maxIdsPerIdPage: Int = 20000,
    maxPagesPerIdPagesBuffer: Int = 1,
    maxWorkingMemoryInBytesForIdPages: Int = 100 * 1024 * 1024,
    maxPayloadsPerPayloadsPage: Int = 1000,
    maxParallelIdActivateQueries: Int = 4,
    maxParallelIdDeactivateQueries: Int = 4,
    maxParallelIdVariousWitnessedQueries: Int = 4,
    maxParallelIdTopologyEventsQueries: Int = 4,
    maxParallelPayloadActivateQueries: Int = 2,
    maxParallelPayloadDeactivateQueries: Int = 2,
    maxParallelPayloadVariousWitnessedQueries: Int = 2,
    maxParallelPayloadTopologyEventsQueries: Int = 2,
    maxParallelPayloadQueries: Int = 2,
    transactionsProcessingParallelism: Int = 8,
    idFilterQueryParallelism: Int = 2,
)
object UpdatesStreamsConfig {
  val default: UpdatesStreamsConfig = UpdatesStreamsConfig()
}
