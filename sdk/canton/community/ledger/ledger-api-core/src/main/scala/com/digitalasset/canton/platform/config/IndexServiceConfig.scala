// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DeprecatedConfigUtils.DeprecatedFieldsFor
import com.digitalasset.canton.config.{DeprecatedConfigUtils, NonNegativeFiniteDuration}
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration.{Duration, FiniteDuration}

/** Ledger api index service specific configurations
  *
  * @param bufferedEventsProcessingParallelism     parallelism for loading and decoding ledger events for populating ledger api server's internal buffers
  * @param bufferedStreamsPageSize                 the page size for streams created from ledger api server's in-memory buffers
  * @param maxContractStateCacheSize               maximum caffeine cache size of mutable state cache of contracts
  * @param maxContractKeyStateCacheSize            maximum caffeine cache size of mutable state cache of contract keys
  * @param maxTransactionsInMemoryFanOutBufferSize maximum number of transactions to hold in the "in-memory fanout" (if enabled)
  * @param apiStreamShutdownTimeout                shutdown timeout for a graceful completion of ledger api server's streams
  * @param inMemoryStateUpdaterParallelism         the processing parallelism of the Ledger API server in-memory state updater
  * @param inMemoryFanOutThreadPoolSize            size of the thread-pool backing the Ledger API in-memory fan-out.
  *                                                If not set, defaults to ((number of thread)/4 + 1)
  * @param preparePackageMetadataTimeOutWarning    timeout for package metadata preparation after which a warning will be logged
  * @param completionsPageSize                     database / pekko page size for batching of ledger api server index ledger completion queries
  * @param activeContractsServiceStreams           configurations pertaining to the ledger api server's "active contracts service"
  * @param transactionFlatStreams                  configurations pertaining to the ledger api server's streams of transaction trees
  * @param transactionTreeStreams                  configurations pertaining to the ledger api server's streams of flat transactions
  * @param globalMaxEventIdQueries                 maximum number of concurrent event id queries across all stream types
  * @param globalMaxEventPayloadQueries            maximum number of concurrent event payload queries across all stream types
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
    inMemoryFanOutThreadPoolSize: Option[Int] = None,
    preparePackageMetadataTimeOutWarning: NonNegativeFiniteDuration =
      IndexServiceConfig.PreparePackageMetadataTimeOutWarning,
    completionsPageSize: Int = IndexServiceConfig.DefaultCompletionsPageSize,
    activeContractsServiceStreams: ActiveContractsServiceStreamsConfig =
      ActiveContractsServiceStreamsConfig.default,
    transactionFlatStreams: TransactionFlatStreamsConfig = TransactionFlatStreamsConfig.default,
    transactionTreeStreams: TransactionTreeStreamsConfig = TransactionTreeStreamsConfig.default,
    globalMaxEventIdQueries: Int = 20,
    globalMaxEventPayloadQueries: Int = 10,
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

  def DefaultInMemoryFanOutThreadPoolSize(logger: Logger): Int = {
    val numberOfThreads = Threading.detectNumberOfThreads(logger)
    numberOfThreads / 4 + 1
  }
}

/** Ledger api active contracts service specific configurations
  *
  * @param maxIdsPerIdPage                   Number of event ids to retrieve in a single query (a page of event ids).
  * @param maxPagesPerIdPagesBuffer          Number of id pages to store in a buffer. There is a buffer for each decomposed filtering constraint.
  * @param maxWorkingMemoryInBytesForIdPages Memory for storing id pages across all id pages buffers. Per single stream.
  * @param maxPayloadsPerPayloadsPage        Number of parallel queries that fetch payloads of create events. Per single stream.
  * @param maxParallelIdCreateQueries        Number of parallel queries that fetch ids of create events. Per single stream.
  * @param maxParallelPayloadCreateQueries   Number of event payloads to retrieve in a single query (a page of event payloads).
  * @param contractProcessingParallelism     The parallelism for contract processing.
  * @param maxIncompletePageSize             The maximum size of an incomplete page. Stream is chunked up this into groups of this size.
  */
final case class ActiveContractsServiceStreamsConfig(
    maxIdsPerIdPage: Int = ActiveContractsServiceStreamsConfig.DefaultAcsIdPageSize,
    maxPagesPerIdPagesBuffer: Int = ActiveContractsServiceStreamsConfig.DefaultAcsIdPageBufferSize,
    maxWorkingMemoryInBytesForIdPages: Int =
      ActiveContractsServiceStreamsConfig.DefaultAcsIdPageWorkingMemoryBytes,
    maxPayloadsPerPayloadsPage: Int = ActiveContractsServiceStreamsConfig.DefaultEventsPageSize,
    maxParallelIdCreateQueries: Int =
      ActiveContractsServiceStreamsConfig.DefaultAcsIdFetchingParallelism,
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
  val DefaultAcsIdFetchingParallelism: Int = 2
  // Must be a power of 2
  val DefaultAcsContractFetchingParallelism: Int = 2
  val DefaultContractProcessingParallelism: Int = 8

  val default: ActiveContractsServiceStreamsConfig = ActiveContractsServiceStreamsConfig()

  trait ActiveContractsServiceDeprecationsImplicits {
    implicit def deprecatedActiveContractsServiceConfig[X <: ActiveContractsServiceStreamsConfig]
        : DeprecatedFieldsFor[X] = new DeprecatedFieldsFor[ActiveContractsServiceStreamsConfig] {
      override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
        DeprecatedConfigUtils.MovedConfigPath("acs-id-page-size", "max-ids-per-id-page"),
        DeprecatedConfigUtils
          .MovedConfigPath("acs-id-page-buffer-size", "max-pages-per-id-pages-buffer"),
        DeprecatedConfigUtils
          .MovedConfigPath("acs-id-fetching-parallelism", "max-parallel-id-create-queries"),
        DeprecatedConfigUtils.MovedConfigPath(
          "acs-contract-fetching-parallelism",
          "max-parallel-payload-create-queries",
        ),
      )
    }
  }

  object DeprecatedImplicits extends ActiveContractsServiceDeprecationsImplicits
}

/** Flat transaction streams configuration.
  *
  * @param maxIdsPerIdPage                    Number of event ids to retrieve in a single query (a page of event ids).
  * @param maxPagesPerIdPagesBuffer           Number of id pages to store in a buffer. There is a buffer for each decomposed filtering constraint.
  * @param maxWorkingMemoryInBytesForIdPages  Memory for storing id pages across all id pages buffers. Per single stream.
  * @param maxPayloadsPerPayloadsPage         Number of event payloads to retrieve in a single query (a page of event payloads).
  * @param maxParallelIdCreateQueries         Number of parallel queries that fetch ids of create events. Per single stream.
  * @param maxParallelIdConsumingQueries      Number of parallel queries that fetch ids of consuming events. Per single stream.
  * @param maxParallelIdAssignQueries         Number of parallel queries that fetch ids of assign events. Per single stream.
  * @param maxParallelIdUnassignQueries       Number of parallel queries that fetch ids of unassign events. Per single stream.
  * @param maxParallelPayloadCreateQueries    Number of parallel queries that fetch payloads of create events. Per single stream.
  * @param maxParallelPayloadConsumingQueries Number of parallel queries that fetch payloads of consuming events. Per single stream.
  * @param maxParallelPayloadAssignQueries    Number of parallel queries that fetch payloads of assign events. Per single stream.
  * @param maxParallelPayloadUnassignQueries  Number of parallel queries that fetch payloads of unassign events. Per single stream.
  * @param maxParallelPayloadQueries          Upper bound on the number of parallel queries that fetch payloads. Per single stream.
  * @param transactionsProcessingParallelism  Number of transactions to process in parallel. Per single stream.
  */
final case class TransactionFlatStreamsConfig(
    maxIdsPerIdPage: Int = 20000,
    maxPagesPerIdPagesBuffer: Int = 1,
    maxWorkingMemoryInBytesForIdPages: Int = 100 * 1024 * 1024,
    maxPayloadsPerPayloadsPage: Int = 1000,
    maxParallelIdCreateQueries: Int = 4,
    maxParallelIdConsumingQueries: Int = 4,
    maxParallelIdAssignQueries: Int = 4,
    maxParallelIdUnassignQueries: Int = 4,
    maxParallelPayloadCreateQueries: Int = 2,
    maxParallelPayloadConsumingQueries: Int = 2,
    maxParallelPayloadAssignQueries: Int = 2,
    maxParallelPayloadUnassignQueries: Int = 2,
    maxParallelPayloadQueries: Int = 2,
    transactionsProcessingParallelism: Int = 8,
)
object TransactionFlatStreamsConfig {
  val default: TransactionFlatStreamsConfig = TransactionFlatStreamsConfig()
}

/** Transaction tree streams configuration.
  *
  * @param maxIdsPerIdPage                        Number of event ids to retrieve in a single query (a page of event ids).
  * @param maxPagesPerIdPagesBuffer               Number of id pages to store in a buffer. There is a buffer for each decomposed filtering constraint.
  * @param maxWorkingMemoryInBytesForIdPages      Memory for storing id pages across all id pages buffers. Per single stream.
  * @param maxPayloadsPerPayloadsPage             Number of event payloads to retrieve in a single query (a page of event payloads).
  * @param maxParallelIdCreateQueries             Number of parallel queries that fetch ids of create events. Per single stream.
  * @param maxParallelIdConsumingQueries          Number of parallel queries that fetch ids of consuming events. Per single stream.
  * @param maxParallelIdNonConsumingQueries       Number of parallel queries that fetch payloads of non-consuming events. Per single stream.
  * @param maxParallelIdAssignQueries             Number of parallel queries that fetch payloads of assign events. Per single stream.
  * @param maxParallelIdUnassignQueries           Number of parallel queries that fetch payloads of unassign events. Per single stream.
  * @param maxParallelPayloadCreateQueries        Number of parallel queries that fetch payloads of create events. Per single stream.
  * @param maxParallelPayloadConsumingQueries     Number of parallel queries that fetch payloads of consuming events. Per single stream.
  * @param maxParallelPayloadNonConsumingQueries  Number of parallel queries that fetch ids of non-consuming events. Per single stream.
  * @param maxParallelPayloadAssignQueries        Number of parallel queries that fetch ids of assign events. Per single stream.
  * @param maxParallelPayloadUnassignQueries      Number of parallel queries that fetch ids of unassign events. Per single stream.
  * @param maxParallelPayloadQueries              Upper bound on the number of parallel queries that fetch payloads. Per single stream.
  * @param transactionsProcessingParallelism      Number of transactions to process in parallel. Per single stream.
  */
final case class TransactionTreeStreamsConfig(
    maxIdsPerIdPage: Int = 20000,
    maxPagesPerIdPagesBuffer: Int = 1,
    maxWorkingMemoryInBytesForIdPages: Int = 100 * 1024 * 1024,
    maxPayloadsPerPayloadsPage: Int = 1000,
    maxParallelIdCreateQueries: Int = 8,
    maxParallelIdConsumingQueries: Int = 8,
    maxParallelIdNonConsumingQueries: Int = 4,
    maxParallelIdAssignQueries: Int = 4,
    maxParallelIdUnassignQueries: Int = 4,
    maxParallelPayloadCreateQueries: Int = 2,
    maxParallelPayloadConsumingQueries: Int = 2,
    maxParallelPayloadNonConsumingQueries: Int = 2,
    maxParallelPayloadAssignQueries: Int = 2,
    maxParallelPayloadUnassignQueries: Int = 2,
    maxParallelPayloadQueries: Int = 2,
    transactionsProcessingParallelism: Int = 8,
)
object TransactionTreeStreamsConfig {
  val default: TransactionTreeStreamsConfig = TransactionTreeStreamsConfig()
}
