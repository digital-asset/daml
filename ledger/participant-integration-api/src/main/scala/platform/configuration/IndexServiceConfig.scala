// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.io.File

import scala.concurrent.duration.{Duration, FiniteDuration}

final case class IndexServiceConfig(
    archiveFiles: List[File] = IndexServiceConfig.DefaultArchiveFiles,
    eventsPageSize: Int = IndexServiceConfig.DefaultEventsPageSize,
    eventsProcessingParallelism: Int = IndexServiceConfig.DefaultEventsProcessingParallelism,
    bufferedStreamsPageSize: Int = IndexServiceConfig.DefaultBufferedStreamsPageSize,
    acsIdPageSize: Int = IndexServiceConfig.DefaultAcsIdPageSize,
    acsIdPageBufferSize: Int = IndexConfiguration.DefaultAcsIdPageBufferSize,
    acsIdFetchingParallelism: Int = IndexServiceConfig.DefaultAcsIdFetchingParallelism,
    acsContractFetchingParallelism: Int = IndexServiceConfig.DefaultAcsContractFetchingParallelism,
    acsGlobalParallelism: Int = IndexServiceConfig.DefaultAcsGlobalParallelism,
    maxContractStateCacheSize: Long = IndexServiceConfig.DefaultMaxContractStateCacheSize,
    maxContractKeyStateCacheSize: Long = IndexServiceConfig.DefaultMaxContractKeyStateCacheSize,
    maxTransactionsInMemoryFanOutBufferSize: Long =
      IndexServiceConfig.DefaultMaxTransactionsInMemoryFanOutBufferSize,
    enableInMemoryFanOutForLedgerApi: Boolean =
      IndexServiceConfig.DefaultEnableInMemoryFanOutForLedgerApi,
    apiStreamShutdownTimeout: Duration = IndexServiceConfig.DefaultApiStreamShutdownTimeout,
)

object IndexServiceConfig {
  val DefaultEventsPageSize: Int = 1000
  val DefaultEventsProcessingParallelism: Int = 8
  val DefaultBufferedStreamsPageSize: Int = 100
  val DefaultAcsIdPageSize: Int = 20000
  val DefaultAcsIdPageBufferSize: Int = 1
  val DefaultAcsIdFetchingParallelism: Int = 2
  val DefaultAcsContractFetchingParallelism: Int = 2
  val DefaultAcsGlobalParallelism: Int = 10
  val DefaultMaxContractStateCacheSize: Long = 100000L
  val DefaultMaxContractKeyStateCacheSize: Long = 100000L
  val DefaultMaxTransactionsInMemoryFanOutBufferSize: Long = 10000L
  val DefaultEnableInMemoryFanOutForLedgerApi = false
  val DefaultArchiveFiles = List.empty[File]
  val DefaultApiStreamShutdownTimeout: Duration = FiniteDuration(5, "seconds")
}
