// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.io.File

final case class IndexConfiguration(
    archiveFiles: List[File] = IndexConfiguration.DefaultArchiveFiles,
    eventsPageSize: Int = IndexConfiguration.DefaultEventsPageSize,
    eventsProcessingParallelism: Int = IndexConfiguration.DefaultEventsProcessingParallelism,
    acsIdPageSize: Int = IndexConfiguration.DefaultAcsIdPageSize,
    acsIdFetchingParallelism: Int = IndexConfiguration.DefaultAcsIdFetchingParallelism,
    acsContractFetchingParallelism: Int = IndexConfiguration.DefaultAcsContractFetchingParallelism,
    acsGlobalParallelism: Int = IndexConfiguration.DefaultAcsGlobalParallelism,
    maxContractStateCacheSize: Long = IndexConfiguration.DefaultMaxContractStateCacheSize,
    maxContractKeyStateCacheSize: Long = IndexConfiguration.DefaultMaxContractKeyStateCacheSize,
    maxTransactionsInMemoryFanOutBufferSize: Long =
      IndexConfiguration.DefaultMaxTransactionsInMemoryFanOutBufferSize,
    enableInMemoryFanOutForLedgerApi: Boolean =
      IndexConfiguration.DefaultEnableInMemoryFanOutForLedgerApi,
)

object IndexConfiguration {
  val DefaultEventsPageSize: Int = 1000
  val DefaultEventsProcessingParallelism: Int = 8
  val DefaultAcsIdPageSize: Int = 20000
  val DefaultAcsIdFetchingParallelism: Int = 2
  val DefaultAcsContractFetchingParallelism: Int = 2
  val DefaultAcsGlobalParallelism: Int = 10
  val DefaultMaxContractStateCacheSize: Long = 100000L
  val DefaultMaxContractKeyStateCacheSize: Long = 100000L
  val DefaultMaxTransactionsInMemoryFanOutBufferSize: Long = 10000L
  val DefaultEnableInMemoryFanOutForLedgerApi = false
  val DefaultArchiveFiles = List.empty[File]
}
