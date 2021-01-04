// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import scala.concurrent.duration._

/** Configuration for the batching ledger writer.
  *
  * @param enableBatching Enables batching if true.
  * @param maxBatchQueueSize The maximum number of submissions to queue before dropping new ones.
  * @param maxBatchSizeBytes Maximum size threshold after which batch is immediately emitted.
  * @param maxBatchWaitDuration The maximum duration to wait before emitting the batch.
  * @param maxBatchConcurrentCommits Maximum number of concurrent calls to commit.
  */
final case class BatchingLedgerWriterConfig(
    enableBatching: Boolean,
    maxBatchQueueSize: Int,
    maxBatchSizeBytes: Long,
    maxBatchWaitDuration: FiniteDuration,
    maxBatchConcurrentCommits: Int
)

object BatchingLedgerWriterConfig {
  val reasonableDefault: BatchingLedgerWriterConfig =
    BatchingLedgerWriterConfig(
      enableBatching = false,
      maxBatchQueueSize = 200,
      maxBatchSizeBytes = 4L * 1024L * 1024L /* 4MB */,
      maxBatchWaitDuration = 100.millis,
      maxBatchConcurrentCommits = 5
    )
}
