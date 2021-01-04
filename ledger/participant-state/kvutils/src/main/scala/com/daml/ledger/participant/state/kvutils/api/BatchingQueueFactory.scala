// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import scala.concurrent.duration.{Duration, MILLISECONDS}

object BatchingQueueFactory {
  def batchingQueueFrom(batchingLedgerWriterConfig: BatchingLedgerWriterConfig): BatchingQueue =
    if (batchingLedgerWriterConfig.enableBatching) {
      DefaultBatchingQueue(
        maxQueueSize = batchingLedgerWriterConfig.maxBatchQueueSize,
        maxBatchSizeBytes = batchingLedgerWriterConfig.maxBatchSizeBytes,
        maxWaitDuration = batchingLedgerWriterConfig.maxBatchWaitDuration,
        maxConcurrentCommits = batchingLedgerWriterConfig.maxBatchConcurrentCommits
      )
    } else {
      batchingQueueForSerialValidation(batchingLedgerWriterConfig.maxBatchQueueSize)
    }

  private def batchingQueueForSerialValidation(maxBatchQueueSize: Int): DefaultBatchingQueue =
    DefaultBatchingQueue(
      maxQueueSize = maxBatchQueueSize,
      maxBatchSizeBytes = 1,
      maxWaitDuration = Duration(1, MILLISECONDS),
      maxConcurrentCommits = 1
    )
}
