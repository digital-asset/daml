// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.api.BatchingLedgerWriterConfig

final case class ExtraConfig(
    batchingLedgerWriterConfig: BatchingLedgerWriterConfig,
    preExecute: Boolean,
)

object ExtraConfig {
  val reasonableDefault =
    ExtraConfig(
      batchingLedgerWriterConfig =
        BatchingLedgerWriterConfig.reasonableDefault.copy(maxBatchConcurrentCommits = 2),
      preExecute = false)
}
