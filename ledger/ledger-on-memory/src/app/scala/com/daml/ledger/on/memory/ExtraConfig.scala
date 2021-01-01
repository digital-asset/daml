// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.api.BatchingLedgerWriterConfig

final case class ExtraConfig(
    batchingLedgerWriterConfig: BatchingLedgerWriterConfig,
    alwaysPreExecute: Boolean,
)

object ExtraConfig {
  val reasonableDefault: ExtraConfig =
    ExtraConfig(
      batchingLedgerWriterConfig =
        BatchingLedgerWriterConfig.reasonableDefault.copy(maxBatchConcurrentCommits = 2),
      alwaysPreExecute = false)
}
