// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import com.daml.ledger.participant.state.kvutils.api.BatchingLedgerWriterConfig

final case class ExtraConfig(
    maxInboundMessageSize: Int,
    batchingLedgerWriterConfig: BatchingLedgerWriterConfig
)

object ExtraConfig {
  val defaultMaxInboundMessageSize: Int = 4 * 1024 * 1024
  val default =
    ExtraConfig(defaultMaxInboundMessageSize, BatchingLedgerWriterConfig.reasonableDefault)
}
