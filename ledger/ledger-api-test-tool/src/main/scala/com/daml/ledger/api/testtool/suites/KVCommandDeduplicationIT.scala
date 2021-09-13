// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.deduplication.CommandDeduplicationBase.DeduplicationFeatures
import com.daml.ledger.api.testtool.infrastructure.deduplication.{
  CommandDeduplicationBase,
  KVCommandDeduplicationBase,
}

import scala.concurrent.duration.FiniteDuration

/** Command deduplication tests for KV ledgers
  * KV ledgers have both participant side deduplication and committer side deduplication.
  * The committer side deduplication period adds `minSkew` to the participant-side one, so we have to account for that as well.
  * If updating the time model fails then the tests will assume a `minSkew` of 1 second.
  */
class KVCommandDeduplicationIT(timeoutScaleFactor: Double, ledgerTimeInterval: FiniteDuration)
    extends KVCommandDeduplicationBase(timeoutScaleFactor, ledgerTimeInterval) {

  override def testNamingPrefix: String = "KVCommandDeduplication"

  override def deduplicationFeatures: CommandDeduplicationBase.DeduplicationFeatures =
    DeduplicationFeatures(
      participantDeduplication = true,
      appendOnlySchema = false,
    )
}
