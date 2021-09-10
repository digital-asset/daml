// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.deduplication.KVCommandDeduplicationBase

import scala.concurrent.duration.FiniteDuration

/** For append-only schemas we can run extra assertions based on the [[com.daml.ledger.api.v1.completion.Completion.submissionId]] and the [[com.daml.ledger.api.v1.completion.Completion.deduplicationPeriod]]
  * Therefore this test suite is more comprehensive compared to [[KVCommandDeduplicationIT]]
  */
class AppendOnlyKVCommandDeduplicationIT(
    timeoutScaleFactor: Double,
    ledgerTimeInterval: FiniteDuration,
) extends KVCommandDeduplicationBase(timeoutScaleFactor, ledgerTimeInterval) {
  override protected def isAppendOnly: Boolean = true

  override protected def testNamingPrefix: String = "AppendOnlyKVCommandDeduplication"
}
