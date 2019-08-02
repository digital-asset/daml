// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

sealed abstract case class LedgerTestSummaries(
    summaries: Vector[LedgerTestSummary],
    failure: Boolean)

object LedgerTestSummaries {
  def apply(summaries: Vector[LedgerTestSummary]): LedgerTestSummaries =
    new LedgerTestSummaries(summaries, summaries.exists(_.result.failure)) {}
}
