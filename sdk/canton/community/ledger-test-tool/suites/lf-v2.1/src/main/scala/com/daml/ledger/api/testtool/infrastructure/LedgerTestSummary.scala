// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.infrastructure

final case class LedgerTestSummary(
    suite: String,
    name: String,
    description: String,
    result: Either[Result.Failure, Result.Success],
)
