// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

final case class LedgerTestSummary(
    suite: String,
    name: String,
    description: String,
    result: Either[Result.Failure, Result.Success],
)
