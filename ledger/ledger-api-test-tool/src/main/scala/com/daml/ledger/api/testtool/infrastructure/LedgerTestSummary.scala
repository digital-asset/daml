// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

final case class LedgerTestSummary(
    suite: String,
    test: String,
    configuration: LedgerSessionConfiguration,
    result: Either[Result.Failure, Result.Success],
)
