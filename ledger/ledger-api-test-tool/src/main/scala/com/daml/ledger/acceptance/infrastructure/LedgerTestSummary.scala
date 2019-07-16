// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

final case class LedgerTestSummary(
    suite: String,
    test: String,
    configuration: LedgerSessionConfiguration,
    result: Result)
