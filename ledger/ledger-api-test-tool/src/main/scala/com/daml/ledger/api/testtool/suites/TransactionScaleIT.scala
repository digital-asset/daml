// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Result.Retired

import scala.concurrent.Future

// TODO error codes: Revisit
// This test suite has been retired (see https://github.com/digital-asset/daml/pull/6651)
final class TransactionScaleIT extends LedgerTestSuite {

  test(
    "TXLargeCommand",
    "Accept huge submissions with a large number of commands",
    allocate(NoParties),
  )(_ => { case _ => Future.failed(Retired) })

  test(
    "TXManyCommands",
    "Accept many, large commands at once",
    allocate(NoParties),
  )(_ => { case _ => Future.failed(Retired) })

}
