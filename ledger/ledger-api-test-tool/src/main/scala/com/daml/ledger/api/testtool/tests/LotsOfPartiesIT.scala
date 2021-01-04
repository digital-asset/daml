// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import com.daml.ledger.api.testtool.infrastructure.Result.Retired

import scala.concurrent.Future

// This test suite has been retired (see https://github.com/digital-asset/daml/pull/6651)
final class LotsOfPartiesIT extends LedgerTestSuite {

  test(
    "LOPseeTransactionsInMultipleSinglePartySubscriptions",
    "Observers should see transactions in multiple single-party subscriptions",
    allocate(NoParties),
  )(_ => _ => Future.failed(Retired))

  test(
    "LOPseeTransactionsInSingleMultiPartySubscription",
    "Observers should see transactions in a single multi-party subscription",
    allocate(NoParties),
  )(_ => _ => Future.failed(Retired))

  test(
    "LOPseeActiveContractsInMultipleSinglePartySubscriptions",
    "Observers should see active contracts in multiple single-party subscriptions",
    allocate(NoParties),
  )(_ => _ => Future.failed(Retired))

  test(
    "LOPseeActiveContractsInSingleMultiPartySubscription",
    "Observers should see active contracts in a single multi-party subscription",
    allocate(NoParties),
  )(_ => _ => Future.failed(Retired))

}
