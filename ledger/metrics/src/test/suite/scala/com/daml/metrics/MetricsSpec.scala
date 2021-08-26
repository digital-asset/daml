// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MetricsSpec extends AnyFlatSpec with Matchers {

  it should "do it" in {
    val tested = new Metrics(null)

    tested.daml.ledger.database.transactions.TransactionName.ledger_reset.name shouldBe "ledger_reset"

  }

}
