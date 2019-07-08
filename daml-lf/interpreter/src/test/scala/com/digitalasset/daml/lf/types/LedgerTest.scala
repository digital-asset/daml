// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy

import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.types.Ledger
import com.digitalasset.daml.lf.types.Ledger.ScenarioTransactionId
import org.scalatest.{Matchers, WordSpec}

class LedgerTest extends WordSpec with Matchers {
  "Ledger" should {
    "be initialized with a step id of -1 having the given TimeStamp" in {
      val ts = Time.Timestamp.MinValue
      val ledger = Ledger.initialLedger(ts)
      ledger.scenarioStepId shouldEqual ScenarioTransactionId(-1)
      ledger.currentTime shouldEqual ts
    }
  }
}
