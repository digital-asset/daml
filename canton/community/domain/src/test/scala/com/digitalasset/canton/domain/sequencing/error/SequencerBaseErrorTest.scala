// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.error

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.error.{FabricErrors, SequencerBaseError}
import org.scalatest.wordspec.AnyWordSpec

class SequencerBaseErrorTest extends AnyWordSpec with BaseTest {

  "stringFromContext" should {
    "produce a string" in {
      SequencerBaseError.stringFromContext(
        FabricErrors.TransactionErrors.InvalidTransaction.Warn("fcn", "msg", 1L)
      ) shouldBe "FABRIC_TRANSACTION_INVALID(5,0): At block 1 found invalid fcn transaction. That indicates malicious " +
        "or faulty behavior, so skipping it. Error: msg; blockHeight=1, fcn=fcn, test=SequencerBaseErrorTest"
    }
  }
}
