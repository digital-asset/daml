// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.fetchcontracts

import com.daml.ledger.api.v1

import org.scalatest.wordspec.AsyncWordSpec

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class AcsTxStreamsTest extends AsyncWordSpec {
  "foo" when {
    "bar" should {
      "baz" in {
	val txEnd = v1.transaction.Transaction(offset = "84")
	assert(txEnd == txEnd)
      }
    }
  }
}
