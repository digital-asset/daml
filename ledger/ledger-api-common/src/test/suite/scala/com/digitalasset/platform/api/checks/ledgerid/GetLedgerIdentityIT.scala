// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.api.checks.ledgerid

import com.daml.ledger.api.v1.ledger_identity_service.GetLedgerIdentityRequest
import com.daml.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpec}

class GetLedgerIdentityIT(service: => LedgerIdentityService, expectedLedgerId: String)
    extends WordSpec
    with Matchers
    with ScalaFutures {

  "Ledger Identity Service" when {

    "Ledger Identity is requested" should {

      "return it" in {

        whenReady(service.getLedgerIdentity(GetLedgerIdentityRequest()))(
          _.ledgerId shouldEqual expectedLedgerId)
      }
    }
  }
}
