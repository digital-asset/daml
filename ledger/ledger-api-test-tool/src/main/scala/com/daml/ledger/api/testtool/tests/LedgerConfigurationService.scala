// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import io.grpc.Status

class LedgerConfigurationService(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] val configSucceeds =
    LedgerTest("ConfigSucceeds", "Return a valid configuration for a valid request") { context =>
      for {
        ledger <- context.participant()
        config <- ledger.configuration()
      } yield {
        assert(config.minTtl.isDefined, "The minTTL field of the configuration is empty")
        assert(config.maxTtl.isDefined, "The maxTTL field of the configuration is empty")
      }
    }

  private[this] val configLedgerId =
    LedgerTest("ConfigLedgerId", "Return NOT_FOUND to invalid ledger identifier") { context =>
      val invalidLedgerId = "THIS_IS_AN_INVALID_LEDGER_ID"
      for {
        ledger <- context.participant()
        failure <- ledger.configuration(overrideLedgerId = Some(invalidLedgerId)).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
      }
    }

  override val tests: Vector[LedgerTest] = Vector(
    configSucceeds,
    configLedgerId
  )
}
