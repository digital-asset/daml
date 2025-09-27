// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.suites.v2_1

import com.daml.ledger.api.testtool.infrastructure.Allocation.*
import com.daml.ledger.api.testtool.infrastructure.Assertions.*
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import io.grpc.health.v1.health.HealthCheckResponse

class HealthServiceIT extends LedgerTestSuite {
  test("HScheck", "The Health.Check endpoint reports everything is well", allocate(NoParties))(
    implicit ec => { case Participants(Participant(ledger, Seq())) =>
      for {
        health <- ledger.checkHealth()
      } yield {
        assertEquals("HSisServing", health.status, HealthCheckResponse.ServingStatus.SERVING)
      }
    }
  )

  test("HSwatch", "The Health.Watch endpoint reports everything is well", allocate(NoParties))(
    implicit ec => { case Participants(Participant(ledger, Seq())) =>
      for {
        healthSeq <- ledger.watchHealth()
      } yield {
        val health = assertSingleton("HScontinuesToServe", healthSeq)
        assert(health.status == HealthCheckResponse.ServingStatus.SERVING)
      }
    }
  )
}
