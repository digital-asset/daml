// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.suites.v1_8

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.LedgerTestSuite
import io.grpc.health.v1.health.HealthCheckResponse

class HealthServiceIT extends LedgerTestSuite {
  test("HScheck", "The Health.Check endpoint reports everything is well", allocate(NoParties))(
    implicit ec => { case Participants(Participant(ledger)) =>
      for {
        health <- ledger.checkHealth()
      } yield {
        assertEquals("HSisServing", health.status, HealthCheckResponse.ServingStatus.SERVING)
      }
    }
  )

  test("HSwatch", "The Health.Watch endpoint reports everything is well", allocate(NoParties))(
    implicit ec => { case Participants(Participant(ledger)) =>
      for {
        healthSeq <- ledger.watchHealth()
      } yield {
        val health = assertSingleton("HScontinuesToServe", healthSeq)
        assert(health.status == HealthCheckResponse.ServingStatus.SERVING)
      }
    }
  )
}
