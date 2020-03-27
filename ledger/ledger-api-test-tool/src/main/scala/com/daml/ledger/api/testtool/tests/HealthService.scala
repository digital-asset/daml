// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import io.grpc.health.v1.health.HealthCheckResponse

class HealthService(session: LedgerSession) extends LedgerTestSuite(session) {
  test("HScheck", "The Health.Check endpoint reports everything is well", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      for {
        health <- ledger.checkHealth()
      } yield {
        assertEquals("HSisServing", health.status, HealthCheckResponse.ServingStatus.SERVING)
      }
  }

  test("HSwatch", "The Health.Watch endpoint reports everything is well", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      for {
        healthSeq <- ledger.watchHealth()
      } yield {
        val health = assertSingleton("HScontinuesToServe", healthSeq)
        assert(health.status == HealthCheckResponse.ServingStatus.SERVING)
      }
  }
}
