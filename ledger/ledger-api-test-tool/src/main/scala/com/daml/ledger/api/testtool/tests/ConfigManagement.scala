// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.admin.config_management_service.TimeModel
import com.google.protobuf.duration.Duration
import io.grpc.Status

final class ConfigManagement(session: LedgerSession) extends LedgerTestSuite(session) {
  test(
    "CMSetAndGetTimeModel",
    "It should be able to get, set and restore the time model",
    allocate(NoParties),
  ) {

    case Participants(Participant(ledger)) =>
      val newTimeModel = TimeModel(
        minTransactionLatency = Some(Duration(0, 1)),
        maxClockSkew = Some(Duration(60, 0)),
        maxTtl = Some(Duration(120, 0)),
      )
      for {
        // Get the current time model
        response1 <- ledger.getTimeModel()
        oldTimeModel = {
          assert(response1.timeModel.isDefined, "Expected time model to be defined")
          response1.timeModel.get
        }

        // Set a new temporary time model
        t1 <- ledger.time()
        _ <- ledger.setTimeModel(
          mrt = t1.plusSeconds(30),
          generation = response1.configurationGeneration,
          newTimeModel = newTimeModel,
        )

        // Retrieve the new model
        response2 <- ledger.getTimeModel()

        // Restore the original time model
        t2 <- ledger.time()
        _ <- ledger.setTimeModel(
          mrt = t2.plusSeconds(30),
          generation = response2.configurationGeneration,
          newTimeModel = oldTimeModel,
        )

        // Verify that we've restored the original time model
        response3 <- ledger.getTimeModel()

        // Try to set a time model with an expired MRT.
        t3 <- ledger.time()
        expiredMRTFailure <- ledger
          .setTimeModel(
            mrt = t3.minusSeconds(10),
            generation = response3.configurationGeneration,
            newTimeModel = oldTimeModel,
          )
          .failed
      } yield {
        assert(
          response1.configurationGeneration < response2.configurationGeneration,
          "Expected configuration generation to have increased after setting time model",
        )
        assert(
          response2.configurationGeneration < response3.configurationGeneration,
          "Expected configuration generation to have increased after setting time model the second time",
        )
        assert(response2.timeModel.equals(Some(newTimeModel)), "Setting the new time model failed")
        assert(
          response3.timeModel.equals(response1.timeModel),
          "Restoring the original time model failed",
        )

        assertGrpcError(expiredMRTFailure, Status.Code.ABORTED, "")
      }
  }

  // TODO(JM): Test that sets the time model and verifies that a transaction with invalid
  // ttl/mrt won't be accepted. Can only implement once ApiSubmissionService properly
  // uses currently set configuration.
}
