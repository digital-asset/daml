// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.time.Instant

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
    allocate(NoParties)) {

    case Participants(Participant(ledger)) =>
      val newTimeModel = TimeModel(
        minTransactionLatency = Some(Duration(0, 1)),
        maxClockSkew = Some(Duration(1, 2)),
        maxTtl = Some(Duration(2, 3))
      )
      for {
        // Get the current time model
        getResponse <- ledger.getTimeModel()
        oldTimeModel = {
          assert(getResponse.timeModel.isDefined, "Expected time model to be defined")
          getResponse.timeModel.get
        }

        // Set a new temporary time model
        _ <- ledger.setTimeModel(
          mrt = Instant.now.plusSeconds(30),
          generation = getResponse.configurationGeneration,
          newTimeModel = newTimeModel
        )

        // Retrieve the new model
        getResponse2 <- ledger.getTimeModel()

        // Restore the original time model
        _ <- ledger.setTimeModel(
          mrt = Instant.now.plusSeconds(30),
          generation = getResponse2.configurationGeneration,
          newTimeModel = oldTimeModel
        )

        // Verify that we've restored the original time model
        getResponse3 <- ledger.getTimeModel()
      } yield {
        assert(
          getResponse.configurationGeneration < getResponse2.configurationGeneration,
          "Expected configuration generation to have increased after setting time model"
        )
        assert(
          getResponse2.configurationGeneration < getResponse3.configurationGeneration,
          "Expected configuration generation to have increased after setting time model the second time"
        )
        assert(
          getResponse2.timeModel.equals(Some(newTimeModel)),
          "Setting the new time model failed")
        assert(
          getResponse3.timeModel.equals(getResponse.timeModel),
          "Restoring the original time model failed")
      }
  }

  test("CMSetTimeModelTimeout", "It should time out requests with too low MRT", allocate(NoParties)) {

    case Participants(Participant(ledger)) =>
      for {
        // Get the current time model
        getResponse <- ledger.getTimeModel()
        oldTimeModel = {
          assert(getResponse.timeModel.isDefined, "Expected time model to be defined")
          getResponse.timeModel.get
        }

        // Try to set a time model with an expired MRT.
        failure <- ledger
          .setTimeModel(
            mrt = Instant.now.minusSeconds(10),
            generation = getResponse.configurationGeneration,
            newTimeModel = oldTimeModel
          )
          .failed
      } yield {
        assertGrpcError(failure, Status.Code.ABORTED, "")
      }
  }

  // TODO(JM): Test that sets the time model and verifies that a transaction with invalid
  // ttl/mrt won't be accepted. Can only implement once ApiSubmissionService properly
  // uses currently set configuration.
}
