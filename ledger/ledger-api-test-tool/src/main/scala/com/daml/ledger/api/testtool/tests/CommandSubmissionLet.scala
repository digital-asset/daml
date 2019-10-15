// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.time.{Duration, Instant}

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.test_stable.Test.Dummy
import com.google.protobuf.timestamp.Timestamp
import io.grpc.{Status, StatusException, StatusRuntimeException}

final class CommandSubmissionLet(session: LedgerSession) extends LedgerTestSuite(session) {

  private[this] def timestamp(i: Instant): Timestamp =
    new Timestamp(i.getEpochSecond, i.getNano)

  private[this] def instant(t: Timestamp): Instant =
    Instant.ofEpochSecond(t.seconds, t.nanos.toLong)

  /** Moves all time values in the request (the LET and MRT) by the specified amount.
    * This simulates a request from a client with a skewed clock. */
  private[this] def moveRequestTime(
      request: SubmitAndWaitRequest,
      offset: java.time.Duration): SubmitAndWaitRequest =
    request.copy(
      commands = request.commands.map(command =>
        command.copy(
          ledgerEffectiveTime = command.ledgerEffectiveTime.map(let =>
            timestamp(instant(let).plus(offset))),
          maximumRecordTime = command.maximumRecordTime.map(mrt =>
            timestamp(instant(mrt).plus(offset)))
      )))

  private val submitAndWaitAbortIfLetHigh =
    LedgerTest("CSLAbortIfLetHigh", "SubmitAndWait returns ABORTED if LET is too high") { context =>
      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        maxTtl <- ledger.latestMaxTtl()
        request <- ledger
          .submitAndWaitRequest(alice, Dummy(alice).create.command)
          .map(moveRequestTime(_, maxTtl.plus(Duration.ofSeconds(1L))))
        submitFailure <- ledger.submitAndWait(request).failed
      } yield {
        assertGrpcError(submitFailure, Status.Code.ABORTED, "TRANSACTION_OUT_OF_TIME_WINDOW: ")
      }
    }

  private val submitAndWaitSuccessIfLetRight =
    LedgerTest(
      "CSLSuccessIfLetRight",
      "SubmitAndWait returns OK if LET is within the accepted interval") { context =>
      // The maximum accepted clock skew depends on the ledger and is not exposed through the LedgerConfigurationService,
      // and there might be an actual clock skew between the devices running the test and the ledger.
      // This test therefore does not attempt to simulate any clock skew
      // but simply checks whether basic command submission with an unmodified LET works.
      val maxAcceptableSkew = Duration.ofMillis(0L)

      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        request <- ledger
          .submitAndWaitRequest(alice, Dummy(alice).create.command)
          .map(moveRequestTime(_, maxAcceptableSkew))
        _ <- ledger.submitAndWait(request)
      } yield {
        // No assertions to make, since the command went through as expected
        ()
      }
    }

  private val submitAndWaitAbortIfLetLow =
    LedgerTest("CSLAbortIfLetLow", "SubmitAndWait returns ABORTED if LET is too low") { context =>
      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        maxTtl <- ledger.latestMaxTtl()
        request <- ledger
          .submitAndWaitRequest(alice, Dummy(alice).create.command)
          .map(
            moveRequestTime(
              _,
              Duration
                .ofSeconds(0)
                .minus(maxTtl)
                .minus(Duration.ofSeconds(1L))))
        submitFailure <- ledger.submitAndWait(request).failed
      } yield {
        // In this case, the ledger's response races with the client's timeout detection.
        // So we can't be sure what the error message will be.
        submitFailure match {
          case _: StatusRuntimeException => ()
          case _: StatusException => ()
          case _ =>
            assert(
              false,
              "submitAndWait did not fail with a StatusException or StatusRuntimeException")
        }
      }
    }

  override val tests: Vector[LedgerTest] = Vector(
    submitAndWaitAbortIfLetHigh,
    submitAndWaitSuccessIfLetRight,
    submitAndWaitAbortIfLetLow
  )
}
