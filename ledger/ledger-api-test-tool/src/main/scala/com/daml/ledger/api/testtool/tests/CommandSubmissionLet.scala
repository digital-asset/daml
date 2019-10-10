// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import java.time.Duration

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.test_stable.Test.Dummy
import io.grpc.{Status, StatusException, StatusRuntimeException}

final class CommandSubmissionLet(session: LedgerSession) extends LedgerTestSuite(session) {

  private val submitAndWaitAbortIfLetHigh =
    LedgerTest("CSLAbortIfLetHigh", "SubmitAndWait returns ABORTED if LET is too high") { context =>
      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        maxTtl <- ledger.latestMaxTtl()
        request <- ledger
          .submitAndWaitRequest(alice, Dummy(alice).create.command)
          .map(ledger.moveRequestTime(_, maxTtl.plus(Duration.ofSeconds(1L))))
        submitFailure <- ledger.submitAndWait(request).failed
      } yield {
        assertGrpcError(submitFailure, Status.Code.ABORTED, "TRANSACTION_OUT_OF_TIME_WINDOW: ")
      }
    }

  private val submitAndWaitSuccessIfLetRight =
    LedgerTest(
      "CSLSuccessIfLetRight",
      "SubmitAndWait returns OK if LET is within the accepted interval") { context =>
      // A time skew where the client time runs `maxTtl` behind the ledger time,
      // plus some small delay to account for actual network delays.
      def maxAcceptableSkew(maxTtl: Duration) = {
        Duration
          .ofSeconds(0)
          .minus(maxTtl)
          .plus(Duration.ofSeconds(1L))
      }

      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        maxTtl <- ledger.latestMaxTtl()
        skew = maxAcceptableSkew(maxTtl)
        request <- ledger
          .submitAndWaitRequest(alice, Dummy(alice).create.command)
          .map(ledger.moveRequestTime(_, skew))
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
            ledger.moveRequestTime(
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
