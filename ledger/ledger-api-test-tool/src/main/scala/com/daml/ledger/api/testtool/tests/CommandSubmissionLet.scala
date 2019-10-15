// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.digitalasset.ledger.test_stable.Test.Dummy
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import io.grpc.{Status, StatusException, StatusRuntimeException}

final class CommandSubmissionLet(session: LedgerSession) extends LedgerTestSuite(session) {

  // Adds (maxTTL+1) seconds to a Timestamp
  private def overflow(maxTtl: Duration)(t: Timestamp): Timestamp =
    t.withSeconds(t.seconds + maxTtl.seconds + ((t.nanos + maxTtl.nanos) / 1E9).toLong + 1L)
      .withNanos(((t.nanos + maxTtl.nanos) % 1E9).toInt)

  // Subtracts (maxTTL+1) seconds from a Timestamp
  private def underflow(maxTtl: Duration)(t: Timestamp): Timestamp =
    t.withSeconds(t.seconds - maxTtl.seconds + ((t.nanos - maxTtl.nanos) / 1E9).toLong - 1L)
      .withNanos(((t.nanos - maxTtl.nanos) % 1E9).toInt)

  private[this] val submitAndWaitSuccessIfLetRight =
    LedgerTest(
      "CSLSuccessIfLetRight",
      "SubmitAndWait returns OK if LET is within the accepted interval") { context =>
      // The maximum accepted clock skew depends on the ledger and is not exposed through the LedgerConfigurationService,
      // and there might be an actual clock skew between the devices running the test and the ledger.
      // This test therefore does not attempt to simulate any clock skew
      // but simply checks whether basic command submission with an unmodified LET works.

      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        request <- ledger.submitRequest(alice, Dummy(alice).create.command)
        _ <- ledger.submit(request)
      } yield {
        // No assertions to make, since the command went through as expected
      }
    }

  private[this] val submitAndWaitAbortIfLetHigh =
    LedgerTest("CSLAbortIfLetHigh", "SubmitAndWait returns ABORTED if LET is too high") { context =>
      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        LedgerConfiguration(_, Some(maxTtl)) <- ledger.configuration()
        request <- ledger.submitRequest(alice, Dummy(alice).create.command)
        invalidRequest = request
          .update(_.commands.ledgerEffectiveTime.modify(overflow(maxTtl)))
          .update(_.commands.maximumRecordTime.modify(overflow(maxTtl)))
        failure <- ledger.submit(invalidRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.ABORTED, "TRANSACTION_OUT_OF_TIME_WINDOW: ")
      }
    }

  private[this] val submitAndWaitAbortIfLetLow =
    LedgerTest("CSLAbortIfLetLow", "SubmitAndWait returns ABORTED if LET is too low") { context =>
      for {
        ledger <- context.participant()
        alice <- ledger.allocateParty()
        LedgerConfiguration(_, Some(maxTtl)) <- ledger.configuration()
        request <- ledger.submitRequest(alice, Dummy(alice).create.command)
        invalidRequest = request
          .update(_.commands.ledgerEffectiveTime.modify(underflow(maxTtl)))
          .update(_.commands.maximumRecordTime.modify(underflow(maxTtl)))
        failure <- ledger.submit(invalidRequest).failed
      } yield {
        // In this case, the ledger's response races with the client's timeout detection.
        // So we can't be sure what the error message will be.
        failure match {
          case _: StatusRuntimeException | _: StatusException => ()
          case _ => fail("Submission should have failed with gRPC exception")
        }
      }
    }

  override val tests: Vector[LedgerTest] = Vector(
    submitAndWaitSuccessIfLetRight,
    submitAndWaitAbortIfLetHigh,
    submitAndWaitAbortIfLetLow
  )
}
