// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.digitalasset.ledger.test_stable.Test.Dummy
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import io.grpc.{Status, StatusException, StatusRuntimeException}

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

  private def sum(t: Timestamp, d: Duration): Timestamp =
    t.withSeconds(t.seconds + d.seconds + ((t.nanos + d.nanos) / 1E9).toLong)
      .withNanos(((t.nanos + d.nanos) % 1E9).toInt)

  private def offset(t: Timestamp, s: Long): Timestamp =
    Timestamp(t.seconds + s, t.nanos)

  private[this] val configJustMinTtl =
    LedgerTest("ConfigJustMinTtl", "LET+minTTL should be an acceptable MRT") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        LedgerConfiguration(Some(minTtl), _) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        let = request.getCommands.getLedgerEffectiveTime
        adjustedRequest = request.update(_.commands.maximumRecordTime := sum(let, minTtl))
        _ <- ledger.submit(adjustedRequest)
      } yield {
        // Nothing to do, success is enough
      }
    }

  private[this] val configUnderflowMinTtl =
    LedgerTest("ConfigUnderflowMinTtl", "LET+minTTL-1 should NOT be an acceptable MRT") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        LedgerConfiguration(Some(minTtl), _) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        let = request.getCommands.getLedgerEffectiveTime
        adjustedRequest = request.update(
          _.commands.maximumRecordTime := offset(sum(let, minTtl), -1))
        failure <- ledger.submit(adjustedRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "out of bounds")
      }
    }

  private[this] val configJustMaxTtl =
    LedgerTest("ConfigJustMaxTtl", "LET+maxTTL should be an acceptable MRT") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        LedgerConfiguration(_, Some(maxTtl)) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        let = request.getCommands.getLedgerEffectiveTime
        adjustedRequest = request.update(_.commands.maximumRecordTime := sum(let, maxTtl))
        _ <- ledger.submit(adjustedRequest)
      } yield {
        // Nothing to do, success is enough
      }
    }

  private[this] val configOverflowMaxTtl =
    LedgerTest("ConfigOverflowMaxTtl", "LET+maxTTL+1 should NOT be an acceptable MRT") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        LedgerConfiguration(_, Some(maxTtl)) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        let = request.getCommands.getLedgerEffectiveTime
        adjustedRequest = request.update(
          _.commands.maximumRecordTime := offset(sum(let, maxTtl), 1))
        failure <- ledger.submit(adjustedRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "out of bounds")
      }
    }

  // Adds (maxTTL+1) seconds to a Timestamp
  private def overflow(maxTtl: Duration)(t: Timestamp): Timestamp =
    t.withSeconds(t.seconds + maxTtl.seconds + ((t.nanos + maxTtl.nanos) / 1E9).toLong + 1L)
      .withNanos(((t.nanos + maxTtl.nanos) % 1E9).toInt)

  // Subtracts (maxTTL+1) seconds from a Timestamp
  private def underflow(maxTtl: Duration)(t: Timestamp): Timestamp =
    t.withSeconds(t.seconds - maxTtl.seconds + ((t.nanos + maxTtl.nanos) / 1E9).toLong - 1L)
      .withNanos(((t.nanos - maxTtl.nanos) % 1E9).toInt)

  private[this] val submitSuccessIfLetRight =
    LedgerTest(
      "CSLSuccessIfLetRight",
      "Submission returns OK if LET is within the accepted interval") { context =>
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

  private[this] val submitAbortIfLetHigh =
    LedgerTest("CSLAbortIfLetHigh", "Submission returns ABORTED if LET is too high") { context =>
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

  private[this] val submitAbortIfLetLow =
    LedgerTest("CSLAbortIfLetLow", "Submission returns ABORTED if LET is too low") { context =>
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
    configSucceeds,
    configLedgerId,
    configJustMinTtl,
    configUnderflowMinTtl,
    configJustMaxTtl,
    configOverflowMaxTtl,
    submitSuccessIfLetRight,
    submitAbortIfLetHigh,
    submitAbortIfLetLow
  )
}
