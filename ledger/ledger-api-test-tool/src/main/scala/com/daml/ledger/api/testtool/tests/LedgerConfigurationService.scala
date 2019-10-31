// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.digitalasset.ledger.test_stable.Test.Dummy
import com.google.protobuf
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

  private[this] val configJustMinTtl =
    LedgerTest("ConfigJustMinTtl", "LET+minTTL should be an acceptable MRT") { context =>
      for {
        ledger <- context.participant()
        party <- ledger.allocateParty()
        LedgerConfiguration(Some(minTtl), _) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        let = request.getCommands.getLedgerEffectiveTime
        mrt = let.asJava.plus(minTtl.asJava).asProtobuf
        adjustedRequest = request.update(_.commands.maximumRecordTime := mrt)
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
        mrt = let.asJava.plus(minTtl.asJava).minusSeconds(1).asProtobuf
        adjustedRequest = request.update(_.commands.maximumRecordTime := mrt)
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
        mrt = let.asJava.plus(maxTtl.asJava).asProtobuf
        adjustedRequest = request.update(_.commands.maximumRecordTime := mrt)
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
        mrt = let.asJava.plus(maxTtl.asJava).plusSeconds(1).asProtobuf
        adjustedRequest = request.update(_.commands.maximumRecordTime := mrt)
        failure <- ledger.submit(adjustedRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "out of bounds")
      }
    }

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

  private def overflow(ttl: protobuf.duration.Duration)(
      t: protobuf.timestamp.Timestamp): protobuf.timestamp.Timestamp =
    t.asJava.plus(ttl.asJava).plusSeconds(1).asProtobuf

  private def underflow(ttl: protobuf.duration.Duration)(
      t: protobuf.timestamp.Timestamp): protobuf.timestamp.Timestamp =
    t.asJava.minus(ttl.asJava).minusSeconds(1).asProtobuf

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
