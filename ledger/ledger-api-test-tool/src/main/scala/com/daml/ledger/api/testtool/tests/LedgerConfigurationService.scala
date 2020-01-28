// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.Allocation._
import com.daml.ledger.api.testtool.infrastructure.Assertions._
import com.daml.ledger.api.testtool.infrastructure.ProtobufConverters._
import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.digitalasset.ledger.test_stable.Test.Dummy
import com.google.protobuf
import io.grpc.{Status, StatusException, StatusRuntimeException}

class LedgerConfigurationService(session: LedgerSession) extends LedgerTestSuite(session) {
  test("ConfigSucceeds", "Return a valid configuration for a valid request", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      for {
        config <- ledger.configuration()
      } yield {
        assert(config.minTtl.isDefined, "The minTTL field of the configuration is empty")
        assert(config.maxTtl.isDefined, "The maxTTL field of the configuration is empty")
      }
  }

  test("ConfigLedgerId", "Return NOT_FOUND to invalid ledger identifier", allocate(NoParties)) {
    case Participants(Participant(ledger)) =>
      val invalidLedgerId = "THIS_IS_AN_INVALID_LEDGER_ID"
      for {
        failure <- ledger.configuration(overrideLedgerId = Some(invalidLedgerId)).failed
      } yield {
        assertGrpcError(failure, Status.Code.NOT_FOUND, s"Ledger ID '$invalidLedgerId' not found.")
      }
  }

  private def setTtl(request: SubmitRequest, ttl: java.time.Duration): SubmitRequest =
    request.update(
      _.commands.modify(commands =>
        commands
          .copy(maximumRecordTime = commands.ledgerEffectiveTime.map(_.asJava.plus(ttl).asProtobuf),
          ),
      ),
    )

  test("ConfigJustMinTtl", "LET+minTTL should be an acceptable MRT", allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      for {
        LedgerConfiguration(Some(minTtl), _) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        _ <- ledger.submit(setTtl(request, minTtl.asJava))
      } yield {
        // Nothing to do, success is enough
      }
  }

  test(
    "ConfigUnderflowMinTtl",
    "LET+minTTL-1 should NOT be an acceptable MRT",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        LedgerConfiguration(Some(minTtl), _) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        failure <- ledger.submit(setTtl(request, minTtl.asJava.minusSeconds(1))).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "out of bounds")
      }
  }

  test("ConfigJustMaxTtl", "LET+maxTTL should be an acceptable MRT", allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      for {
        LedgerConfiguration(_, Some(maxTtl)) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        _ <- ledger.submit(setTtl(request, maxTtl.asJava.minusSeconds(1)))
      } yield {
        // Nothing to do, success is enough
      }
  }

  test(
    "ConfigOverflowMaxTtl",
    "LET+maxTTL+1 should NOT be an acceptable MRT",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      for {
        LedgerConfiguration(_, Some(maxTtl)) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        failure <- ledger.submit(setTtl(request, maxTtl.asJava.plusSeconds(1))).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "out of bounds")
      }
  }

  test(
    "CSLSuccessIfLetRight",
    "Submission returns OK if LET is within the accepted interval",
    allocate(SingleParty),
  ) {
    case Participants(Participant(ledger, party)) =>
      // The maximum accepted clock skew depends on the ledger and is not exposed through the LedgerConfigurationService,
      // and there might be an actual clock skew between the devices running the test and the ledger.
      // This test therefore does not attempt to simulate any clock skew
      // but simply checks whether basic command submission with an unmodified LET works.
      for {
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        _ <- ledger.submit(request)
      } yield {
        // No assertions to make, since the command went through as expected
      }
  }

  test("CSLAbortIfLetHigh", "Submission returns ABORTED if LET is too high", allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      for {
        LedgerConfiguration(_, Some(maxTtl)) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
        invalidRequest = request
          .update(_.commands.ledgerEffectiveTime.modify(overflow(maxTtl)))
          .update(_.commands.maximumRecordTime.modify(overflow(maxTtl)))
        failure <- ledger.submit(invalidRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.ABORTED, "TRANSACTION_OUT_OF_TIME_WINDOW: ")
      }
  }

  test("CSLAbortIfLetLow", "Submission returns ABORTED if LET is too low", allocate(SingleParty)) {
    case Participants(Participant(ledger, party)) =>
      for {
        LedgerConfiguration(_, Some(maxTtl)) <- ledger.configuration()
        request <- ledger.submitRequest(party, Dummy(party).create.command)
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

  private def overflow(
      ttl: protobuf.duration.Duration,
  )(t: protobuf.timestamp.Timestamp): protobuf.timestamp.Timestamp =
    t.asJava.plus(ttl.asJava).plusSeconds(1).asProtobuf

  private def underflow(
      ttl: protobuf.duration.Duration,
  )(t: protobuf.timestamp.Timestamp): protobuf.timestamp.Timestamp =
    t.asJava.minus(ttl.asJava).minusSeconds(1).asProtobuf
}
