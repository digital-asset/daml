// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import com.daml.ledger.api.testtool.infrastructure.{LedgerSession, LedgerTest, LedgerTestSuite}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfiguration
import com.digitalasset.ledger.test_stable.Test.Dummy
import com.google.protobuf.duration.Duration
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Status

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
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        let = request.getCommands.getLedgerEffectiveTime
        adjustedRequest = request.update(_.commands.maximumRecordTime := sum(let, minTtl))
        _ <- ledger.submitAndWait(adjustedRequest)
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
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        let = request.getCommands.getLedgerEffectiveTime
        adjustedRequest = request.update(
          _.commands.maximumRecordTime := offset(sum(let, minTtl), -1))
        failure <- ledger.submitAndWait(adjustedRequest).failed
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
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        let = request.getCommands.getLedgerEffectiveTime
        adjustedRequest = request.update(_.commands.maximumRecordTime := sum(let, maxTtl))
        _ <- ledger.submitAndWait(adjustedRequest)
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
        request <- ledger.submitAndWaitRequest(party, Dummy(party).create.command)
        let = request.getCommands.getLedgerEffectiveTime
        adjustedRequest = request.update(
          _.commands.maximumRecordTime := offset(sum(let, maxTtl), 1))
        failure <- ledger.submitAndWait(adjustedRequest).failed
      } yield {
        assertGrpcError(failure, Status.Code.INVALID_ARGUMENT, "out of bounds")
      }
    }

  override val tests: Vector[LedgerTest] = Vector(
    configSucceeds,
    configLedgerId,
    configJustMinTtl,
    configUnderflowMinTtl,
    configJustMaxTtl,
    configOverflowMaxTtl
  )
}
