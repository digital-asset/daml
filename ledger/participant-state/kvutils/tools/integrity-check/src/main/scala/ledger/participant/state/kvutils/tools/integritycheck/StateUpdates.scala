// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.IntegrityChecker.ComparisonFailureException
import com.daml.ledger.participant.state.v1.{Offset, RejectionReason, Update}

import scala.concurrent.{ExecutionContext, Future}

final class StateUpdates(
    expectedReadService: ReplayingReadService,
    actualReadService: ReplayingReadService,
) {
  import StateUpdates._

  def compare()(
      implicit materializer: Materializer,
      executionContext: ExecutionContext,
  ): Future[Unit] = {
    println(s"Comparing expected and actual state updates.".white)
    if (expectedReadService.updateCount() != actualReadService.updateCount()) {
      Future.failed(new ComparisonFailureException(
        s"Expected ${expectedReadService.updateCount()} state updates but got ${actualReadService.updateCount()}.",
      ))
    } else {
      expectedReadService
        .stateUpdates(None)
        .zip(actualReadService.stateUpdates(None))
        .mapAsync(1) {
          case ((expectedOffset, expectedUpdate), (actualOffset, actualUpdate)) =>
            println(s"Comparing offset $expectedOffset...")
            Future.sequence(
              Seq(
                compareOffsets(expectedOffset, actualOffset),
                compareUpdates(expectedUpdate, actualUpdate),
              ))
        }
        .runWith(Sink.fold(0)((n, _) => n + 1))
        .map { counter =>
          println(s"Successfully compared $counter state updates.".green)
          println()
        }
    }
  }
}

object StateUpdates {
  private def compareOffsets(expected: Offset, actual: Offset): Future[Unit] =
    if (expected != actual) {
      Future.failed(new ComparisonFailureException(s"Expected offset $expected but got $actual."))
    } else {
      Future.unit
    }

  private def compareUpdates(expected: Update, actual: Update): Future[Unit] = {
    val almostExpected = normalizeUpdate(expected)
    val almostActual = normalizeUpdate(actual)
    if (almostExpected != almostActual) {
      Future.failed(
        new ComparisonFailureException(
          "State update mismatch.",
          "Expected:",
          almostExpected.toString,
          "Actual:",
          almostActual.toString,
        ))
    } else {
      Future.unit
    }
  }

  private def normalizeUpdate(update: Update): Update = update match {
    // Rejection reasons can change arbitrarily. We just want to check the type.
    case Update.CommandRejected(recordTime, submitterInfo, rejectionReason) =>
      Update.CommandRejected(
        recordTime,
        submitterInfo,
        discardRejectionReasonDescription(rejectionReason),
      )
    case _ => update
  }

  private def discardRejectionReasonDescription(reason: RejectionReason): RejectionReason =
    reason match {
      case RejectionReason.Disputed(_) =>
        RejectionReason.Disputed("")
      case RejectionReason.Inconsistent(_) =>
        RejectionReason.Inconsistent("")
      case RejectionReason.InvalidLedgerTime(_) =>
        RejectionReason.InvalidLedgerTime("")
      case RejectionReason.PartyNotKnownOnLedger(_) =>
        RejectionReason.PartyNotKnownOnLedger("")
      case RejectionReason.ResourcesExhausted(_) =>
        RejectionReason.ResourcesExhausted("")
      case RejectionReason.SubmitterCannotActViaParticipant(_) =>
        RejectionReason.SubmitterCannotActViaParticipant("")
    }
}
