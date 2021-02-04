// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.IntegrityChecker.ComparisonFailureException
import com.daml.ledger.participant.state.v1.{Offset, RejectionReason, Update}
import com.daml.lf.data.Time

import scala.concurrent.{ExecutionContext, Future}

trait StateUpdateComparison {
  def compare(): Future[Unit]
}

final class ReadServiceStateUpdateComparison(
    expectedReadService: ReplayingReadService,
    actualReadService: ReplayingReadService,
)(implicit
    materializer: Materializer,
    executionContext: ExecutionContext,
) extends StateUpdateComparison {

  import ReadServiceStateUpdateComparison._

  def compare(): Future[Unit] = {
    println("Comparing expected and actual state updates.".white)
    if (expectedReadService.updateCount() != actualReadService.updateCount()) {
      Future.failed(
        new ComparisonFailureException(
          s"Expected ${expectedReadService.updateCount()} state updates but got ${actualReadService.updateCount()}."
        )
      )
    } else {
      expectedReadService
        .stateUpdates(None)
        .zip(actualReadService.stateUpdates(None))
        .mapAsync(1) { case ((expectedOffset, expectedUpdate), (actualOffset, actualUpdate)) =>
          println(s"Comparing offset $expectedOffset...")
          Future.sequence(
            Seq(
              compareOffsets(expectedOffset, actualOffset),
              compareUpdates(expectedUpdate, actualUpdate),
            )
          )
        }
        .runWith(Sink.fold(0)((n, _) => n + 1))
        .map { counter =>
          println(s"Successfully compared $counter state updates.".green)
          println()
        }
    }
  }
}

object ReadServiceStateUpdateComparison {
  private def compareOffsets(expected: Offset, actual: Offset): Future[Unit] =
    if (expected != actual) {
      Future.failed(new ComparisonFailureException(s"Expected offset $expected but got $actual."))
    } else {
      Future.unit
    }

  private def compareUpdates(expectedUpdate: Update, normalizedUpdate: Update): Future[Unit] = {
    val expectedNormalizedUpdate = normalizeUpdate(expectedUpdate)
    // We ignore the record time set later by post-execution because it's unimportant.
    // We only care about the update type and content.
    val actualNormalizedUpdate =
      updateRecordTime(normalizeUpdate(normalizedUpdate), expectedNormalizedUpdate.recordTime)
    if (expectedNormalizedUpdate != actualNormalizedUpdate) {
      Future.failed(
        new ComparisonFailureException(
          "State update mismatch.",
          "Expected:",
          expectedNormalizedUpdate.toString,
          "Actual:",
          actualNormalizedUpdate.toString,
        )
      )
    } else {
      Future.unit
    }
  }

  // Rejection reason strings can change arbitrarily.
  // We just want to check the structured information.
  private def normalizeUpdate(update: Update): Update = update match {
    case Update.ConfigurationChangeRejected(
          recordTime,
          submissionId,
          participantId,
          proposedConfiguration,
          _,
        ) =>
      Update.ConfigurationChangeRejected(
        recordTime = recordTime,
        submissionId = submissionId,
        participantId = participantId,
        proposedConfiguration = proposedConfiguration,
        rejectionReason = "",
      )
    case Update.CommandRejected(recordTime, submitterInfo, rejectionReason) =>
      Update.CommandRejected(
        recordTime,
        submitterInfo,
        discardRejectionReasonDescription(rejectionReason),
      )
    case _ => update
  }

  private def updateRecordTime(update: Update, newRecordTime: Time.Timestamp): Update =
    update match {
      case u: Update.ConfigurationChanged => u.copy(recordTime = newRecordTime)
      case u: Update.ConfigurationChangeRejected => u.copy(recordTime = newRecordTime)
      case u: Update.PartyAddedToParticipant => u.copy(recordTime = newRecordTime)
      case u: Update.PartyAllocationRejected => u.copy(recordTime = newRecordTime)
      case u: Update.PublicPackageUpload => u.copy(recordTime = newRecordTime)
      case u: Update.PublicPackageUploadRejected => u.copy(recordTime = newRecordTime)
      case u: Update.TransactionAccepted => u.copy(recordTime = newRecordTime)
      case u: Update.CommandRejected => u.copy(recordTime = newRecordTime)
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
