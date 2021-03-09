// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.IntegrityChecker.ComparisonFailureException
import com.daml.ledger.participant.state.v1.{Offset, RejectionReason, TransactionId, Update}
import com.daml.lf.data.Time
import com.daml.lf.transaction.{
  CommittedTransaction,
  Node,
  TransactionVersion,
  VersionedTransaction,
}

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
              compareUpdates(expectedUpdate, actualUpdate, DefaultNormalizationSettings),
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

  case class NormalizationSettings(
      ignoreBlindingInfo: Boolean = false,
      ignoreTransactionId: Boolean = false,
      ignoreFetchAndLookupByKeyNodes: Boolean = false,
  )

  val DefaultNormalizationSettings: NormalizationSettings = NormalizationSettings(
    ignoreBlindingInfo = true,
    ignoreTransactionId = true,
    ignoreFetchAndLookupByKeyNodes = true,
  )

  private def compareOffsets(expected: Offset, actual: Offset): Future[Unit] =
    if (expected != actual) {
      Future.failed(new ComparisonFailureException(s"Expected offset $expected but got $actual."))
    } else {
      Future.unit
    }

  private[integritycheck] def compareUpdates(
      expectedUpdate: Update,
      actualUpdate: Update,
      normalizationSettings: NormalizationSettings,
  ): Future[Unit] = {
    val expectedNormalizedUpdate = normalizeUpdate(expectedUpdate, normalizationSettings)
    // We ignore the record time set later by post-execution because it's unimportant.
    // We only care about the update type and content.
    val actualNormalizedUpdate =
      updateRecordTime(
        normalizeUpdate(actualUpdate, normalizationSettings),
        expectedNormalizedUpdate.recordTime,
      )
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

  // Normalizes updates so that ones generated by different versions of the Daml SDK may be compared.
  // I.e., we just want to check the structured information.
  // Rejection reason strings are always ignored as they can change arbitrarily.
  // Normalization settings control whether we ignore the blinding info or the transaction ID in TransactionAccepted
  // updates:
  //   - We may not want to check blinding info as we haven't always populated these.
  //   - We may not want to care about fetch and lookup-by-key nodes in the transaction tree attached to a
  //   TransactionAccepted event as in some Daml SDK versions we are dropping them.
  //   - We may not want to check transaction ID as it is generated from the serialized form of a Daml submission which
  //   is not expected to stay the same across Daml SDK versions.
  private def normalizeUpdate(
      update: Update,
      normalizationSettings: NormalizationSettings,
  ): Update = update match {
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
    case transactionAccepted: Update.TransactionAccepted =>
      transactionAccepted.copy(
        transactionId =
          if (normalizationSettings.ignoreTransactionId)
            TransactionId.assertFromString("ignored")
          else
            transactionAccepted.transactionId,
        blindingInfo =
          if (normalizationSettings.ignoreBlindingInfo)
            None
          else
            transactionAccepted.blindingInfo,
        transaction =
          if (normalizationSettings.ignoreFetchAndLookupByKeyNodes) {
            CommittedTransaction(dropFetchAndLookupByKeyNodes(transactionAccepted.transaction))
          } else
            transactionAccepted.transaction,
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

  private def dropFetchAndLookupByKeyNodes[Nid, Cid](
      tx: VersionedTransaction[Nid, Cid]
  ): VersionedTransaction[Nid, Cid] = {
    val nodes = tx.nodes.filter {
      case (_, _: Node.NodeFetch[Cid] | _: Node.NodeLookupByKey[Cid]) => false
      case _ => true
    }
    val filteredNodes = nodes.map {
      case (nid, node: Node.NodeExercises[Nid, Cid]) =>
        // FIXME(miklos): Simple copy doesn't work because of a scalaz.Equals clash on the classpath.
        // val filteredNode = node.copy(children = node.children.filter(nodes.contains))
        val filteredNode = Node.NodeExercises(
          targetCoid = node.targetCoid,
          templateId = node.templateId,
          choiceId = node.choiceId,
          optLocation = node.optLocation,
          consuming = node.consuming,
          actingParties = node.actingParties,
          chosenValue = node.chosenValue,
          stakeholders = node.stakeholders,
          signatories = node.signatories,
          choiceObservers = node.choiceObservers,
          children = node.children.filter(nodes.contains),
          exerciseResult = node.exerciseResult,
          key = node.key,
          byKey = node.byKey,
          version = node.version,
        )
        (nid, filteredNode)
      case keep => keep
    }
    val filteredRoots = tx.roots.filter(nodes.contains)
    // FIXME(miklos): Ordering defined within TransactionVersion is not accessible here.
    // val version = roots.iterator.foldLeft(TransactionVersion.minVersion)((acc, nodeId) =>
    //   acc max nodes(nodeId).version
    // )
    VersionedTransaction(TransactionVersion.maxVersion, filteredNodes, filteredRoots)
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
