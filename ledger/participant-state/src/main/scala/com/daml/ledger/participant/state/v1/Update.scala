// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger
package participant.state.v1

import com.daml.lf.data.Time.Timestamp
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.transaction.BlindingInfo
import com.daml.metrics.TelemetryContext

case class UpdateWithContext(update: Update)(implicit val telemetryContext: TelemetryContext)

/** An update to the (abstract) participant state.
  *
  * [[Update]]'s are used in [[ReadService.stateUpdates]] to communicate
  * changes to abstract participant state to consumers. We describe
  *
  * We describe the possible updates in the comments of
  * each of the case classes implementing [[Update]].
  *
  */
sealed trait Update extends Product with Serializable {

  /** Short human-readable one-line description summarizing the state updates content. */
  def description: String

  /** The record time at which the state change was committed. */
  def recordTime: Timestamp
}

object Update {

  /** Signal that the current [[Configuration]] has changed. */
  final case class ConfigurationChanged(
      recordTime: Timestamp,
      submissionId: SubmissionId,
      participantId: ParticipantId,
      newConfiguration: Configuration)
      extends Update {
    override def description: String =
      s"Configuration change '$submissionId' from participant '$participantId' accepted with configuration: $newConfiguration"
  }

  /** Signal that a configuration change submitted by this participant was rejected.
    */
  final case class ConfigurationChangeRejected(
      recordTime: Timestamp,
      submissionId: SubmissionId,
      participantId: ParticipantId,
      proposedConfiguration: Configuration,
      rejectionReason: String)
      extends Update {
    override def description: String = {
      s"Configuration change '$submissionId' from participant '$participantId' was rejected: $rejectionReason"
    }
  }

  /** Signal that a party is hosted at a participant.
    *
    * @param party
    *   The newly allocated party identifier.
    *
    * @param displayName
    *   The user readable description of the party. May not be unique.
    *
    * @param participantId
    *   The participant that this party was added to.
    *
    * @param recordTime
    *   The ledger-provided timestamp at which the party was allocated.
    *
    * @param submissionId
    *   The submissionId of the command which requested party to be added.
    *
    */
  final case class PartyAddedToParticipant(
      party: Party,
      displayName: String,
      participantId: ParticipantId,
      recordTime: Timestamp,
      submissionId: Option[SubmissionId])
      extends Update {
    override def description: String =
      s"Add party '$party' to participant"
  }

  /** Signal that the party allocation request has been Rejected.
    *
    * Initially this will be visible to all participants in the current open world,
    * with a possible need to revisit as part of the per-party package visibility work
    * https://github.com/digital-asset/daml/issues/311.
    *
    * @param submissionId
    *   submissionId of the party allocation command.
    *
    * @param participantId
    *   The participant to which the party was requested to be added. This field
    *   is informative,
    *
    * @param recordTime
    *   The ledger-provided timestamp at which the party was added.
    *
    * @param rejectionReason
    *   reason for rejection of the party allocation entry
    *
    * Consider whether an enumerated set of reject reasons a la [[RejectionReason]] would be helpful, and whether the same breadth of reject
    * types needs to be handled for party allocation entry rejects
    */
  final case class PartyAllocationRejected(
      submissionId: SubmissionId,
      participantId: ParticipantId,
      recordTime: Timestamp,
      rejectionReason: String)
      extends Update {
    override val description: String =
      s"Request to add party to participant with submissionId '$submissionId' failed"
  }

  /** Signal that a set of new packages has been uploaded.
    *
    * @param archives:
    *   The new packages that have been accepted.
    * @param sourceDescription:
    *   Description of the upload, if provided by the submitter.
    * @param recordTime:
    *   The ledger-provided timestamp at which the package upload was committed.
    * @param submissionId:
    *   The submission id of the upload. Unset if this participant was not the submitter.
    */
  final case class PublicPackageUpload(
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
      recordTime: Timestamp,
      submissionId: Option[SubmissionId])
      extends Update {
    override def description: String =
      s"Public package upload: ${archives.map(_.getHash).mkString(", ")}"
  }

  /** Signal that a package upload has been rejected.
    *
    * @param submissionId:
    *   The submission id of the upload.
    * @param recordTime:
    *   The ledger-provided timestamp at which the package upload was committed.
    * @param rejectionReason:
    *   Reason why the upload was rejected.
    */
  final case class PublicPackageUploadRejected(
      submissionId: SubmissionId,
      recordTime: Timestamp,
      rejectionReason: String)
      extends Update {
    override def description: String =
      s"Public package upload rejected, correlationId=$submissionId reason='$rejectionReason'"
  }

  /** Signal the acceptance of a transaction.
    *
    * @param optSubmitterInfo:
    *   The information provided by the submitter of the command that
    *   created this transaction. It must be provided if the submitter is
    *   hosted at this participant. It can be elided otherwise. This allows
    *   ledgers to implement a fine-grained privacy model.
    *
    * @param transactionMeta:
    *   The metadata of the transaction that was provided by the submitter.
    *   It is visible to all parties that can see the transaction.
    *
    * @param transaction:
    *   The view of the transaction that was accepted. This view must
    *   include at least the projection of the accepted transaction to the
    *   set of all parties hosted at this participant. See
    *   https://docs.daml.com/concepts/ledger-model/ledger-privacy.html
    *   on how these views are computed.
    *
    *   Note that ledgers with weaker privacy models can decide to forgo
    *   projections of transactions and always show the complete
    *   transaction.
    *
    * @param recordTime:
    *   The ledger-provided timestamp at which the transaction was recorded.
    *   The last [[Configuration]] set before this [[TransactionAccepted]]
    *   determines how this transaction's recordTime relates to its
    *   [[TransactionMeta.ledgerEffectiveTime]].
    *
    * @param divulgedContracts:
    *   List of divulged contracts. See [[DivulgedContract]] for details.
    */
  final case class TransactionAccepted(
      optSubmitterInfo: Option[SubmitterInfo],
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      transactionId: TransactionId,
      recordTime: Timestamp,
      divulgedContracts: List[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  ) extends Update {
    override def description: String = s"Accept transaction $transactionId"
  }

  /** Signal that a command submitted via [[WriteService]] was rejected.
    *
    * See the different [[RejectionReason]] for why a command can be
    * rejected.
    */
  final case class CommandRejected(
      recordTime: Timestamp,
      submitterInfo: SubmitterInfo,
      reason: RejectionReason,
  ) extends Update {
    override def description: String = {
      s"Reject command ${submitterInfo.commandId}: $reason"
    }
  }

}
