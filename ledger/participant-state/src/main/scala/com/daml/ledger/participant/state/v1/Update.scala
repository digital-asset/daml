// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger
package participant.state.v1

import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml_lf.DamlLf

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
}

object Update {

  /** Signal aliveness and the current record time.  */
  final case class Heartbeat(recordTime: Timestamp) extends Update {
    override def description: String = s"Heartbeat: $recordTime"
  }

  /** Signal that the current [[Configuration]] has changed. */
  final case class ConfigurationChanged(submissionId: String, newConfiguration: Configuration)
      extends Update {
    override def description: String =
      s"Configuration changed to: $newConfiguration"
  }

  /** Signal that a configuration change submitted by this participant was rejected.
    */
  final case class ConfigurationChangeRejected(submissionId: String, reason: String)
      extends Update {
    override def description: String = {
      s"Configuration change '$submissionId' was rejected: $reason"
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
    */
  final case class PartyAddedToParticipant(
      party: Party,
      displayName: String,
      participantId: ParticipantId,
      recordTime: Timestamp)
      extends Update {
    override def description: String =
      s"Add party '$party' to participant"
  }

  /** Signal the uploading of a package that is publicly visible.
    *
    * We expect that ledger or participant-node administrators issue such
    * public uploads. The 'public' qualifier refers to the fact that all
    * parties hosted by a participant (or even all parties connected to a
    * ledger) will see the uploaded package. It is in contrast to a future
    * extension where we plan to support per-party package visibility
    * https://github.com/digital-asset/daml/issues/311.
    *
    *
    * @param archive
    *   The DAML-LF package that was uploaded.
    *
    * @param sourceDescription
    *   A description of the package, provided by the administrator as part of
    *   the upload.
    *
    * @param participantId
    *   The participant through which the package was uploaded. This field
    *   is informative, and can be used by applications to display information
    *   about the origin of the package.
    *
    * @param recordTime
    *   The ledger-provided timestamp at which the package was uploaded.
    *
    */
  final case class PublicPackageUploaded(
      archive: DamlLf.Archive,
      sourceDescription: Option[String],
      participantId: ParticipantId,
      recordTime: Timestamp)
      extends Update {
    override def description: String =
      s"""Public package uploaded: ${archive.getHash}"""
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
      divulgedContracts: List[DivulgedContract]
  ) extends Update {
    override def description: String = s"Accept transaction $transactionId"
  }

  /** Signal that a command submitted via [[WriteService]] was rejected.
    *
    * See the different [[RejectionReason]] for why a command can be
    * rejected.
    */
  final case class CommandRejected(
      submitterInfo: SubmitterInfo,
      reason: RejectionReason,
  ) extends Update {
    override def description: String = {
      s"Reject command ${submitterInfo.commandId}: $reason"
    }
  }

}
