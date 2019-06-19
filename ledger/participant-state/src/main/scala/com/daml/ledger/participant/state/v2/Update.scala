// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger
package participant.state.v2

import java.time.Instant

import com.digitalasset.daml.lf.value.Value
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
  final case class Heartbeat(recordTime: Instant) extends Update {
    override def description: String = s"Heartbeat: $recordTime"
  }

  /** Signal that the current [[Configuration]] has changed. */
  final case class ConfigurationChanged(newConfiguration: Configuration) extends Update {
    override def description: String =
      s"Configuration changed to: $newConfiguration"
  }

  /** Signal that a party is hosted at this participant.
    *
    * As explained in the note on [[ReadService.stateUpdates]], the
    * state updates are only expected to signal all updates pertaining
    * to data affecting the parties hosted at the participant.
    *
    */
  final case class PartyAddedToParticipant(party: Party) extends Update {
    override def description: String = s"Add party '$party' to participant"
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
    */
  final case class PublicPackageUploaded(archive: DamlLf.Archive) extends Update {
    override def description: String = s"Public package ${archive.getHash} uploaded"
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
    *   the metadata of the transaction that was provided by the submitter.
    *   It is visible to all parties that can see the transaction.
    *
    * @param transaction:
    *   the view of the transaction that was accepted. This view must
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
    * @param referencedContracts:
    *   A list of all contracts that were created before this transaction
    *   and referenced by it (via fetch, consuming, or non-consuming
    *   exercise nodes). This list is provided to enable consumers of
    *   [[ReadService.stateUpdates]] to implement the divulgence semantics
    *   as described here:
    *   https://docs.daml.com/concepts/ledger-model/ledger-privacy.html
    *
    */
  final case class TransactionAccepted(
      optSubmitterInfo: Option[SubmitterInfo],
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      transactionId: TransactionId,
      recordTime: Instant,
      referencedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)]
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
