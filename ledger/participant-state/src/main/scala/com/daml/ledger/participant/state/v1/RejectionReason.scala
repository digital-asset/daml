// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

/** Reasons for rejections of transaction submission.
  *
  * Used to provide details for [[Update.CommandRejected]].
  */
sealed abstract class RejectionReason extends Product with Serializable {
  def description: String
}

object RejectionReason {

  /** The transaction relied on contracts or keys being active that were no longer
    * active at the point where it was sequenced or a contract key was being created
    * that already exists.
    * See https://docs.daml.com/concepts/ledger-model/ledger-integrity.html
    * for the definition of ledger consistency.
    */
  final case class Inconsistent(reason: String) extends RejectionReason {
    override def description: String = s"Inconsistent: $reason"
  }

  /** The transaction has been disputed.
    *
    * This means that the underlying ledger and its validation logic
    * considered the transaction potentially invalid. This can be due to a bug
    * in the submission or validation logic, or due to malicious behaviour.
    */
  final case class Disputed(reason: String) extends RejectionReason {
    override def description: String = s"Disputed: $reason"
  }

  /** The Participant node did not have sufficient resources with the
    * ledger to submit the transaction.
    */
  final case class ResourcesExhausted(reason: String) extends RejectionReason {
    override def description: String = s"Resources exhausted: $reason"
  }

  /** A party mentioned as a stakeholder or actor has not been on-boarded on
    * the ledger.
    *
    * This rejection reason is available for ledger that do require some
    * explicit on-boarding steps for a party to exist; e.g., generating key
    * material and registering the party with the ledger-wise
    * identity-manager.
    *
    */
  final case class PartyNotKnownOnLedger(reason: String) extends RejectionReason {
    override def description: String = s"Party not known on ledger: $reason"
  }

  /** The submitter cannot act via this participant.
    *
    * @param reason: details on why the submitter cannot act; e.g., because
    *   it is not hosted on the participant or because its write access to
    *   the ledger has been deactivated.
    *
    */
  final case class SubmitterCannotActViaParticipant(reason: String) extends RejectionReason {
    override def description: String = s"Submitter cannot act via participant: $reason"
  }

  /** The ledger time of the transaction submission violated one of the
    *  following constraints on ledger time:
    *  - The difference between the ledger time and the record time
    *    in the ledger state at which the transaction was sequenced must
    *    stay within bounds defined by the ledger.
    *  - The ledger time of the transaction must be greater than or equal
    *    to the ledger time of any contract used by the transaction.
    */
  final case class InvalidLedgerTime(reason: String) extends RejectionReason {
    override def description: String = s"Invalid ledger time: $reason"
  }
}
