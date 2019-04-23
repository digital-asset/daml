// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

/** Reasons for rejections of transaction submission.
  *
  * Used to provide details for [[Update.CommandRejected]].
  */
sealed trait RejectionReason extends Product with Serializable {
  def description: String
}

object RejectionReason {

  /** The transaction relied on contracts being active that were no longer
    * active at the point where it was sequenced.  See
    * https://docs.daml.com/concepts/ledger-model/ledger-integrity.html
    * for the definition of ledger consistency.
    */
  final case object Inconsistent extends RejectionReason {
    override def description: String = "Inconsistent"
  }

  /** The transaction has been disputed.
    *
    * This means that the underlying ledger and its validation logic
    * considered the transaction potentially invalid. This can be due to a bug
    * in the submission or validation logic, or due to malicious behaviour.
    */
  final case class Disputed(reason: String) extends RejectionReason {
    override def description: String = "Disputed: " + reason
  }

  /** The Participant node did not have sufficient resources with the
    * ledger to submit the transaction.
    */
  final case object ResourcesExhausted extends RejectionReason {
    override def description: String = "Resources exhausted"
  }

  /** The transaction submission exceeded its maximum record time.
    *
    * This means the 'maximumRecordTime' was smaller than the record time
    * in the ledger state at which the transaction was sequenced.
    */
  final case object MaximumRecordTimeExceeded extends RejectionReason {
    override def description: String =
      "The maximum record time of the command exceeded"
  }

  /** The participant or ledger has already accepted a transaction with the
    * same command-id.

    * The guarantee provided by the ledger is to never store two transactions
    * with [[SubmitterInfo]] with the same '(submitter, applicationId,
    * commandId)' tuple.
    *
    * This is used to protect against duplicate submissions of transactions
    * that do not consume any contract; e.g., a transaction creating a
    * contract. These transactions can be sometimes submitted twice in case
    * of faults in the submitting application.
    */
  final case object DuplicateCommand extends RejectionReason {
    override def description: String = "Duplicate command"
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
  final case object PartyNotKnownOnLedger extends RejectionReason {
    override def description: String = "Party not known on ledger"
  }

  /** The submitter cannot act via this participant.
    *
    * @param details: details on why the submitter cannot act; e.g., because
    *   it is not hosted on the participant or because its write access to
    *   the ledger has been deactivated.
    *
    */
  final case class SubmitterCannotActViaParticipant(details: String) extends RejectionReason {
    override def description: String = "Submitter cannot act via participant: " + details
  }
}
