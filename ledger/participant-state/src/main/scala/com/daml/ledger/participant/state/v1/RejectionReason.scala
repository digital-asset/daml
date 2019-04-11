// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.v1

// Kinds of rejections
sealed trait RejectionReason {
  def description: String
}

object RejectionReason {

  /** The transaction relied on contracts being active that were no longer
    * active at the point where it was sequenced.
    */
  final case object Inconsistent extends RejectionReason {
    override def description: String = "Inconsistent"
  }

  /** The Participant node did not have sufficient resources with the
    * to submit the transaction.
    */
  final case object ResourcesExhausted extends RejectionReason {
    override def description: String = "Resources exhausted"
  }

  /** The transaction submission timed out.
    *
    * This means the 'maximumRecordTime' was smaller than the recordTime seen
    * in an event in the Participant node.
    */
  final case object MaximumRecordTimeExceeded extends RejectionReason {
    override def description: String =
      "The maximum record time of the command exceeded"
  }

  /** The transaction submission was disputed.
    *
    * This means that the underlying ledger and its validation logic
    * considered the transaction potentially invalid. This can be due to a bug
    * in the submission or validiation logic, or due to malicious behaviour.
    */
  final case class Disputed(reason: String) extends RejectionReason {
    override def description: String = "Disputed: " + reason
  }

  /** The participant node has already seen a command with the same commandId
    * during its implementation specific deduplication window.
    *
    * TODO (SM): explain in more detail how command de-duplication should
    * work.
    */
  final case object DuplicateCommandId extends RejectionReason {
    override def description: String = "Duplicate command"
  }

  final case object PartyNotKnownOnLedger extends RejectionReason {
    override def description: String = "Party not known on ledger"
  }

  final case object SubmitterNotHostedOnParticipant extends RejectionReason {
    override def description: String = "Submitter not hosted on participant"
  }
}
