// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId, SequencerId}

sealed trait SubmissionRequestType {
  def name: String
}

object SubmissionRequestType {
  case object ConfirmationResponse extends SubmissionRequestType {
    override val name: String = "confirmation response"
  }
  case object ConfirmationRequest extends SubmissionRequestType {
    override def name: String = "confirmation request"
  }
  case object Verdict extends SubmissionRequestType {
    override def name: String = "verdict"
  }
  case object Commitment extends SubmissionRequestType {
    override def name: String = "commitment"
  }
  case object TopUp extends SubmissionRequestType {
    override def name: String = "top up"
  }
  case object TopUpMed extends SubmissionRequestType {
    override def name: String = "top up med"
  }
  case object TopologyTransaction extends SubmissionRequestType {
    override def name: String = "topology transaction"
  }
  case object TimeProof extends SubmissionRequestType {
    override def name: String = "time proof"
  }
  final case class Unexpected(description: String) extends SubmissionRequestType {
    override def name: String = "unexpected"
  }

  def submissionRequestType(allRecipients: Set[Recipient], member: Member): SubmissionRequestType =
    allRecipients
      .foldLeft(RecipientStats()) {
        case (acc, MemberRecipient(ParticipantId(_))) =>
          acc.copy(participants = true)
        case (acc, MemberRecipient(MediatorId(_)) | MediatorGroupRecipient(_)) =>
          acc.copy(mediators = true)
        case (acc, MemberRecipient(SequencerId(_)) | SequencersOfSynchronizer) =>
          acc.copy(sequencers = true)
        case (acc, AllMembersOfSynchronizer) => acc.copy(broadcast = true)
      }
      .submissionRequestType(member)

  private final case class RecipientStats(
      participants: Boolean = false,
      mediators: Boolean = false,
      sequencers: Boolean = false,
      broadcast: Boolean = false,
  ) {
    def submissionRequestType(sender: Member): SubmissionRequestType =
      // by looking at the recipient lists and the sender, we'll figure out what type of message we've been getting
      (sender, participants, mediators, sequencers, broadcast) match {
        case (ParticipantId(_), false, true, false, false) => ConfirmationResponse
        case (ParticipantId(_), true, true, false, false) => ConfirmationRequest
        case (MediatorId(_), true, false, false, false) => Verdict
        case (ParticipantId(_), true, false, false, false) => Commitment
        case (SequencerId(_), true, false, true, false) => TopUp
        case (SequencerId(_), false, true, true, false) => TopUpMed
        case (_, false, false, false, true) => TopologyTransaction
        case (_, false, false, false, false) => TimeProof
        case _ =>
          def r(boolean: Boolean, s: String) = if (boolean) Seq(s) else Seq.empty
          val recipients = r(participants, "participants") ++
            r(mediators, "mediators") ++
            r(sequencers, "sequencers") ++
            r(broadcast, "broadcast")
          Unexpected(s"Unexpected message from $sender to " + recipients.mkString(","))
      }
  }

}
