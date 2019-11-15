package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.v1.ParticipantId

sealed abstract class PackageUploadEntry extends Product with Serializable

object PackageUploadEntry {
  final case class Accepted(
      submissionId: String,
      participantId: ParticipantId
  ) extends PackageUploadEntry
  final case class Rejected(
      submissionId: String,
      participantId: ParticipantId,
      reason: String
  ) extends PackageUploadEntry
}
