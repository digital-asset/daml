package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}

sealed abstract class ConfigurationEntry extends Product with Serializable

object ConfigurationEntry {

  final case class Accepted(
      submissionId: String,
      participantId: ParticipantId,
      configuration: Configuration,
  ) extends ConfigurationEntry

  final case class Rejected(
      submissionId: String,
      participantId: ParticipantId,
      rejectionReason: String,
      proposedConfiguration: Configuration
  ) extends ConfigurationEntry

}
