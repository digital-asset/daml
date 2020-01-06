// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.digitalasset.ledger.api.domain

sealed abstract class ConfigurationEntry extends Product with Serializable {
  def toDomain: domain.ConfigurationEntry
}

object ConfigurationEntry {

  final case class Accepted(
      submissionId: String,
      participantId: ParticipantId,
      configuration: Configuration,
  ) extends ConfigurationEntry {
    override def toDomain: domain.ConfigurationEntry =
      domain.ConfigurationEntry.Accepted(
        submissionId,
        domain.ParticipantId(participantId),
        configuration
      )
  }

  final case class Rejected(
      submissionId: String,
      participantId: ParticipantId,
      rejectionReason: String,
      proposedConfiguration: Configuration
  ) extends ConfigurationEntry {
    override def toDomain: domain.ConfigurationEntry =
      domain.ConfigurationEntry.Rejected(
        submissionId,
        domain.ParticipantId(participantId),
        rejectionReason,
        proposedConfiguration
      )
  }

}
