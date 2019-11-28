// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, SubmissionId}

sealed abstract class ConfigurationEntry extends Product with Serializable

object ConfigurationEntry {

  final case class Accepted(
      submissionId: SubmissionId,
      participantId: ParticipantId,
      configuration: Configuration,
  ) extends ConfigurationEntry

  final case class Rejected(
      submissionId: SubmissionId,
      participantId: ParticipantId,
      rejectionReason: String,
      proposedConfiguration: Configuration
  ) extends ConfigurationEntry

}
