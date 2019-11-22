// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import com.daml.ledger.participant.state.v1.ParticipantId

sealed abstract class PackageUploadEntry extends Product with Serializable {
  def value: String
}

object PackageUploadEntry {
  final case class Accepted(
      submissionId: String,
      participantId: ParticipantId,
  ) extends PackageUploadEntry {
    override def value = "accept"
  }
  final case class Rejected(
      submissionId: String,
      participantId: ParticipantId,
      reason: String
  ) extends PackageUploadEntry {
    override def value = "reject"
  }
}
