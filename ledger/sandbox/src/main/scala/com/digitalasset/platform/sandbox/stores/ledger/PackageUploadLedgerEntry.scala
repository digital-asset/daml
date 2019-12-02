// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import com.daml.ledger.participant.state.v1.ParticipantId

//TODO BH consider removing duplication of this class
sealed abstract class PackageUploadLedgerEntry() extends Product with Serializable {
  val submissionId: String
  val participantId: ParticipantId
  val recordTime: Instant
  def value: String
}

object PackageUploadLedgerEntry {
  final case class Accepted(
      override val submissionId: String,
      override val participantId: ParticipantId,
      override val recordTime: Instant
  ) extends PackageUploadLedgerEntry {
    override def value = "accept"
  }
  final case class Rejected(
      override val submissionId: String,
      override val participantId: ParticipantId,
      override val recordTime: Instant,
      reason: String
  ) extends PackageUploadLedgerEntry {
    override def value = "reject"
  }
}
