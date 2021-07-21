// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import com.daml.ledger.api.domain
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.v1
import com.daml.platform.store.Conversions.RejectionReasonOps
import io.grpc.Status

sealed trait Rejection {
  val reason: String

  val code: Status.Code

  def description: String

  val definiteAnswer: Boolean

  def toDomainRejectionReason: domain.RejectionReason

  def toStateV1RejectionReason: v1.RejectionReason =
    toDomainRejectionReason.toParticipantStateRejectionReason
}

object Rejection {
  val rejectionDomain = "com.daml.ledger"

  object NoLedgerConfiguration extends Rejection {
    override val reason: String = "NO_LEDGER_CONFIGURATION"

    override val description: String =
      "No ledger configuration available, cannot validate ledger time"

    override val code: Status.Code = Status.Code.ABORTED

    override val definiteAnswer: Boolean = false

    override lazy val toDomainRejectionReason: domain.RejectionReason =
      domain.RejectionReason.InvalidLedgerTime(description)
  }

  final case class InvalidLedgerTime(outOfRange: LedgerTimeModel.OutOfRange) extends Rejection {
    override val reason: String = "INVALID_LEDGER_TIME"

    override val code: Status.Code = Status.Code.ABORTED

    override lazy val description: String = outOfRange.message

    override val definiteAnswer: Boolean = true

    override lazy val toDomainRejectionReason: domain.RejectionReason =
      domain.RejectionReason.InvalidLedgerTime(description)
  }
}
