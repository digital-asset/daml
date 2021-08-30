// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import com.daml.ledger.api.domain
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.{v1, v2}
import io.grpc.Status
import com.google.rpc.status.{Status => RpcStatus}

sealed trait Rejection {
  val reason: String

  val code: Status.Code

  def description: String

  def toDomainRejectionReason: domain.RejectionReason

  def toStateV1RejectionReason: v1.RejectionReason

  def toStateV2RejectionReason: v2.Update.CommandRejected.RejectionReasonTemplate =
    v2.Update.CommandRejected.FinalReason(
      RpcStatus.of(code.value(), description, Seq.empty)
    )
}

object Rejection {
  object NoLedgerConfiguration extends Rejection {
    override val reason: String = "NO_LEDGER_CONFIGURATION"

    override val description: String =
      "No ledger configuration available, cannot validate ledger time"

    override val code: Status.Code = Status.Code.ABORTED

    override lazy val toDomainRejectionReason: domain.RejectionReason =
      domain.RejectionReason.InvalidLedgerTime(description)

    override lazy val toStateV1RejectionReason: v1.RejectionReason =
      v1.RejectionReasonV0.InvalidLedgerTime(description)
  }

  final case class InvalidLedgerTime(outOfRange: LedgerTimeModel.OutOfRange) extends Rejection {
    override val reason: String = "INVALID_LEDGER_TIME"

    override val code: Status.Code = Status.Code.ABORTED

    override lazy val description: String = outOfRange.message

    override lazy val toDomainRejectionReason: domain.RejectionReason =
      domain.RejectionReason.InvalidLedgerTime(description)

    override lazy val toStateV1RejectionReason: v1.RejectionReason =
      v1.RejectionReasonV0.InvalidLedgerTime(description)
  }
}
