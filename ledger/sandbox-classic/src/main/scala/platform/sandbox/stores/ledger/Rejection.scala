// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import com.daml.ledger.api.domain
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.{v2 => state}
import com.google.protobuf.any.{Any => AnyProto}
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.{Status => RpcStatus}
import io.grpc.Status

sealed trait Rejection {
  def toDomainRejectionReason: domain.RejectionReason

  def toStateRejectionReason: state.Update.CommandRejected.RejectionReasonTemplate
}

object Rejection {
  val ErrorDomain = "com.daml.on.sql"

  object NoLedgerConfiguration extends Rejection {
    private val description: String =
      "No ledger configuration available, cannot validate ledger time"

    override lazy val toDomainRejectionReason: domain.RejectionReason =
      domain.RejectionReason.InvalidLedgerTime(description)

    override lazy val toStateRejectionReason: state.Update.CommandRejected.RejectionReasonTemplate =
      // TODO error codes: Adapt V2 ?
      state.Update.CommandRejected.FinalReason(
        RpcStatus.of(
          code = Status.Code.ABORTED.value(),
          message = description,
          details = Seq(
            AnyProto.pack(
              ErrorInfo.of(
                reason = "NO_LEDGER_CONFIGURATION",
                domain = ErrorDomain,
                metadata = Map.empty,
              )
            )
          ),
        )
      )
  }

  final case class InvalidLedgerTime(outOfRange: LedgerTimeModel.OutOfRange) extends Rejection {
    override lazy val toDomainRejectionReason: domain.RejectionReason =
      domain.RejectionReason.InvalidLedgerTime(outOfRange.message)

    override lazy val toStateRejectionReason: state.Update.CommandRejected.RejectionReasonTemplate =
      state.Update.CommandRejected.FinalReason(
        RpcStatus.of(
          code = Status.Code.ABORTED.value(),
          message = outOfRange.message,
          details = Seq(
            AnyProto.pack(
              ErrorInfo.of(
                reason = "INVALID_LEDGER_TIME",
                domain = ErrorDomain,
                metadata = Map(
                  "ledgerTime" -> outOfRange.ledgerTime.toString,
                  "lowerBound" -> outOfRange.lowerBound.toString,
                  "upperBound" -> outOfRange.upperBound.toString,
                ),
              )
            )
          ),
        )
      )
  }
}
