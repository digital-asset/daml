// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores.ledger

import com.daml.error.{ContextualizedErrorLogger, ErrorCodesVersionSwitcher}
import com.daml.ledger.api.domain
import com.daml.ledger.configuration.LedgerTimeModel
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.platform.server.api.validation.ErrorFactories
import com.google.protobuf.any.{Any => AnyProto}
import com.google.rpc.error_details.ErrorInfo
import com.google.rpc.status.{Status => RpcStatus}
import io.grpc.Status

sealed trait Rejection {
  def toDomainRejectionReason(
      errorCodesVersionSwitcher: ErrorCodesVersionSwitcher
  ): domain.RejectionReason

  def toStateRejectionReason(errorFactories: ErrorFactories)(implicit
      errorLogger: ContextualizedErrorLogger
  ): state.Update.CommandRejected.RejectionReasonTemplate
}

object Rejection {
  val ErrorDomain = "com.daml.on.sql"

  object NoLedgerConfiguration extends Rejection {
    private val description: String =
      "No ledger configuration available, cannot validate ledger time"

    override def toDomainRejectionReason(
        errorCodesVersionSwitcher: ErrorCodesVersionSwitcher
    ): domain.RejectionReason =
      errorCodesVersionSwitcher.choose[domain.RejectionReason](
        // The V1 rejection reason is not precise enough but changing it in-place involves breaking compatibility.
        // Instead use the error codes version switcher to correct the rejection from now on
        v1 = domain.RejectionReason.InvalidLedgerTime(description),
        v2 = domain.RejectionReason.LedgerConfigNotFound(description),
      )

    override def toStateRejectionReason(errorFactories: ErrorFactories)(implicit
        errorLogger: ContextualizedErrorLogger
    ): state.Update.CommandRejected.RejectionReasonTemplate =
      state.Update.CommandRejected.FinalReason(
        errorFactories.missingLedgerConfig(
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
          ),
          "Cannot validate ledger time",
        )
      )
  }

  final case class InvalidLedgerTime(outOfRange: LedgerTimeModel.OutOfRange) extends Rejection {
    override def toDomainRejectionReason(
        errorCodesVersionSwitcher: ErrorCodesVersionSwitcher
    ): domain.RejectionReason =
      domain.RejectionReason.InvalidLedgerTime(outOfRange.message)

    override def toStateRejectionReason(errorFactories: ErrorFactories)(implicit
        errorLogger: ContextualizedErrorLogger
    ): state.Update.CommandRejected.RejectionReasonTemplate =
      state.Update.CommandRejected.FinalReason(
        errorFactories.CommandRejections.invalidLedgerTime(
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
          ),
          outOfRange.ledgerTime.toInstant,
          outOfRange.lowerBound.toInstant,
          outOfRange.upperBound.toInstant,
        )
      )
  }
}
