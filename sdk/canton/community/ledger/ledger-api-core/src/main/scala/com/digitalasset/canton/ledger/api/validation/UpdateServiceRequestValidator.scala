// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.update_service.{
  GetUpdateByIdRequest,
  GetUpdateByOffsetRequest,
  GetUpdatesRequest,
}
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.messages.update
import com.digitalasset.canton.ledger.api.validation.ValidationErrors.invalidArgument
import com.digitalasset.canton.ledger.api.validation.ValueValidator.*
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.UpdateId
import io.grpc.StatusRuntimeException

object UpdateServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]

  import FieldValidator.*

  final case class PartialValidation(
      begin: Option[Offset],
      end: Option[Offset],
  )

  private def commonValidations(
      req: GetUpdatesRequest
  )(implicit errorLoggingContext: ErrorLoggingContext): Result[PartialValidation] =
    for {
      begin <- ParticipantOffsetValidator
        .validateNonNegative(req.beginExclusive, "begin_exclusive")
      end <- ParticipantOffsetValidator
        .validateOptionalPositive(req.endInclusive, "end_inclusive")
    } yield PartialValidation(
      begin,
      end,
    )

  def validate(
      req: GetUpdatesRequest,
      ledgerEnd: Option[Offset],
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Result[update.GetUpdatesRequest] =
    for {
      partial <- commonValidations(req)
      _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
        "Begin",
        partial.begin,
        ledgerEnd,
      )
      _ <- ParticipantOffsetValidator.offsetIsBeforeEnd(
        "End",
        partial.end,
        ledgerEnd,
      )
      updateFormatProto <- requirePresence(req.updateFormat, "update_format")
      updateFormat <- FormatValidator.validate(updateFormatProto)
    } yield {
      update.GetUpdatesRequest(
        partial.begin,
        partial.end,
        updateFormat,
      )
    }

  def validateUpdateByOffset(
      req: GetUpdateByOffsetRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Result[update.GetUpdateByOffsetRequest] =
    for {
      offset <- ParticipantOffsetValidator.validatePositive(req.offset, "offset")
      updateFormatProto <- requirePresence(req.updateFormat, "update_format")
      updateFormat <- FormatValidator.validate(updateFormatProto)
    } yield {
      update.GetUpdateByOffsetRequest(
        offset = offset,
        updateFormat = updateFormat,
      )
    }

  def validateUpdateById(
      req: GetUpdateByIdRequest
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Result[update.GetUpdateByIdRequest] =
    for {
      _ <- requireNonEmptyString(req.updateId, "update_id")
      updateIdStr <- requireLedgerString(req.updateId)
      updateId <- UpdateId.fromLedgerString(updateIdStr).left.map(e => invalidArgument(e.message))
      updateFormatProto <- requirePresence(req.updateFormat, "update_format")
      updateFormat <- FormatValidator.validate(updateFormatProto)
    } yield {
      update.GetUpdateByIdRequest(
        updateId = updateId,
        updateFormat = updateFormat,
      )
    }

}
