// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.syntax.either.*
import com.daml.error.ContextualizedErrorLogger
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.domain.types.ParticipantOffset
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.platform.ApiOffset
import io.grpc.StatusRuntimeException

object ParticipantOffsetValidator {

  def validateOptionalPositive(ledgerOffsetO: Option[Long], fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[ParticipantOffset]] =
    ledgerOffsetO match {
      case Some(off) =>
        validatePositive(
          off,
          fieldName,
          "the offset has to be either a positive integer (>0) or not defined at all",
        ).map(
          Some(_)
        )
      case None => Right(None)
    }

  def validatePositive(
      ledgerOffset: Long,
      fieldName: String,
      errorMsg: String = "the offset has to be a positive integer (>0)",
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ParticipantOffset] =
    if (ledgerOffset <= 0)
      Left(
        RequestValidationErrors.NonPositiveOffset
          .Error(
            fieldName,
            ledgerOffset,
            errorMsg,
          )
          .asGrpcError
      )
    else
      Right(Offset.fromLong(ledgerOffset).toHexString)

  def validateNonNegative(ledgerOffset: Long, fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ParticipantOffset] =
    if (ledgerOffset < 0)
      Left(
        RequestValidationErrors.NegativeOffset
          .Error(
            fieldName,
            ledgerOffset,
            s"the offset in $fieldName field has to be a non-negative integer (>=0)",
          )
          .asGrpcError
      )
    else
      Right(Offset.fromLong(ledgerOffset).toHexString)

  def offsetIsBeforeEnd(
      offsetType: String,
      ledgerOffset: ParticipantOffset,
      ledgerEnd: ParticipantOffset,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    Either.cond(
      ledgerOffset <= ledgerEnd,
      (),
      RequestValidationErrors.OffsetAfterLedgerEnd
        .Reject(
          offsetType,
          ApiOffset.assertFromStringToLong(ledgerOffset),
          ApiOffset.assertFromStringToLong(ledgerEnd),
        )
        .asGrpcError,
    )

  // Same as above, but with an optional offset.
  def offsetIsBeforeEnd(
      offsetType: String,
      ledgerOffset: Option[ParticipantOffset],
      ledgerEnd: ParticipantOffset,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    ledgerOffset.fold(Either.unit[StatusRuntimeException])(
      offsetIsBeforeEnd(offsetType, _, ledgerEnd)
    )
}
