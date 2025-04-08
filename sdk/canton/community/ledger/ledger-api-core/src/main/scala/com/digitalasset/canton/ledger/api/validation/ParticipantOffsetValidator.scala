// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.logging.ContextualizedErrorLogger
import io.grpc.StatusRuntimeException

object ParticipantOffsetValidator {

  def validateOptionalPositive(offsetO: Option[Long], fieldName: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[Offset]] =
    offsetO match {
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
      offset: Long,
      fieldName: String,
      errorMsg: String = "the offset has to be a positive integer (>0)",
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Offset] =
    Either.cond(
      offset > 0,
      Offset.tryFromLong(offset),
      RequestValidationErrors.NonPositiveOffset
        .Error(
          fieldName,
          offset,
          errorMsg,
        )
        .asGrpcError,
    )

  def validateNonNegative(offset: Long, fieldName: String)(implicit
      errorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[Offset]] =
    Either.cond(
      offset >= 0,
      Offset.tryOffsetOrParticipantBegin(offset),
      RequestValidationErrors.NegativeOffset
        .Error(
          fieldName = fieldName,
          offsetValue = offset,
          message = s"the offset in $fieldName field has to be a non-negative integer (>=0)",
        )
        .asGrpcError,
    )

  def offsetIsBeforeEnd(
      offsetType: String,
      offset: Option[Offset],
      ledgerEnd: Option[Offset],
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    Either.cond(
      offset <= ledgerEnd,
      (),
      RequestValidationErrors.OffsetAfterLedgerEnd
        .Reject(
          offsetType,
          offset.fold(0L)(_.unwrap),
          ledgerEnd.fold(0L)(_.unwrap),
        )
        .asGrpcError,
    )
}
