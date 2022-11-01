// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary
import com.daml.platform.error.definitions.LedgerApiErrors
import com.daml.platform.server.api.validation.FieldValidations
import io.grpc.StatusRuntimeException

import scala.math.Ordered._

object LedgerOffsetValidator {

  private val boundary = "boundary"

  import ValidationErrors.{invalidArgument, missingField}
  import FieldValidations.requireLedgerString

  def validateOptional(
      ledgerOffset: Option[LedgerOffset],
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[domain.LedgerOffset]] =
    ledgerOffset
      .map(validate(_, fieldName))
      .fold[Either[StatusRuntimeException, Option[domain.LedgerOffset]]](Right(None))(
        _.map(Some(_))
      )

  def validate(
      ledgerOffset: LedgerOffset,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.LedgerOffset] = {
    ledgerOffset.value match {
      case LedgerOffset.Value.Absolute(value) =>
        requireLedgerString(value, fieldName).map(domain.LedgerOffset.Absolute)
      case LedgerOffset.Value.Boundary(value) =>
        convertLedgerBoundary(fieldName, value)
      case LedgerOffset.Value.Empty =>
        Left(missingField(fieldName + ".(" + boundary + "|value)"))
    }
  }

  def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: domain.LedgerOffset,
      ledgerEnd: domain.LedgerOffset.Absolute,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    ledgerOffset match {
      case abs: domain.LedgerOffset.Absolute if abs > ledgerEnd =>
        Left(
          LedgerApiErrors.RequestValidation.OffsetAfterLedgerEnd
            .Reject(offsetType, abs.value, ledgerEnd.value)
            .asGrpcError
        )
      case _ => Right(())
    }

  // Same as above, but with an optional offset.
  def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: Option[domain.LedgerOffset],
      ledgerEnd: domain.LedgerOffset.Absolute,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    ledgerOffset.fold[Either[StatusRuntimeException, Unit]](Right(()))(
      offsetIsBeforeEndIfAbsolute(offsetType, _, ledgerEnd)
    )

  private def convertLedgerBoundary(
      fieldName: String,
      value: LedgerBoundary,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.LedgerOffset] = {
    value match {
      case LedgerBoundary.Unrecognized(invalid) =>
        Left(
          invalidArgument(
            s"Unknown ledger $boundary value '$invalid' in field $fieldName.$boundary"
          )
        )
      case LedgerBoundary.LEDGER_BEGIN => Right(domain.LedgerOffset.LedgerBegin)
      case LedgerBoundary.LEDGER_END => Right(domain.LedgerOffset.LedgerEnd)
    }
  }
}
