// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset.ParticipantBoundary
import com.digitalasset.canton.ledger.api.domain
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import io.grpc.StatusRuntimeException

import scala.math.Ordered.*

object ParticipantOffsetValidator {

  private val boundary = "boundary"

  import FieldValidator.requireLedgerString
  import ValidationErrors.{invalidArgument, missingField}

  def validateOptional(
      ledgerOffset: Option[ParticipantOffset],
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Option[domain.ParticipantOffset]] =
    ledgerOffset
      .map(validate(_, fieldName))
      .fold[Either[StatusRuntimeException, Option[domain.ParticipantOffset]]](Right(None))(
        _.map(Some(_))
      )

  def validate(
      ledgerOffset: ParticipantOffset,
      fieldName: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.ParticipantOffset] = {
    ledgerOffset.value match {
      case ParticipantOffset.Value.Absolute(value) =>
        requireLedgerString(value, fieldName).map(domain.ParticipantOffset.Absolute)
      case ParticipantOffset.Value.Boundary(value) =>
        convertLedgerBoundary(fieldName, value)
      case ParticipantOffset.Value.Empty =>
        Left(missingField(fieldName + ".(" + boundary + "|value)"))
    }
  }

  def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: domain.ParticipantOffset,
      ledgerEnd: domain.ParticipantOffset.Absolute,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    ledgerOffset match {
      case abs: domain.ParticipantOffset.Absolute if abs > ledgerEnd =>
        Left(
          RequestValidationErrors.OffsetAfterLedgerEnd
            .Reject(offsetType, abs.value, ledgerEnd.value)
            .asGrpcError
        )
      case _ => Right(())
    }

  // Same as above, but with an optional offset.
  def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: Option[domain.ParticipantOffset],
      ledgerEnd: domain.ParticipantOffset.Absolute,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    ledgerOffset.fold[Either[StatusRuntimeException, Unit]](Right(()))(
      offsetIsBeforeEndIfAbsolute(offsetType, _, ledgerEnd)
    )

  private def convertLedgerBoundary(
      fieldName: String,
      value: ParticipantBoundary,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, domain.ParticipantOffset] = {
    value match {
      case ParticipantBoundary.Unrecognized(invalid) =>
        Left(
          invalidArgument(
            s"Unknown ledger $boundary value '$invalid' in field $fieldName.$boundary"
          )
        )
      case ParticipantBoundary.PARTICIPANT_BEGIN => Right(domain.ParticipantOffset.ParticipantBegin)
      case ParticipantBoundary.PARTICIPANT_END => Right(domain.ParticipantOffset.ParticipantEnd)
    }
  }
}
