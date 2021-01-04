// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.ledger.api.domain
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset.LedgerBoundary
import com.daml.platform.server.api.validation.ErrorFactories.{
  invalidArgument,
  missingField,
  outOfRange
}
import com.daml.platform.server.api.validation.FieldValidations.requireLedgerString
import io.grpc.StatusRuntimeException

object LedgerOffsetValidator {

  private val boundary = "boundary"

  def validateOptional(
      ledgerOffset: Option[LedgerOffset],
      fieldName: String): Either[StatusRuntimeException, Option[domain.LedgerOffset]] =
    ledgerOffset
      .map(validate(_, fieldName))
      .fold[Either[StatusRuntimeException, Option[domain.LedgerOffset]]](Right(None))(
        _.map(Some(_)))

  def validate(
      ledgerOffset: LedgerOffset,
      fieldName: String): Either[StatusRuntimeException, domain.LedgerOffset] = {
    ledgerOffset match {
      case LedgerOffset(LedgerOffset.Value.Absolute(value)) =>
        requireLedgerString(value, fieldName).map(domain.LedgerOffset.Absolute)
      case LedgerOffset(LedgerOffset.Value.Boundary(value)) =>
        convertLedgerBoundary(fieldName, value)
      case LedgerOffset(LedgerOffset.Value.Empty) =>
        Left(missingField(fieldName + ".(" + boundary + "|value)"))
    }
  }

  def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: domain.LedgerOffset,
      ledgerEnd: domain.LedgerOffset.Absolute,
      offsetOrdering: Ordering[domain.LedgerOffset.Absolute])
    : Either[StatusRuntimeException, Unit] =
    ledgerOffset match {
      case abs: domain.LedgerOffset.Absolute if offsetOrdering.gt(abs, ledgerEnd) =>
        Left(outOfRange(s"$offsetType offset ${abs.value} is after ledger end ${ledgerEnd.value}"))
      case _ => Right(())
    }

  // Same as above, but with an optional offset.
  def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: Option[domain.LedgerOffset],
      ledgerEnd: domain.LedgerOffset.Absolute,
      offsetOrdering: Ordering[domain.LedgerOffset.Absolute])
    : Either[StatusRuntimeException, Unit] =
    ledgerOffset.fold[Either[StatusRuntimeException, Unit]](Right(()))(
      offsetIsBeforeEndIfAbsolute(offsetType, _, ledgerEnd, offsetOrdering))

  private def convertLedgerBoundary(
      fieldName: String,
      value: LedgerBoundary): Either[StatusRuntimeException, domain.LedgerOffset] = {
    value match {
      case LedgerBoundary.Unrecognized(invalid) =>
        Left(
          invalidArgument(
            s"Unknown ledger $boundary value '$invalid' in field $fieldName.$boundary"))
      case LedgerBoundary.LEDGER_BEGIN => Right(domain.LedgerOffset.LedgerBegin)
      case LedgerBoundary.LEDGER_END => Right(domain.LedgerOffset.LedgerEnd)
    }
  }
}
