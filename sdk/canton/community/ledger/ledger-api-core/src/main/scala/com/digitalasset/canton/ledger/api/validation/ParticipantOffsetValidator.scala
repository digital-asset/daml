// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.daml.lf.data.Ref
import io.grpc.StatusRuntimeException

import scala.math.Ordered.*

object ParticipantOffsetValidator {

  private val boundary = "boundary"

  import ValidationErrors.invalidArgument

  def validate(ledgerOffset: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, String] =
    if (ledgerOffset.isEmpty)
      Right(ledgerOffset)
    else
      Ref.LedgerString.fromString(ledgerOffset).left.map(invalidArgument)

  // Same as above, but with an optional offset.
  def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: Option[String],
      ledgerEnd: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    ledgerOffset.fold[Either[StatusRuntimeException, Unit]](Right(()))(
      offsetIsBeforeEndIfAbsolute(offsetType, _, ledgerEnd)
    )

  def offsetIsBeforeEndIfAbsolute(
      offsetType: String,
      ledgerOffset: String,
      ledgerEnd: String,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    if (ledgerOffset > ledgerEnd)
      Left(
        RequestValidationErrors.OffsetAfterLedgerEnd
          .Reject(offsetType, ledgerOffset, ledgerEnd)
          .asGrpcError
      )
    else Right(())
}
