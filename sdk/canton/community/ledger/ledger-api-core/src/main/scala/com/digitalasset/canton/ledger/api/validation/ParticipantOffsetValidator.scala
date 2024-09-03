// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.error.ContextualizedErrorLogger
import com.digitalasset.canton.ledger.api.domain.types.ParticipantOffset
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.daml.lf.data.Ref
import io.grpc.StatusRuntimeException

import scala.math.Ordered.*

object ParticipantOffsetValidator {

  import ValidationErrors.invalidArgument

  def validate(ledgerOffset: String)(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, ParticipantOffset] =
    Ref.HexString.fromString(ledgerOffset).left.map(invalidArgument)

  def offsetIsBeforeEnd(
      offsetType: String,
      ledgerOffset: ParticipantOffset,
      ledgerEnd: ParticipantOffset,
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

  // Same as above, but with an optional offset.
  def offsetIsBeforeEnd(
      offsetType: String,
      ledgerOffset: Option[ParticipantOffset],
      ledgerEnd: ParticipantOffset,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[StatusRuntimeException, Unit] =
    ledgerOffset.fold[Either[StatusRuntimeException, Unit]](Right(()))(
      offsetIsBeforeEnd(offsetType, _, ledgerEnd)
    )
}
