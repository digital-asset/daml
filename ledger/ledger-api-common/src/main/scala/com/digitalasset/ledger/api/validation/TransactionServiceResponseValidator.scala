// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.validation

import com.daml.ledger.api.domain.{LedgerId, LedgerOffset}
import com.daml.lf.data.Ref.LedgerString

import scala.concurrent.{ExecutionContext, Future}

class TransactionServiceResponseValidator(
    ledgerId: LedgerId,
    fetchLedgerEnd: String => Future[LedgerOffset.Absolute],
)(implicit ec: ExecutionContext) {
  def validate[Response](
      response: Response,
      offsetFromResponse: Response => LedgerString,
      offsetOrdering: Ordering[LedgerOffset.Absolute],
  ): Future[Response] =
    for {
      ledgerEnd <- fetchLedgerEnd(ledgerId.toString)
      offset = offsetFromResponse(response)
      ledgerOffset = LedgerOffset.Absolute(offset)
      validatedResponse <- LedgerOffsetValidator
        .offsetIsBeforeEndIfAbsolute(
          offsetType = "End",
          ledgerOffset = ledgerOffset,
          ledgerEnd = ledgerEnd,
          offsetOrdering = offsetOrdering,
        )
        .fold(Future.failed, _ => Future.successful(response))
    } yield validatedResponse
}
