// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import com.daml.ledger.api.domain.{LedgerId, TransactionId}
import com.daml.lf.data.Ref.Party

final case class GetTransactionByIdRequest(
    ledgerId: Option[LedgerId],
    transactionId: TransactionId,
    requestingParties: Set[Party],
)
