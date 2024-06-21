// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.transaction

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.canton.ledger.api.domain.TransactionId

final case class GetTransactionByIdRequest(
    transactionId: TransactionId,
    requestingParties: Set[Party],
)
