// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.update

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.TransactionFormat
import com.digitalasset.daml.lf.data.Ref.Party

final case class GetTransactionByOffsetRequest(
    offset: Offset,
    transactionFormat: TransactionFormat,
)

// TODO(#23504) cleanup
final case class GetTransactionByOffsetRequestForTrees(
    offset: Offset,
    requestingParties: Set[Party],
)
