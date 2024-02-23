// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.transaction

import com.digitalasset.canton.ledger.api.domain.{ParticipantOffset, TransactionFilter}

final case class GetTransactionsRequest(
    startExclusive: ParticipantOffset,
    endInclusive: Option[ParticipantOffset],
    filter: TransactionFilter,
    verbose: Boolean,
)
