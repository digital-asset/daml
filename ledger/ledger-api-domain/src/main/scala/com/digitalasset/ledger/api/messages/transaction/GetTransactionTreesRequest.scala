// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import com.daml.ledger.api.domain.{LedgerId, LedgerOffset}
import com.daml.lf.data.Ref.Party

final case class GetTransactionTreesRequest(
    ledgerId: Option[LedgerId],
    startExclusive: LedgerOffset,
    endInclusive: Option[LedgerOffset],
    parties: Set[Party],
    verbose: Boolean,
)
