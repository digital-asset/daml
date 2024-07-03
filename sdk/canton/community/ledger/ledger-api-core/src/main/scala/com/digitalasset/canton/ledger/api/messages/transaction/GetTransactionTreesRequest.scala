// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.transaction

import com.daml.lf.data.Ref.Party
import com.digitalasset.canton.ledger.api.domain.{LedgerId, LedgerOffset}

final case class GetTransactionTreesRequest(
    ledgerId: Option[LedgerId],
    startExclusive: LedgerOffset,
    endInclusive: Option[LedgerOffset],
    parties: Set[Party],
    sendPrunedOffsets: Boolean,
    verbose: Boolean,
)
