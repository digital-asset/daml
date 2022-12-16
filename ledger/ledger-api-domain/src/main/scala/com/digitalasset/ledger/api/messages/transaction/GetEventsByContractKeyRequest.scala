// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import com.daml.lf.data.Ref.Party
import com.daml.ledger.api.domain.LedgerOffset
import com.daml.lf.data.Ref
import com.daml.lf.value.Value

case class GetEventsByContractKeyRequest(
    contractKey: Value,
    templateId: Ref.Identifier,
    requestingParties: Set[Party],
    maxEvents: Int,
    startExclusive: LedgerOffset,
    endInclusive: LedgerOffset,
)
