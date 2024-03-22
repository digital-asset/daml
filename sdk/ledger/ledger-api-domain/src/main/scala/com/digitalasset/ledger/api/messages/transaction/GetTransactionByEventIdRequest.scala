// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import com.daml.lf.data.Ref.Party
import com.daml.ledger.api.domain.{EventId, LedgerId}

final case class GetTransactionByEventIdRequest(
    ledgerId: Option[LedgerId],
    eventId: EventId,
    requestingParties: Set[Party],
)
