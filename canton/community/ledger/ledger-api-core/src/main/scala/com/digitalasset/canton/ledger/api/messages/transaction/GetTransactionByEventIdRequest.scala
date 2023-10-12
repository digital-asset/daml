// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.transaction

import com.daml.lf.data.Ref.Party
import com.digitalasset.canton.ledger.api.domain.{EventId, LedgerId}

final case class GetTransactionByEventIdRequest(
    ledgerId: Option[LedgerId],
    eventId: EventId,
    requestingParties: Set[Party],
)
