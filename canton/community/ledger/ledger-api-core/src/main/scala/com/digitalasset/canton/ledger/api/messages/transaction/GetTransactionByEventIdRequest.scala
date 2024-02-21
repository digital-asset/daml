// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.transaction

import com.daml.lf.data.Ref.Party
import com.digitalasset.canton.ledger.api.domain.EventId

final case class GetTransactionByEventIdRequest(
    eventId: EventId,
    requestingParties: Set[Party],
)
