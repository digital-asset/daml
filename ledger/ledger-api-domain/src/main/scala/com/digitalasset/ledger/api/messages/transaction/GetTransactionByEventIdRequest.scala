// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import brave.propagation.TraceContext
import com.daml.lf.data.Ref.Party
import com.daml.ledger.api.domain.{EventId, LedgerId}

final case class GetTransactionByEventIdRequest(
    ledgerId: LedgerId,
    eventId: EventId,
    requestingParties: Set[Party],
    traceContext: Option[TraceContext])
