// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.messages.transaction

import brave.propagation.TraceContext
import com.digitalasset.daml.lf.data.Ref.{LedgerId, Party}
import com.digitalasset.ledger.api.domain.EventId

final case class GetTransactionByEventIdRequest(
    ledgerId: LedgerId,
    eventId: EventId,
    requestingParties: Set[Party],
    traceContext: Option[TraceContext])
