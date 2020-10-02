// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.messages.transaction

import brave.propagation.TraceContext
import com.daml.ledger.api.domain.LedgerId

final case class GetLedgerEndRequest(ledgerId: LedgerId, traceContext: Option[TraceContext])
