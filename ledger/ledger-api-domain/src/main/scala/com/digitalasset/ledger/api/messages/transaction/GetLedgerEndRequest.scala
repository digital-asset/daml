// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.messages.transaction

import brave.propagation.TraceContext
import com.digitalasset.ledger.api.domain.LedgerId

final case class GetLedgerEndRequest(ledgerId: LedgerId, traceContext: Option[TraceContext])
