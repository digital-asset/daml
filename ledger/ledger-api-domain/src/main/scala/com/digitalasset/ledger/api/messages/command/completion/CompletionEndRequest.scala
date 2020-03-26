// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.api.messages.command.completion

import brave.propagation.TraceContext
import com.digitalasset.ledger.api.domain.LedgerId

case class CompletionEndRequest(ledgerId: LedgerId, traceContext: Option[TraceContext])
