// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.services.commands.tracker

import java.time.Instant

import com.digitalasset.ledger.api.v1.trace_context.TraceContext

private[tracker] final case class TrackingData[Context](
    commandId: String,
    commandTimeout: Instant,
    traceContext: Option[TraceContext],
    context: Context)
