// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.services.commands.tracker

import java.time.Instant

private[tracker] final case class TrackingData[Context](
    commandId: String,
    commandTimeout: Instant,
    context: Context,
)
