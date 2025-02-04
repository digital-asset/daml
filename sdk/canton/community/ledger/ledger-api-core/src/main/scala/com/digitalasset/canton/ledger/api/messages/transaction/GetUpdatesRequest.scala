// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.transaction

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.EventFormat

final case class GetUpdatesRequest(
    startExclusive: Option[Offset],
    endInclusive: Option[Offset],
    eventFormat: EventFormat,
)
