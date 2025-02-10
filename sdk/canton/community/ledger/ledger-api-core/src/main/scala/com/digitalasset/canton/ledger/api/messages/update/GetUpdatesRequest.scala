// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.messages.update

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.{EventFormat, UpdateFormat}

final case class GetUpdatesRequest(
    startExclusive: Option[Offset],
    endInclusive: Option[Offset],
    updateFormat: UpdateFormat,
)

// TODO(#23504) cleanup
final case class GetUpdatesRequestForTrees(
    startExclusive: Option[Offset],
    endInclusive: Option[Offset],
    eventFormat: EventFormat,
)
