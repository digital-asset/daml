// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.digitalasset.canton.data.Offset

// (startExclusive, endInclusive]
final case class EventsRange(
    startExclusiveOffset: Offset,
    startExclusiveEventSeqId: Long,
    endInclusiveOffset: Offset,
    endInclusiveEventSeqId: Long,
)
