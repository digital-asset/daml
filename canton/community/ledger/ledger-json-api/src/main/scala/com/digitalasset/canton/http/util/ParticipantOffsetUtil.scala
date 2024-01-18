// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.util

import com.daml.ledger.api.v2.participant_offset.ParticipantOffset

object ParticipantOffsetUtil {
  implicit val AbsoluteOffsetOrdering: Ordering[ParticipantOffset.Value.Absolute] =
    Ordering.by(_.value)
}
