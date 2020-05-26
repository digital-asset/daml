// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.util

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset

object LedgerOffsetUtil {
  implicit val AbsoluteOffsetOrdering: Ordering[LedgerOffset.Value.Absolute] = Ordering.by(_.value)
}
