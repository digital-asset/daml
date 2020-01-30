// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.util

import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset
import com.digitalasset.ledger.api.v1.ledger_offset.LedgerOffset.Value

object LedgerOffsetUtil {
  implicit val AbsoluteOffsetOrdering: Ordering[LedgerOffset.Value.Absolute] =
    new Ordering[LedgerOffset.Value.Absolute] {
      override def compare(x: Value.Absolute, y: Value.Absolute): Int =
        if (isCompositeOffsetFormat(x)) CompositeAbsoluteOffsetIntOrdering.compare(x, y)
        else IntAbsoluteOffsetOrdering.compare(x, y)
    }

  private val IntAbsoluteOffsetOrdering: Ordering[LedgerOffset.Value.Absolute] =
    Ordering.by[LedgerOffset.Value.Absolute, Long](_.value.toLong)

  private val CompositeAbsoluteOffsetIntOrdering: Ordering[LedgerOffset.Value.Absolute] =
    Ordering.by[LedgerOffset.Value.Absolute, (Long, Long)](parseCompositeOffset)

  private def isCompositeOffsetFormat(a: LedgerOffset.Value.Absolute): Boolean =
    a.value.contains('-')

  private def parseCompositeOffset(a: LedgerOffset.Value.Absolute): (Long, Long) = {
    val offset: String = a.value
    offset.split('-') match {
      case Array(_, a2, a3) =>
        (a2.toLong, a3.toLong)
      case _ =>
        throw new IllegalArgumentException(
          s"Expected composite offset in the format: '<block-hash>-<block-height>-<event-id>', got: ${offset: String}")
    }
  }
}
