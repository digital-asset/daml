// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.ledger.offset.Offset

trait LedgerEndCache {
  def apply(): (Offset, Long)
}

trait MutableLedgerEndCache extends LedgerEndCache {
  def set(ledgerEnd: (Offset, Long)): Unit
}

object MutableLedgerEndCache {
  def apply(): MutableLedgerEndCache =
    new MutableLedgerEndCache {
      @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
      @volatile private var ledgerEnd: (Offset, Long) = _

      override def set(ledgerEnd: (Offset, Long)): Unit = this.ledgerEnd = ledgerEnd

      @SuppressWarnings(Array("org.wartremover.warts.Null"))
      override def apply(): (Offset, Long) =
        if (ledgerEnd == null) throw new IllegalStateException("uninitialized")
        else ledgerEnd
    }
}

object ImmutableLedgerEndCache {
  def apply(ledgerEnd: (Offset, Long)): LedgerEndCache = () => ledgerEnd
}
