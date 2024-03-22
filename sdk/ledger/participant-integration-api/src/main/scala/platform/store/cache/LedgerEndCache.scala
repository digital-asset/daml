// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.cache

import com.daml.ledger.offset.Offset

trait LedgerEndCache {
  def apply(): (Offset, Long)
}

trait MutableLedgerEndCache extends LedgerEndCache {
  def set(ledgerEnd: (Offset, Long)): Unit
}

object MutableLedgerEndCache {
  def apply(): MutableLedgerEndCache =
    new MutableLedgerEndCache {
      @volatile private var ledgerEnd: (Offset, Long) = _

      override def set(ledgerEnd: (Offset, Long)): Unit = this.ledgerEnd = ledgerEnd

      override def apply(): (Offset, Long) =
        if (ledgerEnd == null) throw new IllegalStateException("uninitialized")
        else ledgerEnd
    }
}
