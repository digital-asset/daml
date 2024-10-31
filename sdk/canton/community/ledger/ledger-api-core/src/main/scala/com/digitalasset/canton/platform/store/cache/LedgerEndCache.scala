// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp}

trait LedgerEndCache {
  def apply(): (Option[AbsoluteOffset], Long)
  def publicationTime: CantonTimestamp
}

trait MutableLedgerEndCache extends LedgerEndCache {
  def set(ledgerEnd: (Option[AbsoluteOffset], Long, CantonTimestamp)): Unit
}

object MutableLedgerEndCache {
  def apply(): MutableLedgerEndCache =
    new MutableLedgerEndCache {
      @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
      @volatile private var ledgerEnd: (Option[AbsoluteOffset], Long, CantonTimestamp) = _

      override def set(ledgerEnd: (Option[AbsoluteOffset], Long, CantonTimestamp)): Unit =
        this.ledgerEnd = ledgerEnd

      @SuppressWarnings(Array("org.wartremover.warts.Null"))
      override def apply(): (Option[AbsoluteOffset], Long) =
        if (ledgerEnd == null) throw new IllegalStateException("uninitialized")
        else {
          val (offset, sequentialId, _) = ledgerEnd
          offset -> sequentialId
        }

      @SuppressWarnings(Array("org.wartremover.warts.Null"))
      override def publicationTime: CantonTimestamp =
        if (ledgerEnd == null) throw new IllegalStateException("uninitialized")
        else {
          val (_, _, publicationTime) = ledgerEnd
          publicationTime
        }
    }
}
