// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd

import java.util.concurrent.atomic.AtomicReference

trait LedgerEndCache {
  def apply(): Option[LedgerEnd]
}

trait MutableLedgerEndCache extends LedgerEndCache {
  def set(ledgerEnd: Option[LedgerEnd]): Unit
}

object MutableLedgerEndCache {
  def apply(): MutableLedgerEndCache =
    new MutableLedgerEndCache {
      private val ledgerEnd: AtomicReference[Option[LedgerEnd]] =
        new AtomicReference[Option[LedgerEnd]](None)

      override def set(ledgerEnd: Option[LedgerEnd]): Unit =
        this.ledgerEnd.set(ledgerEnd)

      override def apply(): Option[LedgerEnd] =
        ledgerEnd.get()
    }
}
