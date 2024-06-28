// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.ledger.offset.Offset

import java.util.concurrent.atomic.AtomicReference

class PruningOffsetCache {
  private val lastOffset: AtomicReference[Option[Offset]] =
    new AtomicReference(None)
  def set(offset: Offset): Unit = lastOffset.set(Some(offset))
  def get: Option[Offset] = lastOffset.get()
}
