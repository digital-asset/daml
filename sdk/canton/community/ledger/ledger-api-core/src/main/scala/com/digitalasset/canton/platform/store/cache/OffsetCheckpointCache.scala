// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.daml.lf.data.Time.Timestamp

import java.util.concurrent.atomic.AtomicReference

class OffsetCheckpointCache {
  private val offsetCheckpoint: AtomicReference[Option[OffsetCheckpoint]] =
    new AtomicReference(None)

  def push(newOffsetCheckpoint: OffsetCheckpoint): Unit =
    offsetCheckpoint.set(Some(newOffsetCheckpoint))

  def getOffsetCheckpoint: Option[OffsetCheckpoint] = offsetCheckpoint.get()

}

final case class OffsetCheckpoint(offset: Offset, domainTimes: Map[DomainId, Timestamp])
