// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.ledger.api.v2.offset_checkpoint as v2
import com.daml.ledger.api.v2.offset_checkpoint.SynchronizerTime
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.TimestampConversion.fromInstant
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.daml.lf.data.Time.Timestamp

import java.util.concurrent.atomic.AtomicReference

class OffsetCheckpointCache {
  private val offsetCheckpoint: AtomicReference[Option[OffsetCheckpoint]] =
    new AtomicReference(None)

  def push(newOffsetCheckpoint: OffsetCheckpoint): Unit =
    offsetCheckpoint.set(Some(newOffsetCheckpoint))

  def getOffsetCheckpoint: Option[OffsetCheckpoint] = offsetCheckpoint.get()

}

final case class OffsetCheckpoint(
    offset: Offset,
    synchronizerTimes: Map[SynchronizerId, Timestamp],
) {
  lazy val toApi: v2.OffsetCheckpoint =
    v2.OffsetCheckpoint(
      offset = offset.unwrap,
      synchronizerTimes = synchronizerTimes.map { case (synchronizer, t) =>
        SynchronizerTime(synchronizer.toProtoPrimitive, Some(fromInstant(t.toInstant)))
      }.toSeq,
    )

}
