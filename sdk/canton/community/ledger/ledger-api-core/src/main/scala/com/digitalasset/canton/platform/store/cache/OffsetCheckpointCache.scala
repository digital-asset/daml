// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.cache

import com.daml.ledger.api.v2.offset_checkpoint as v2
import com.daml.ledger.api.v2.offset_checkpoint.DomainTime
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.util.TimestampConversion.fromInstant
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

final case class OffsetCheckpoint(offset: Offset, domainTimes: Map[DomainId, Timestamp]) {
  lazy val toApi: v2.OffsetCheckpoint =
    v2.OffsetCheckpoint(
      offset = offset.unwrap,
      domainTimes = domainTimes.map { case (domain, t) =>
        DomainTime(domain.toProtoPrimitive, Some(fromInstant(t.toInstant)))
      }.toSeq,
    )

}
