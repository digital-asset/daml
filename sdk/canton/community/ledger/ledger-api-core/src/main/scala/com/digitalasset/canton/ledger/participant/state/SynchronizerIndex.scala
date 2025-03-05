// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{RepairCounter, SequencerCounter}

final case class SynchronizerIndex(
    repairIndex: Option[RepairIndex],
    sequencerIndex: Option[SequencerIndex],
    recordTime: CantonTimestamp,
) {
  def max(otherSynchronizerIndex: SynchronizerIndex): SynchronizerIndex =
    new SynchronizerIndex(
      repairIndex = repairIndex.iterator
        .++(otherSynchronizerIndex.repairIndex.iterator)
        .maxByOption(identity),
      sequencerIndex = sequencerIndex.iterator
        .++(otherSynchronizerIndex.sequencerIndex.iterator)
        .maxByOption(_.counter),
      recordTime = recordTime max otherSynchronizerIndex.recordTime,
    )

  override def toString: String =
    s"SynchronizerIndex(sequencerIndex=$sequencerIndex, repairIndex=$repairIndex, recordTime=$recordTime)"
}

object SynchronizerIndex {
  def of(repairIndex: RepairIndex): SynchronizerIndex =
    SynchronizerIndex(
      Some(repairIndex),
      None,
      repairIndex.timestamp,
    )

  def of(sequencerIndex: SequencerIndex): SynchronizerIndex =
    SynchronizerIndex(
      None,
      Some(sequencerIndex),
      sequencerIndex.timestamp,
    )

  def of(recordTime: CantonTimestamp): SynchronizerIndex =
    SynchronizerIndex(
      None,
      None,
      recordTime,
    )
}

final case class SequencerIndex(counter: SequencerCounter, timestamp: CantonTimestamp)

final case class RepairIndex(timestamp: CantonTimestamp, counter: RepairCounter)

object RepairIndex {
  implicit val orderingRepairIndex: Ordering[RepairIndex] =
    Ordering.by[RepairIndex, (CantonTimestamp, RepairCounter)](repairIndex =>
      (repairIndex.timestamp, repairIndex.counter)
    )
}
