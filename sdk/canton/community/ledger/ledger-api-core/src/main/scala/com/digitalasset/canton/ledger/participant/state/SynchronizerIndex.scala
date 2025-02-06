// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

final case class SynchronizerIndex(
    requestIndex: Option[RequestIndex],
    sequencerIndex: Option[SequencerIndex],
    recordTime: CantonTimestamp,
) {
  def max(otherSynchronizerIndex: SynchronizerIndex): SynchronizerIndex =
    new SynchronizerIndex(
      requestIndex = requestIndex.iterator
        .++(otherSynchronizerIndex.requestIndex.iterator)
        .maxByOption(_.counter),
      sequencerIndex = sequencerIndex.iterator
        .++(otherSynchronizerIndex.sequencerIndex.iterator)
        .maxByOption(_.counter),
      recordTime = recordTime max otherSynchronizerIndex.recordTime,
    )

  override def toString: String =
    s"SynchronizerIndex(requestIndex=$requestIndex, sequencerIndex=$sequencerIndex, recordTime=$recordTime)"
}

object SynchronizerIndex {
  def of(requestIndex: RequestIndex): SynchronizerIndex =
    SynchronizerIndex(
      Some(requestIndex),
      requestIndex.sequencerCounter.map(
        SequencerIndex(_, requestIndex.timestamp)
      ),
      requestIndex.timestamp,
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

final case class RequestIndex(
    counter: RequestCounter,
    sequencerCounter: Option[SequencerCounter],
    timestamp: CantonTimestamp,
)
