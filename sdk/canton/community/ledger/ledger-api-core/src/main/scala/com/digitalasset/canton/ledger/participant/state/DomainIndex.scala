// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

final case class DomainIndex(
    requestIndex: Option[RequestIndex],
    sequencerIndex: Option[SequencerIndex],
    recordTime: CantonTimestamp,
) {
  def max(otherDomainIndex: DomainIndex): DomainIndex =
    new DomainIndex(
      requestIndex =
        requestIndex.iterator.++(otherDomainIndex.requestIndex.iterator).maxByOption(_.counter),
      sequencerIndex = sequencerIndex.iterator
        .++(otherDomainIndex.sequencerIndex.iterator)
        .maxByOption(_.counter),
      recordTime = recordTime max otherDomainIndex.recordTime,
    )

  override def toString: String =
    s"DomainIndex(requestIndex=$requestIndex, sequencerIndex=$sequencerIndex, recordTime=$recordTime)"
}

object DomainIndex {
  def of(requestIndex: RequestIndex): DomainIndex =
    DomainIndex(
      Some(requestIndex),
      requestIndex.sequencerCounter.map(
        SequencerIndex(_, requestIndex.timestamp)
      ),
      requestIndex.timestamp,
    )

  def of(sequencerIndex: SequencerIndex): DomainIndex =
    DomainIndex(
      None,
      Some(sequencerIndex),
      sequencerIndex.timestamp,
    )

  def of(recordTime: CantonTimestamp): DomainIndex =
    DomainIndex(
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
