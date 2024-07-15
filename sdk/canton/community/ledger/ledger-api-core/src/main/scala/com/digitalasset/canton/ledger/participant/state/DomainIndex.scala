// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{RequestCounter, SequencerCounter}

final case class DomainIndex(
    requestIndex: Option[RequestIndex],
    sequencerIndex: Option[SequencerIndex],
) {
  def max(otherDomainIndex: DomainIndex): DomainIndex =
    new DomainIndex(
      requestIndex =
        requestIndex.iterator.++(otherDomainIndex.requestIndex.iterator).maxByOption(_.counter),
      sequencerIndex = sequencerIndex.iterator
        .++(otherDomainIndex.sequencerIndex.iterator)
        .maxByOption(_.counter),
    )

  override def toString: String =
    s"DomainIndex(requestIndex=$requestIndex, sequencerIndex=$sequencerIndex)"
}

object DomainIndex {
  def of(requestIndex: RequestIndex): DomainIndex =
    DomainIndex(
      Some(requestIndex),
      requestIndex.sequencerCounter.map(
        SequencerIndex(_, requestIndex.timestamp)
      ),
    )

  def of(sequencerIndex: SequencerIndex): DomainIndex =
    DomainIndex(
      None,
      Some(sequencerIndex),
    )

  val empty: DomainIndex = new DomainIndex(None, None)
}

final case class SequencerIndex(counter: SequencerCounter, timestamp: CantonTimestamp)

final case class RequestIndex(
    counter: RequestCounter,
    sequencerCounter: Option[SequencerCounter],
    timestamp: CantonTimestamp,
)
