// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusStatus,
}
import com.digitalasset.canton.topology.SequencerId

/** As part of retransmissions, we broadcast an epoch status to other nodes. However, because we have
  * separate modules processing each segment, we need to request and get segment statuses from each segment
  * first in order to build the epoch status. This helper class is used to process (potentially out-of-order)
  * segment status messages and then build the epoch status once all segment status messages have arrived.
  */
class EpochStatusBuilder(from: SequencerId, epochNumber: EpochNumber, numberOfSegments: Int) {
  private val segmentArray =
    Array.fill[Option[ConsensusStatus.SegmentStatus]](numberOfSegments)(None)

  def receive(msg: Consensus.RetransmissionsMessage.SegmentStatus): Unit =
    segmentArray(msg.segmentIndex) = Some(msg.status)

  def epochStatus: Option[ConsensusStatus.EpochStatus] =
    segmentArray.toList.sequence.map { segments =>
      ConsensusStatus.EpochStatus.create(from, epochNumber, segments)
    }

}
