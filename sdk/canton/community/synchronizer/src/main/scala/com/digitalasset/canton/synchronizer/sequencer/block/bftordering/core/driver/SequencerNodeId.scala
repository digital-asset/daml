// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver

import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.topology.SequencerId

object SequencerNodeId {

  def toBftNodeId(sequencerId: SequencerId): BftNodeId = BftNodeId(sequencerId.toProtoPrimitive)

  def fromBftNodeId(
      node: BftNodeId,
      fieldName: String = "node",
  ): ProtoConverter.ParsingResult[SequencerId] =
    SequencerId.fromProtoPrimitive(node, fieldName)
}
