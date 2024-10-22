// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering

import com.digitalasset.canton.topology.SequencerId

/** The class allows preserving some contextual information (the info from consensus about whether a block
  * is the last in an epoch) during the roundtrip from output to local availability to retrieve the batches.
  * Since the output module processes blocks in order, this boolean information is enough to determine
  * when an epoch ends and topology should be queried.
  */
final case class OrderedBlockForOutput(
    orderedBlock: OrderedBlock,
    from: SequencerId, // Only used for metrics
    isLastInEpoch: Boolean,
    isStateTransferred: Boolean = false,
)
