// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.availability.ProofOfAvailability
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata

final case class OrderedBlock(
    metadata: BlockMetadata,
    batchRefs: Seq[ProofOfAvailability],
    canonicalCommitSet: CanonicalCommitSet,
)
