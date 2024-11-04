// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data

import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.EpochNumber
import com.digitalasset.canton.tracing.TraceContext

trait OutputBlocksReader[E <: Env[E]] {

  def loadOutputBlockMetadata(
      startEpochNumberInclusive: EpochNumber,
      endEpochNumberInclusive: EpochNumber,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Seq[OutputBlockMetadata]]
  protected def loadOutputBlockMetadataActionName(
      startEpochNumberInclusive: EpochNumber,
      endEpochNumberInclusive: EpochNumber,
  ): String =
    s"load output block metadata from epochs interval [$startEpochNumberInclusive, $endEpochNumberInclusive]"
}
