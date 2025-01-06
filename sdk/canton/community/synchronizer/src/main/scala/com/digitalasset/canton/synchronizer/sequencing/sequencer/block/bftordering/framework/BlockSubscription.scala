// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework

import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.KillSwitch
import org.apache.pekko.stream.scaladsl.Source

trait BlockSubscription {
  def subscription(): Source[BlockFormat.Block, KillSwitch]

  def receiveBlock(block: BlockFormat.Block)(implicit traceContext: TraceContext): Unit
}
