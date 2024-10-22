// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework

import com.digitalasset.canton.domain.block.BlockFormat
import com.digitalasset.canton.tracing.TraceContext
import org.apache.pekko.stream.scaladsl.{Keep, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches}

class EmptyBlockSubscription extends BlockSubscription {

  override def subscription(): Source[BlockFormat.Block, KillSwitch] =
    Source.empty.viaMat(KillSwitches.single)(Keep.right)

  override def receiveBlock(block: BlockFormat.Block)(implicit traceContext: TraceContext): Unit =
    ()
}
