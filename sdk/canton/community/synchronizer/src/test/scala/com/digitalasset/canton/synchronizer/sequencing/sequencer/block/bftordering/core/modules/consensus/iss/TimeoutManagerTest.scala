// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssSegmentModule.BlockCompletionTimeout
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNormalTimeout
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.unit.modules.{
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.tracing.TraceContext
import org.scalatest.wordspec.AsyncWordSpec

class TimeoutManagerTest extends AsyncWordSpec with BftSequencerBaseTest {
  private val v0 = ViewNumber.First
  private val v1 = ViewNumber(v0 + 1)
  private val v2 = ViewNumber(v1 + 1)

  private val timeout0 = PbftNormalTimeout(BlockMetadata(EpochNumber.First, BlockNumber.First), v0)
  private val timeout1 = PbftNormalTimeout(BlockMetadata(EpochNumber.First, BlockNumber.First), v1)
  private val timeout2 = PbftNormalTimeout(BlockMetadata(EpochNumber.First, BlockNumber.First), v2)

  "TimeoutManager" should {
    "be able to schedule, reschedule and cancel events" in {
      val timeoutManager =
        new TimeoutManager[ProgrammableUnitTestEnv, ConsensusSegment.Message, BlockNumber](
          loggerFactory,
          BlockCompletionTimeout,
          timeoutId = BlockNumber.First,
        )
      val context = new ProgrammableUnitTestContext[ConsensusSegment.Message]()

      timeoutManager.scheduleTimeout(timeout0)(context, TraceContext.empty)
      context.lastDelayedMessage shouldBe Some((1, timeout0))
      context.lastCancelledEvent shouldBe None

      timeoutManager.scheduleTimeout(timeout1)(context, TraceContext.empty)
      context.lastDelayedMessage shouldBe Some((2, timeout1))
      // previous event got automatically cancelled
      context.lastCancelledEvent shouldBe Some((1, timeout0))

      timeoutManager.cancelTimeout()
      context.lastDelayedMessage shouldBe Some((2, timeout1))
      // previous event got explicitly cancelled
      context.lastCancelledEvent shouldBe Some((2, timeout1))

      timeoutManager.cancelTimeout() // nothing happens
      context.lastDelayedMessage shouldBe Some((2, timeout1))
      context.lastCancelledEvent shouldBe Some((2, timeout1))

      timeoutManager.scheduleTimeout(timeout2)(context, TraceContext.empty)
      context.lastDelayedMessage shouldBe Some((3, timeout2))
      // previous event had already been cancelled
      context.lastCancelledEvent shouldBe Some((2, timeout1))
    }
  }
}
