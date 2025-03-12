// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState.Segment
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.SegmentInProgress.RehydrationMessages
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PbftNetworkMessage
import org.scalatest.wordspec.AsyncWordSpec

class SegmentInProgressTest extends AsyncWordSpec with BftSequencerBaseTest {

  import SegmentStateTest.*

  "SegmentInProgress" should {

    def rehydrationMessages(
        pbftMessagesForIncompleteBlocks: Seq[SignedMessage[PbftNetworkMessage]]
    ) =
      SegmentInProgress.rehydrationMessages(
        Segment(myId, NonEmpty(Seq, BlockNumber(0L), BlockNumber(2L))),
        EpochStore.EpochInProgress(
          completedBlocks = Seq.empty,
          pbftMessagesForIncompleteBlocks,
        ),
      )

    val v0 = ViewNumber.First
    val v1 = v0 + 1
    val v2 = v1 + 1
    val v3 = v2 + 1

    val ppView0 = createPrePrepare(blockNumber = 0L, view = v0, from = myId)
    val ppBlock2View0 = createPrePrepare(blockNumber = 2L, view = v0, from = myId)
    val p1View0 = createPrepare(blockNumber = 0L, view = v0, from = myId, ppView0.message.hash)
    val p2View0 =
      createPrepare(blockNumber = 0L, view = v0, from = otherIds(0), ppView0.message.hash)

    val viewChange1 =
      createViewChange(viewNumber = v1, from = otherIds(0), originalLeader = myId, Seq.empty)
    val ppView1 = createPrePrepare(blockNumber = 0L, view = v1, from = otherIds(0))
    val newView1 = createNewView(
      viewNumber = v1,
      from = otherIds(0),
      originalLeader = myId,
      Seq(viewChange1),
      Seq(ppView1),
    )
    val p1View1 =
      createPrepare(blockNumber = 0L, view = v1, from = otherIds(0), ppView0.message.hash)
    val p2View1 =
      createPrepare(blockNumber = 0L, view = v1, from = otherIds(1), ppView0.message.hash)

    val viewChange2 =
      createViewChange(viewNumber = v2, from = otherIds(1), originalLeader = myId, Seq.empty)
    val viewChange3 =
      createViewChange(viewNumber = v3, from = otherIds(2), originalLeader = myId, Seq.empty)

    val ppView0AnotherSegment = createPrePrepare(blockNumber = 1L, view = v0, from = otherIds(0))
    val p1View0AnotherSegment =
      createPrepare(blockNumber = 1L, view = v0, from = otherIds(0), ppView0.message.hash)
    val p2View0AnotherSegment =
      createPrepare(blockNumber = 1L, view = v0, from = otherIds(1), ppView0.message.hash)

    "rehydrate all messages if no view-change involved" in {
      rehydrationMessages(
        Seq[SignedMessage[PbftNetworkMessage]](ppView0, ppBlock2View0, p1View0, p2View0)
      ) shouldBe
        RehydrationMessages(
          prepares = Seq(p1View0, p2View0),
          oldViewsMessages = Seq.empty,
          currentViewMessages = Seq(ppView0, ppBlock2View0),
        )
    }

    "include view-change message to indicate start of view change" in {
      rehydrationMessages(
        Seq[SignedMessage[PbftNetworkMessage]](
          ppView0,
          ppBlock2View0,
          p1View0,
          p2View0,
          viewChange1,
        )
      ) shouldBe RehydrationMessages(
        prepares = Seq(p1View0, p2View0),
        oldViewsMessages = Seq(ppView0, ppBlock2View0),
        currentViewMessages = Seq(viewChange1),
      )
    }

    "exclude view-change messages for a view if new-view is already present" in {
      rehydrationMessages(
        Seq[SignedMessage[PbftNetworkMessage]](ppView0, p1View0, p2View0, viewChange1, newView1)
      ) shouldBe RehydrationMessages(
        prepares = Seq(p1View0, p2View0),
        oldViewsMessages = Seq(ppView0),
        currentViewMessages = Seq(newView1),
      )
    }

    "exclude pre-prepare for a view if new-view is already included" in {
      rehydrationMessages(
        Seq[SignedMessage[PbftNetworkMessage]](
          ppView0,
          p1View0,
          p2View0,
          viewChange1,
          newView1,
          ppView1,
        )
      ) shouldBe RehydrationMessages(
        prepares = Seq(p1View0, p2View0),
        oldViewsMessages = Seq(ppView0),
        currentViewMessages = Seq(newView1),
      )
    }

    "only include prepares from highest view for a block" in {
      rehydrationMessages(
        Seq[SignedMessage[PbftNetworkMessage]](
          ppView0,
          p1View0,
          p2View0,
          viewChange1,
          newView1,
          ppView1,
          p1View1,
          p2View1,
        )
      ) shouldBe RehydrationMessages(
        prepares = Seq(p1View1, p2View1),
        oldViewsMessages = Seq(ppView0),
        currentViewMessages = Seq(newView1),
      )
    }

    "include view-change after the latest new-view" in {
      rehydrationMessages(
        Seq[SignedMessage[PbftNetworkMessage]](
          ppView0,
          p1View0,
          p2View0,
          viewChange1,
          newView1,
          ppView1,
          p1View1,
          p2View1,
          viewChange2,
        )
      ) shouldBe RehydrationMessages(
        prepares = Seq(p1View1, p2View1),
        oldViewsMessages = Seq[SignedMessage[PbftNetworkMessage]](
          ppView0,
          newView1,
        ),
        currentViewMessages = Seq(viewChange2),
      )
    }

    "only include the highest view-change after the latest new-view" in {
      rehydrationMessages(
        Seq[SignedMessage[PbftNetworkMessage]](
          ppView0,
          p1View0,
          p2View0,
          viewChange1,
          newView1,
          ppView1,
          p1View1,
          p2View1,
          viewChange2,
          viewChange3,
        )
      ) shouldBe RehydrationMessages(
        prepares = Seq(p1View1, p2View1),
        oldViewsMessages = Seq[SignedMessage[PbftNetworkMessage]](
          ppView0,
          newView1,
        ),
        currentViewMessages = Seq(
          viewChange3
        ),
      )
    }

    "messages for other segments are filtered out" in {
      rehydrationMessages(
        Seq[SignedMessage[PbftNetworkMessage]](
          ppView0AnotherSegment,
          p1View0AnotherSegment,
          p2View0AnotherSegment,
        )
      ) shouldBe RehydrationMessages(Seq(), Seq(), Seq())
    }
  }
}
