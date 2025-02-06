// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data

import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.{
  Block,
  Epoch,
  EpochInProgress,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Genesis.GenesisEpoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  OrderedBlocksReader,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.{
  BlockMetadata,
  EpochInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  NewView,
  PbftNetworkMessage,
  PrePrepare,
  Prepare,
  ViewChange,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

trait EpochStoreTest extends AsyncWordSpec {
  this: AsyncWordSpec & BftSequencerBaseTest =>

  import EpochStoreTest.*

  private[bftordering] def epochStore(
      createStore: () => EpochStore[PekkoEnv] & OrderedBlocksReader[PekkoEnv]
  ): Unit = {

    "completeEpoch" should {
      "create and retrieve Epochs" in {
        val store = createStore()
        val blockNumber0 = 9L
        val prePrepare0 = prePrepare(EpochNumber.First, blockNumber0)
        val commitMessages0 = commitMessages(EpochNumber.First, blockNumber0)
        val epochInfo0 = EpochInfo.mk(
          number = EpochNumber.First,
          startBlockNumber = BlockNumber.First,
          length = 10L,
        )
        val epoch0 = Epoch(
          epochInfo0,
          commitMessages0,
        )

        val epochNumber1 = 10L
        val prePrepare1 = prePrepare(epochNumber1, 19L)
        val commitMessages1 = commitMessages(epochNumber1, 19L)

        val epochInfo1 = EpochInfo.mk(
          number = epochNumber1,
          startBlockNumber = 10L,
          length = 10L,
        )
        val epoch1 = Epoch(
          epochInfo1,
          commitMessages1,
        )

        for {
          _ <- store.startEpoch(epochInfo0)
          // idempotent writes are supported
          _ <- store.startEpoch(epochInfo0)
          _ <- store.addOrderedBlock(prePrepare0, commitMessages0)
          // idempotent writes are supported
          _ <- store.addOrderedBlock(prePrepare0, commitMessages0)

          e0 <- store.latestEpoch(includeInProgress = false)
          e1 <- store.latestEpoch(includeInProgress = true)

          _ <- store.completeEpoch(epochInfo0.number)
          e2 <- store.latestEpoch(includeInProgress = false)
          e3 <- store.latestEpoch(includeInProgress = true)

          // idempotent writes are supported
          _ <- store.completeEpoch(epochInfo0.number)
          e4 <- store.latestEpoch(includeInProgress = false)
          e5 <- store.latestEpoch(includeInProgress = true)

          _ <- store.startEpoch(epochInfo1)
          _ <- store.addOrderedBlock(prePrepare1, commitMessages1)
          e6 <- store.latestEpoch(includeInProgress = false)
          e7 <- store.latestEpoch(includeInProgress = true)
        } yield {
          e0 shouldBe GenesisEpoch
          e1 shouldBe epoch0
          e2 shouldBe epoch0
          e3 shouldBe epoch0
          e4 shouldBe epoch0
          e5 shouldBe epoch0
          e6 shouldBe epoch0
          e7 shouldBe epoch1
        }
      }
    }

    "latestCompletedEpoch" should {
      "return the genesisEpoch initially" in {
        val store = createStore()
        for {
          e0 <- store.latestEpoch(includeInProgress = false)
          e1 <- store.latestEpoch(includeInProgress = true)
        } yield {
          e0 shouldBe GenesisEpoch
          e1 shouldBe GenesisEpoch
        }
      }
    }

    "addOrderedBlock" should {
      "create and retrieve EpochInProgress" in {
        val store = createStore()
        val activeEpoch0Info = EpochInfo.mk(EpochNumber.First, BlockNumber.First, 10)
        val activeEpoch1Info = EpochInfo.mk(1L, 10L, 10)

        def addOrderedBlock(
            epochNumber: Long,
            blockNumber: Long,
            viewNumber: Long = ViewNumber.First,
        ) =
          store.addOrderedBlock(
            prePrepare(epochNumber, blockNumber, viewNumber),
            commitMessages(epochNumber, blockNumber, viewNumber),
          )

        for {
          _ <- store.startEpoch(activeEpoch0Info)

          // these shouldn't show up in loadEpochProgress because block 0 is being completed
          _ <- store.addPrePrepare(prePrepare(EpochNumber.First, BlockNumber.First))
          _ <- store.addPrepares(Seq(prepare(EpochNumber.First, BlockNumber.First)))

          _ <- addOrderedBlock(EpochNumber.First, BlockNumber.First)
          _ <- addOrderedBlock(EpochNumber.First, 1L)
          _ <- addOrderedBlock(EpochNumber.First, 2L)

          // these will appear in loadEpochProgress as pbftMessagesForIncompleteBlocks because block 3 is not complete
          _ <- store.addPrePrepare(prePrepare(EpochNumber.First, 3L))
          _ <- store.addPrepares(Seq(prepare(EpochNumber.First, 3L)))

          // view change messages will appear always because we don't check in the DB if the segment has finished
          _ <- store.addViewChangeMessage(viewChange(EpochNumber.First, 0L))
          _ <- store.addViewChangeMessage(newView(EpochNumber.First, 0L))

          // in-progress messages for later views are accounted for separately
          _ <- store.addPrePrepare(
            prePrepare(EpochNumber.First, 3L, viewNumber = ViewNumber.First + 1)
          )
          _ <- store.addPrepares(
            Seq(prepare(EpochNumber.First, 3L, viewNumber = ViewNumber.First + 1))
          )

          e0 <- store.loadEpochProgress(activeEpoch0Info)

          // updating an existing row should be ignored
          _ <- addOrderedBlock(
            EpochNumber.First,
            BlockNumber.First,
            viewNumber = ViewNumber.First + 1,
          )

          _ <- store.startEpoch(activeEpoch1Info)

          // test out-of-order and gap inserts in new activeEpoch
          _ <- addOrderedBlock(1L, 13L)
          _ <- addOrderedBlock(1L, 10L)
          _ <- addOrderedBlock(1L, 11L)

          e1 <- store.loadEpochProgress(activeEpoch1Info)
        } yield {
          e0 shouldBe EpochInProgress(
            Seq(BlockNumber.First, 1L, 2L).map(n =>
              Block(
                activeEpoch0Info.number,
                BlockNumber(n),
                CommitCertificate(
                  prePrepare(activeEpoch0Info.number, n),
                  commitMessages(activeEpoch0Info.number, n),
                ),
              )
            ),
            pbftMessagesForIncompleteBlocks = Seq[SignedMessage[PbftNetworkMessage]](
              viewChange(EpochNumber.First, 0L),
              newView(EpochNumber.First, 0L),
              prePrepare(EpochNumber.First, 3L),
              prePrepare(EpochNumber.First, 3L, viewNumber = ViewNumber.First + 1),
              prepare(EpochNumber.First, 3L),
              prepare(EpochNumber.First, 3L, viewNumber = ViewNumber.First + 1),
            ),
          )
          e1 shouldBe EpochInProgress(
            Seq(10L, 11L, 13L).map(n =>
              Block(
                activeEpoch1Info.number,
                BlockNumber(n),
                CommitCertificate(
                  prePrepare(activeEpoch1Info.number, n),
                  commitMessages(activeEpoch1Info.number, n),
                ),
              )
            ),
            Seq.empty,
          )
        }
      }
    }

    "loadPrePrepares" should {
      "load pre-prepares" in {
        val store = createStore()
        val epoch0 = EpochInfo.mk(EpochNumber.First, BlockNumber.First, 1)
        val epoch1 = EpochInfo.mk(1L, 1L, 1)
        val epoch2 = EpochInfo.mk(2L, 2L, 2)
        val epoch3 = EpochInfo.mk(3L, 3L, 3)
        for {
          _ <- store.startEpoch(epoch0)
          _ <- store.addOrderedBlock(
            prePrepare(EpochNumber.First, BlockNumber.First),
            commitMessages(EpochNumber.First, BlockNumber.First),
          )
          _ <- store.startEpoch(epoch2)
          _ <- store.addOrderedBlock(
            prePrepare(epochNumber = 2L, blockNumber = 2L),
            commitMessages(epochNumber = 2L, blockNumber = 2L),
          )
          _ <- store.startEpoch(epoch1)
          _ <- store.addOrderedBlock(
            prePrepare(epochNumber = 1L, blockNumber = 1L),
            commitMessages(epochNumber = 1L, blockNumber = 1L),
          )
          _ <- store.startEpoch(epoch3)
          _ <- store.addOrderedBlock(
            prePrepare(epochNumber = 3L, blockNumber = 3L),
            commitMessages(epochNumber = 3L, blockNumber = 3L),
          )
          blocks <- store.loadCompleteBlocks(
            startEpochNumberInclusive = EpochNumber(1L),
            endEpochNumberInclusive = EpochNumber(2L),
          )
        } yield {
          blocks shouldBe Seq(
            Block(
              EpochNumber(1L),
              BlockNumber(1L),
              CommitCertificate(prePrepare(1L, 1L), commitMessages(1L, 1L)),
            ),
            Block(
              EpochNumber(2L),
              BlockNumber(2L),
              CommitCertificate(prePrepare(2L, 2L), commitMessages(2L, 2L)),
            ),
          )
        }
      }
    }

    "loadOrderedBlocks" should {
      "load ordered blocks" in {
        val store = createStore()
        val epoch0 = EpochInfo.mk(EpochNumber.First, BlockNumber.First, length = 2)

        val expectedOrderedBlocks =
          Seq(
            orderedBlock(BlockNumber.First, isLastInEpoch = false),
            orderedBlock(BlockNumber(1), isLastInEpoch = true),
          )

        for {
          _ <- store.startEpoch(epoch0)
          _ <- store.addOrderedBlock(
            prePrepare(epochNumber = EpochNumber.First, blockNumber = BlockNumber.First),
            Seq.empty,
          )
          _ <- store.addOrderedBlock(
            prePrepare(epochNumber = EpochNumber.First, blockNumber = BlockNumber(1)),
            Seq.empty,
          )
          blocks <- store.loadOrderedBlocks(initialBlockNumber = BlockNumber.First)
        } yield {
          blocks should contain theSameElementsInOrderAs expectedOrderedBlocks
        }
      }
    }
  }
}

object EpochStoreTest {

  private def prePrepare(
      epochNumber: Long,
      blockNumber: Long,
      viewNumber: Long = ViewNumber.First,
  ) = PrePrepare
    .create(
      BlockMetadata.mk(epochNumber, blockNumber),
      ViewNumber(viewNumber),
      CantonTimestamp.Epoch,
      OrderingBlock(Seq.empty),
      CanonicalCommitSet(Set.empty),
      from = fakeSequencerId("address"),
    )
    .fakeSign

  private def prepare(
      epochNumber: Long,
      blockNumber: Long,
      viewNumber: Long = ViewNumber.First,
  ) =
    Prepare
      .create(
        BlockMetadata.mk(epochNumber, blockNumber),
        ViewNumber(viewNumber),
        Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
        CantonTimestamp.Epoch,
        from = fakeSequencerId("address"),
      )
      .fakeSign

  private def commitMessages(
      epochNumber: Long,
      blockNumber: Long,
      viewNumber: Long = ViewNumber.First,
  ) = (0L to 2L).map { i =>
    Commit
      .create(
        BlockMetadata.mk(epochNumber, blockNumber),
        ViewNumber(viewNumber),
        Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
        CantonTimestamp.Epoch,
        from = fakeSequencerId(s"address$i"),
      )
      .fakeSign
  }

  def viewChange(
      epochNumber: Long,
      segmentNumber: Long,
      viewNumber: Long = ViewNumber.First,
  ): SignedMessage[ViewChange] =
    ViewChange
      .create(
        BlockMetadata.mk(epochNumber, segmentNumber),
        0,
        ViewNumber(viewNumber),
        CantonTimestamp.Epoch,
        consensusCerts = Seq.empty,
        fakeSequencerId("address"),
      )
      .fakeSign

  def newView(
      epochNumber: Long,
      segmentNumber: Long,
      viewNumber: Long = ViewNumber.First,
  ): SignedMessage[NewView] =
    NewView
      .create(
        BlockMetadata.mk(epochNumber, segmentNumber),
        segmentIndex = 0,
        viewNumber = ViewNumber(viewNumber),
        localTimestamp = CantonTimestamp.Epoch,
        viewChanges = Seq.empty,
        prePrepares = Seq.empty,
        fakeSequencerId("address"),
      )
      .fakeSign

  private def orderedBlock(blockNumber: BlockNumber, isLastInEpoch: Boolean) =
    OrderedBlockForOutput(
      OrderedBlock(
        BlockMetadata.mk(EpochNumber.First, blockNumber),
        batchRefs = Seq.empty,
        CanonicalCommitSet.empty,
      ),
      fakeSequencerId("address"),
      isLastInEpoch,
      mode = OrderedBlockForOutput.Mode.FromConsensus,
    )
}
