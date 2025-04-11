// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.{
  OutputBlockMetadata,
  OutputEpochMetadata,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStoreTest.createBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait OutputMetadataStoreTest extends AsyncWordSpec {
  this: AsyncWordSpec & BftSequencerBaseTest =>

  private[bftordering] def outputBlockMetadataStore(
      createStore: () => OutputMetadataStore[PekkoEnv]
  ): Unit =
    "OutputBlockMetadataStore" should {

      "create and retrieve blocks" in {
        val store = createStore()
        val block = createBlock(BlockNumber.First)
        for {
          _ <- store.insertBlockIfMissing(block)
          retrievedBlocks <- store.getBlockFromInclusive(BlockNumber.First)
        } yield {
          retrievedBlocks should contain only OutputBlockMetadata(
            epochNumber = EpochNumber.First,
            blockNumber = BlockNumber.First,
            blockBftTime = CantonTimestamp.Epoch,
          )
        }
      }

      "create and retrieve epochs" in {
        val store = createStore()
        val epoch = OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
        for {
          _ <- store.insertEpochIfMissing(epoch)
          retrievedEpoch <- store.getEpoch(EpochNumber.First)
        } yield {
          retrievedEpoch should contain(epoch)
        }
      }

      "allow adding blocks out of order" in {
        val store = createStore()
        val block1 = createBlock(BlockNumber.First)
        val block2 = createBlock(1L)

        for {
          _ <- store.insertBlockIfMissing(block2)
          retrievedBlocks <- store.getBlockFromInclusive(BlockNumber.First)
          _ = retrievedBlocks shouldBe empty

          _ <- store.insertBlockIfMissing(block1)
          retrievedBlocks <- store.getBlockFromInclusive(BlockNumber.First)

        } yield {
          retrievedBlocks should contain theSameElementsInOrderAs Seq(block1, block2)
        }
      }

      "can only insert once per block number" in {
        val store = createStore()
        val block = createBlock(BlockNumber.First)
        val wrongBlock = block.copy(blockBftTime = CantonTimestamp.MaxValue)
        for {
          // Use plain futures for `suppressWarningsAndErrors` to work
          _ <- toFuture(store.insertBlockIfMissing(block))
          _ <- toFuture(store.insertBlockIfMissing(block)) // does nothing
          _ <- loggerFactory.suppressWarningsAndErrors(
            toFuture(store.insertBlockIfMissing(wrongBlock))
          ) // does nothing but an implementation is free to log warning or errors

          retrievedBlocks <- toFuture(store.getBlockFromInclusive(BlockNumber.First))
        } yield {
          retrievedBlocks should contain only block
        }
      }

      "can only insert once per epoch number" in {
        val store = createStore()
        val epoch = OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
        val wrongEpoch = epoch.copy(couldAlterOrderingTopology = false)
        for {
          // Use plain futures for `suppressWarningsAndErrors` to work
          _ <- toFuture(store.insertEpochIfMissing(epoch))
          _ <- toFuture(store.insertEpochIfMissing(epoch)) // does nothing
          _ <- loggerFactory.suppressWarningsAndErrors(
            toFuture(store.insertEpochIfMissing(wrongEpoch))
          ) // does nothing but an implementation is free to log warning or errors

          retrievedEpoch <- toFuture(store.getEpoch(EpochNumber.First))
        } yield {
          retrievedEpoch should contain(epoch)
        }
      }

      "get a block at or directly before a timestamp" in {
        val store = createStore()
        val block1 = createBlock(BlockNumber.First)
        val block2 = createBlock(1L, timestamp = CantonTimestamp.Epoch.plusMillis(1))
        val block3 = createBlock(2L, timestamp = CantonTimestamp.Epoch.plusMillis(3))
        val storeInit: Future[Unit] = for {
          _ <- store.insertBlockIfMissing(block1)
          _ <- store.insertBlockIfMissing(block2)
          _ <- store.insertBlockIfMissing(block3)
        } yield ()

        forAll(
          Table[CantonTimestamp, Option[Long]](
            ("at or directly before timestamp", "expected block number"),
            (CantonTimestamp.Epoch.minusMillis(1), None),
            (CantonTimestamp.Epoch, Some(BlockNumber.First)),
            (CantonTimestamp.Epoch.plusMillis(2), Some(1L)),
          )
        ) { case (timestamp, blockNumber) =>
          storeInit.flatMap { _ =>
            store
              .getLatestBlockAtOrBefore(timestamp)
              .map(_.map(_.blockNumber) shouldBe blockNumber)
          }
        }
      }

      "get the first block in an epoch" in {
        val store = createStore()
        val block1 = createBlock(BlockNumber.First)
        val block2 = createBlock(1L)
        val block3 = createBlock(2L, epochNumber = 1L)
        val storeInit: Future[Unit] = for {
          _ <- store.insertBlockIfMissing(block1)
          _ <- store.insertBlockIfMissing(block2)
          _ <- store.insertBlockIfMissing(block3)
        } yield ()

        forAll(
          Table[Long, Option[Long]](
            ("epoch number", "expected block number"),
            (EpochNumber.First, Some(BlockNumber.First)),
            (1L, Some(2L)),
            (2L, None),
          )
        ) { case (epochNumber, blockNumber) =>
          storeInit.flatMap { _ =>
            store
              .getFirstBlockInEpoch(EpochNumber(epochNumber))
              .map(_.map(_.blockNumber) shouldBe blockNumber)
          }
        }
      }

      "get the last block in an epoch" in {
        val store = createStore()
        val block1 = createBlock(BlockNumber.First)
        val block2 = createBlock(1L)
        val block3 = createBlock(2L, epochNumber = 1L)
        val storeInit: Future[Unit] = for {
          _ <- store.insertBlockIfMissing(block1)
          _ <- store.insertBlockIfMissing(block2)
          _ <- store.insertBlockIfMissing(block3)
        } yield ()

        forAll(
          Table[Long, Option[Long]](
            ("epoch number", "expected block number"),
            (EpochNumber.First, Some(1L)),
            (1L, Some(2L)),
            (2L, None),
          )
        ) { case (epochNumber, blockNumber) =>
          storeInit.flatMap { _ =>
            store
              .getLastBlockInEpoch(EpochNumber(epochNumber))
              .map(_.map(_.blockNumber) shouldBe blockNumber)
          }
        }
      }

      "get the last consecutive block in the store" when {

        "the store is empty" in {
          val store = createStore()
          store.getLastConsecutiveBlock.map(_ shouldBe empty)
        }

        "all blocks in the store are consecutive" in {
          val store = createStore()
          val block1 = createBlock(BlockNumber.First)
          val block2 = createBlock(1L)
          val storeInit: Future[Unit] = for {
            _ <- store.insertBlockIfMissing(block1)
            _ <- store.insertBlockIfMissing(block2)
          } yield ()

          storeInit.flatMap { _ =>
            store.getLastConsecutiveBlock
              .map(_.map(_.blockNumber) shouldBe Some(1L))
          }
        }

        "blocks in the store are consecutive only after an initial gap" in {
          val store = createStore()
          val block1 = createBlock(1L)
          val block2 = createBlock(2L)
          val storeInit: Future[Unit] = for {
            _ <- store.insertBlockIfMissing(block1)
            _ <- store.insertBlockIfMissing(block2)
          } yield ()

          storeInit.flatMap { _ =>
            store.getLastConsecutiveBlock
              .map(_.map(_.blockNumber) shouldBe None)
          }
        }

        "blocks in the store are consecutive only after a gap after the initial block" in {
          val store = createStore()
          val block1 = createBlock(BlockNumber.First)
          val block2 = createBlock(2L)
          val block3 = createBlock(3L)
          val storeInit: Future[Unit] = for {
            _ <- store.insertBlockIfMissing(block1)
            _ <- store.insertBlockIfMissing(block2)
            _ <- store.insertBlockIfMissing(block3)
          } yield ()

          storeInit.flatMap { _ =>
            store.getLastConsecutiveBlock
              .map(_.map(_.blockNumber) shouldBe Some(BlockNumber.First))
          }
        }

        "blocks in the store are not all consecutive" in {
          val store = createStore()
          val block1 = createBlock(BlockNumber.First)
          val block2 = createBlock(1L)
          val block3 = createBlock(3L)
          val storeInit: Future[Unit] = for {
            _ <- store.insertBlockIfMissing(block1)
            _ <- store.insertBlockIfMissing(block2)
            _ <- store.insertBlockIfMissing(block3)
          } yield ()

          storeInit.flatMap { _ =>
            store.getLastConsecutiveBlock
              .map(_.map(_.blockNumber) shouldBe Some(1L))
          }
        }
      }

      "set that there are pending changes in the next epoch" in {
        val store = createStore()
        val block1 = createBlock(BlockNumber.First)
        val storeInit: Future[Unit] = for {
          _ <- store.insertBlockIfMissing(block1)
        } yield ()

        for {
          _ <- storeInit
          _ <- store.insertEpochIfMissing(
            OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
          )
          // Idempotent
          _ <- store.insertEpochIfMissing(
            OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
          )
          retrievedBlocks <- store.getBlockFromInclusive(BlockNumber.First)
        } yield {
          retrievedBlocks should contain only OutputBlockMetadata(
            EpochNumber.First,
            BlockNumber.First,
            blockBftTime = CantonTimestamp.Epoch,
          )
        }
      }

      "set initial lower bound for newly onboarded node" in {
        val store = createStore()
        val block1 = BlockNumber.First
        val epoch1 = EpochNumber.First

        val block2 = BlockNumber(BlockNumber.First + 1L)
        val epoch2 = EpochNumber(EpochNumber.First + 1L)

        for {
          lowerBound <- store.getLowerBound()
          _ = lowerBound shouldBe None

          result <- store.saveOnboardedNodeLowerBound(epoch1, block1)
          _ = result shouldBe (Right(()))

          // idempotent
          result <- store.saveOnboardedNodeLowerBound(epoch1, block1)
          _ = result shouldBe (Right(()))

          lowerBound <- store.getLowerBound()
          _ = lowerBound shouldBe Some(OutputMetadataStore.LowerBound(epoch1, block1))

          // can only be set once with this method
          result <- store.saveOnboardedNodeLowerBound(epoch2, block2)
          _ = result shouldBe (Left(
            "The initial lower bound for this node has already been set to LowerBound(0,0), so cannot set it to LowerBound(1,1)"
          ))

          // lower bound is unchanged
          lowerBound <- store.getLowerBound()
          _ = lowerBound shouldBe Some(OutputMetadataStore.LowerBound(epoch1, block1))
        } yield succeed
      }

      "prune" should {
        "prune epochs and blocks" in {
          val store = createStore()
          val block1 = createBlock(BlockNumber.First)
          val block2 = createBlock(1L, epochNumber = EpochNumber(1L))
          val block3 = createBlock(2L, epochNumber = EpochNumber(2L))

          val epoch1 = OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
          val epoch2 = OutputEpochMetadata(EpochNumber(1L), couldAlterOrderingTopology = true)
          val epoch3 = OutputEpochMetadata(EpochNumber(2L), couldAlterOrderingTopology = true)
          for {
            numberOfRecords0 <- store.loadNumberOfRecords
            _ = numberOfRecords0 shouldBe (OutputMetadataStore.NumberOfRecords.empty)

            _ <- store.insertEpochIfMissing(epoch1)
            _ <- store.insertBlockIfMissing(block1)

            numberOfRecords1 <- store.loadNumberOfRecords
            _ = numberOfRecords1 shouldBe (OutputMetadataStore.NumberOfRecords(
              epochs = 1L,
              blocks = 1L,
            ))

            _ <- store.insertBlockIfMissing(block2)
            _ <- store.insertBlockIfMissing(block3)
            _ <- store.insertEpochIfMissing(epoch2)
            _ <- store.insertEpochIfMissing(epoch3)

            numberOfRecords2 <- store.loadNumberOfRecords
            _ = numberOfRecords2 shouldBe (OutputMetadataStore.NumberOfRecords(
              epochs = 3L,
              blocks = 3L,
            ))

            lastBlock <- store.getLastConsecutiveBlock
            _ = lastBlock shouldBe Some(block3)

            pruningEpoch = EpochNumber(EpochNumber.First + 1)
            _ <- store.saveLowerBound(pruningEpoch)
            _ <- store.prune(pruningEpoch)

            lowerBound <- store.getLowerBound()
            numberOfRecordsAfterPruning <- store.loadNumberOfRecords

            _ = numberOfRecordsAfterPruning shouldBe (OutputMetadataStore.NumberOfRecords(
              epochs = 2L,
              blocks = 2L,
            ))
            _ = lowerBound shouldBe Some(
              OutputMetadataStore.LowerBound(pruningEpoch, BlockNumber(1L))
            )

            lastBlockAfterPruning <- store.getLastConsecutiveBlock
            _ = lastBlockAfterPruning shouldBe Some(block3)

          } yield succeed
        }
      }

    }
}

object OutputMetadataStoreTest {

  private def createBlock(
      blockNumber: Long,
      epochNumber: Long = EpochNumber.First,
      timestamp: CantonTimestamp = CantonTimestamp.Epoch,
  ) =
    OutputBlockMetadata(
      epochNumber = EpochNumber(epochNumber),
      blockNumber = BlockNumber(blockNumber),
      blockBftTime = timestamp,
    )
}
