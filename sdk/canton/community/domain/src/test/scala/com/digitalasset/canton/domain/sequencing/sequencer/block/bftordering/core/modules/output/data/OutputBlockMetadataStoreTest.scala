// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStoreTest.createBlock
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem.PekkoEnv
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

trait OutputBlockMetadataStoreTest extends AsyncWordSpec {
  this: AsyncWordSpec & BftSequencerBaseTest =>

  private[bftordering] def outputBlockMetadataStore(
      createStore: () => OutputBlockMetadataStore[PekkoEnv]
  ): Unit =
    "OutputBlockMetadataStore" should {

      "create and retrieve blocks" in {
        val store = createStore()
        val block = createBlock(BlockNumber.First)
        for {
          _ <- store.insertIfMissing(block)
          retrievedBlocks <- store.getFromInclusive(BlockNumber.First)
        } yield {
          retrievedBlocks should contain only OutputBlockMetadata(
            epochNumber = EpochNumber.First,
            blockNumber = BlockNumber.First,
            blockBftTime = CantonTimestamp.Epoch,
            epochCouldAlterSequencingTopology = true,
          )
        }
      }

      "allow adding blocks out of order" in {
        val store = createStore()
        val block1 = createBlock(BlockNumber.First)
        val block2 = createBlock(1L)

        for {
          _ <- store.insertIfMissing(block2)
          retrievedBlocks <- store.getFromInclusive(BlockNumber.First)
          _ = retrievedBlocks shouldBe empty

          _ <- store.insertIfMissing(block1)
          retrievedBlocks <- store.getFromInclusive(BlockNumber.First)

        } yield {
          retrievedBlocks should contain theSameElementsInOrderAs Seq(block1, block2)
        }
      }

      "can only insert once per block number" in {
        val store = createStore()
        val block = createBlock(BlockNumber.First)
        val wrongBlock = block.copy(blockBftTime = CantonTimestamp.MaxValue)
        for {
          _ <- store.insertIfMissing(block)
          _ <- store.insertIfMissing(block) // does nothing
          _ <- store.insertIfMissing(wrongBlock).failed // does nothing

          retrievedBlocks <- store.getFromInclusive(BlockNumber.First)
        } yield {
          retrievedBlocks should contain only block
        }
      }

      "get a block at or directly before a timestamp" in {
        val store = createStore()
        val block1 = createBlock(BlockNumber.First)
        val block2 = createBlock(1L, timestamp = CantonTimestamp.Epoch.plusMillis(1))
        val block3 = createBlock(2L, timestamp = CantonTimestamp.Epoch.plusMillis(3))
        val storeInit: Future[Unit] = for {
          _ <- store.insertIfMissing(block1)
          _ <- store.insertIfMissing(block2)
          _ <- store.insertIfMissing(block3)
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
              .getLatestAtOrBefore(timestamp)
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
          _ <- store.insertIfMissing(block1)
          _ <- store.insertIfMissing(block2)
          _ <- store.insertIfMissing(block3)
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
              .getFirstInEpoch(EpochNumber(epochNumber))
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
          _ <- store.insertIfMissing(block1)
          _ <- store.insertIfMissing(block2)
          _ <- store.insertIfMissing(block3)
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
              .getLastInEpoch(EpochNumber(epochNumber))
              .map(_.map(_.blockNumber) shouldBe blockNumber)
          }
        }
      }

      "get the last consecutive block in the store" when {

        "the store is empty" in {
          val store = createStore()
          store.getLastConsecutive.map(_ shouldBe empty)
        }

        "all blocks in the store are consecutive" in {
          val store = createStore()
          val block1 = createBlock(BlockNumber.First)
          val block2 = createBlock(1L)
          val storeInit: Future[Unit] = for {
            _ <- store.insertIfMissing(block1)
            _ <- store.insertIfMissing(block2)
          } yield ()

          storeInit.flatMap { _ =>
            store.getLastConsecutive
              .map(_.map(_.blockNumber) shouldBe Some(1L))
          }
        }

        "blocks in the store are consecutive only after an initial gap" in {
          val store = createStore()
          val block1 = createBlock(1L)
          val block2 = createBlock(2L)
          val storeInit: Future[Unit] = for {
            _ <- store.insertIfMissing(block1)
            _ <- store.insertIfMissing(block2)
          } yield ()

          storeInit.flatMap { _ =>
            store.getLastConsecutive
              .map(_.map(_.blockNumber) shouldBe None)
          }
        }

        "blocks in the store are consecutive only after a gap after the initial block" in {
          val store = createStore()
          val block1 = createBlock(BlockNumber.First)
          val block2 = createBlock(2L)
          val block3 = createBlock(3L)
          val storeInit: Future[Unit] = for {
            _ <- store.insertIfMissing(block1)
            _ <- store.insertIfMissing(block2)
            _ <- store.insertIfMissing(block3)
          } yield ()

          storeInit.flatMap { _ =>
            store.getLastConsecutive
              .map(_.map(_.blockNumber) shouldBe Some(BlockNumber.First))
          }
        }

        "blocks in the store are not all consecutive" in {
          val store = createStore()
          val block1 = createBlock(BlockNumber.First)
          val block2 = createBlock(1L)
          val block3 = createBlock(3L)
          val storeInit: Future[Unit] = for {
            _ <- store.insertIfMissing(block1)
            _ <- store.insertIfMissing(block2)
            _ <- store.insertIfMissing(block3)
          } yield ()

          storeInit.flatMap { _ =>
            store.getLastConsecutive
              .map(_.map(_.blockNumber) shouldBe Some(1L))
          }
        }
      }
    }
}

object OutputBlockMetadataStoreTest {

  private def createBlock(
      blockNumber: Long,
      epochNumber: Long = EpochNumber.First,
      timestamp: CantonTimestamp = CantonTimestamp.Epoch,
  ) =
    OutputBlockMetadata(
      epochNumber = EpochNumber(epochNumber),
      blockNumber = BlockNumber(blockNumber),
      blockBftTime = timestamp,
      epochCouldAlterSequencingTopology =
        true, // Set to true only to ensure that we can read back non-default values
    )
}
