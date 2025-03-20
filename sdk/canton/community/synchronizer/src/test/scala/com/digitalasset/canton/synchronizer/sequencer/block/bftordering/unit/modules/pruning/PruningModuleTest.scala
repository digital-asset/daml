// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.pruning

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig.PruningConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  EpochStoreReader,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.PruningModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.{
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps

class PruningModuleTest extends AnyWordSpec with BftSequencerBaseTest {

  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))

  val latestBlock = OutputBlockMetadata(
    epochNumber = EpochNumber(100),
    blockNumber = BlockNumber(1000),
    blockBftTime = aTimestamp,
  )

  "PruningModule" when {
    "performing pruning" should {
      "try to perform pruning with latest pruning point when starting module" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        when(outputStore.getLowerBound()(traceContext)).thenReturn(() =>
          Some(OutputMetadataStore.LowerBound(EpochNumber(10), BlockNumber(10)))
        )

        module.receiveInternal(Pruning.Start)

        context.runPipedMessages() should contain only Pruning.PerformPruning(EpochNumber(10))
      }

      "schedule pruning if no previous pruning point exists" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        when(outputStore.getLowerBound()(traceContext)).thenReturn(() => None)

        module.receiveInternal(Pruning.Start)

        context.runPipedMessages() should contain only Pruning.SchedulePruning
      }

      "kickstart pruning by finding the latest block" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        when(outputStore.getLastConsecutiveBlock(traceContext)).thenReturn(() => Some(latestBlock))

        module.receiveInternal(Pruning.KickstartPruning)

        context.runPipedMessages() should contain only Pruning.ComputePruningPoint(latestBlock)
      }

      "compute pruning point based on retention period and min blocks to keep" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]

        val retentionPeriod: FiniteDuration = 3.days
        val minNumberOfBlocksToKeep: Int = 10

        val module = createPruningModule[ProgrammableUnitTestEnv](
          retentionPeriod,
          minNumberOfBlocksToKeep,
          outputStore = outputStore,
        )

        val blockAtEpoch50 =
          Some(OutputBlockMetadata(EpochNumber(50), BlockNumber(500), CantonTimestamp.Epoch))
        val blockAtEpoch40 =
          Some(OutputBlockMetadata(EpochNumber(40), BlockNumber(400), CantonTimestamp.Epoch))

        val pruningTimestamp = latestBlock.blockBftTime.minus(retentionPeriod.toJava)
        when(outputStore.getLatestBlockAtOrBefore(pruningTimestamp)(traceContext))
          .thenReturn(() => blockAtEpoch50)

        val number = BlockNumber(latestBlock.blockNumber - minNumberOfBlocksToKeep)
        when(outputStore.getBlock(number)(traceContext)).thenReturn(() => blockAtEpoch40)

        module.receiveInternal(Pruning.ComputePruningPoint(latestBlock))

        context.runPipedMessages() should contain only Pruning.SaveNewLowerBound(EpochNumber(40))
      }

      "save new lower bound" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]

        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        when(outputStore.saveLowerBound(EpochNumber(40))(traceContext)).thenReturn(() => Right(()))

        module.receiveInternal(Pruning.SaveNewLowerBound(EpochNumber(40)))
        context.runPipedMessages() should contain only Pruning.PerformPruning(EpochNumber(40))
      }

      "perform pruning by pruning stores" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val epochStore: EpochStore[ProgrammableUnitTestEnv] =
          mock[EpochStore[ProgrammableUnitTestEnv]]
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]

        val module = createPruningModule[ProgrammableUnitTestEnv](
          epochStore = epochStore,
          outputStore = outputStore,
        )

        when(epochStore.prune(EpochNumber(40))(traceContext)).thenReturn(() =>
          EpochStore.NumberOfRecords(10L, 10L, 0)
        )
        when(outputStore.prune(EpochNumber(40))(traceContext)).thenReturn(() =>
          OutputMetadataStore.NumberOfRecords(10L, 10L)
        )

        module.receiveInternal(Pruning.PerformPruning(EpochNumber(40)))
        context.runPipedMessages() should contain only Pruning.SchedulePruning
      }

      "schedule pruning" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val module = createPruningModule[ProgrammableUnitTestEnv]()
        module.receiveInternal(Pruning.SchedulePruning)
        context.lastDelayedMessage should contain((1, Pruning.KickstartPruning))
      }

    }
  }

  private def createPruningModule[E <: Env[E]](
      retentionPeriod: FiniteDuration = 30.days,
      minNumberOfBlocksToKeep: Int = 100,
      pruningFrequency: FiniteDuration = 1.hour,
      epochStore: EpochStore[E] = mock[EpochStore[E]],
      outputStore: OutputMetadataStore[E] = mock[OutputMetadataStore[E]],
  ): PruningModule[E] = {
    val stores = BftOrderingStores[E](
      mock[P2PEndpointsStore[E]],
      mock[AvailabilityStore[E]],
      epochStore,
      mock[EpochStoreReader[E]],
      outputStore,
    )
    val pruning = new PruningModule[E](
      PruningConfig(retentionPeriod, minNumberOfBlocksToKeep, pruningFrequency),
      stores,
      loggerFactory,
      timeouts,
    )
    pruning
  }

}
