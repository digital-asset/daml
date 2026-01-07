// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.scheduler.{Cron, PruningSchedule}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftOrderingModuleSystemInitializer.BftOrderingStores
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  EpochStoreReader,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.data.BftOrdererPruningSchedulerStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.pruning.{
  BftOrdererPruningSchedule,
  BftPruningStatus,
  PruningModule,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.OrderingRequestBatch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Pruning.{
  KickstartPruning,
  SchedulePruning,
}
import com.digitalasset.canton.time.{PositiveSeconds, SimClock}
import org.scalatest.wordspec.AnyWordSpec

import java.time.Instant
import scala.concurrent.Promise
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.Success

class PruningModuleTest extends AnyWordSpec with BftSequencerBaseTest {

  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))

  val latestBlock = OutputBlockMetadata(
    epochNumber = EpochNumber(100),
    blockNumber = BlockNumber(1000),
    blockBftTime = aTimestamp,
  )

  val retentionPeriod: FiniteDuration = 3.days
  val minNumberOfBlocksToKeep: Int = 10

  val blockAtEpoch50 =
    Some(OutputBlockMetadata(EpochNumber(50), BlockNumber(500), CantonTimestamp.Epoch))
  val blockAtEpoch40 =
    Some(OutputBlockMetadata(EpochNumber(40), BlockNumber(400), CantonTimestamp.Epoch))

  val pruningSchedule = BftOrdererPruningSchedule(
    PruningSchedule(
      Cron.tryCreate("* /10 * * * ? *"),
      PositiveSeconds.tryOfSeconds(1),
      PositiveSeconds.tryOfSeconds(30),
    ),
    minBlocksToKeep = 100,
  )

  private def moduleReadyToBePruned(): PruningModule[ProgrammableUnitTestEnv] = {
    val epochStore: EpochStore[ProgrammableUnitTestEnv] =
      mock[EpochStore[ProgrammableUnitTestEnv]]
    val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
      mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
    val availabilityStore: AvailabilityStore[ProgrammableUnitTestEnv] =
      mock[AvailabilityStore[ProgrammableUnitTestEnv]]

    when(epochStore.prune(EpochNumber(40))(traceContext)).thenReturn(() =>
      EpochStore.NumberOfRecords(10L, 10L, 0)
    )
    when(outputStore.prune(EpochNumber(40))(traceContext)).thenReturn(() =>
      OutputMetadataStore.NumberOfRecords(10L, 10L)
    )
    when(
      availabilityStore.prune(
        EpochNumber(40 - OrderingRequestBatch.BatchValidityDurationEpochs + 1L)
      )(traceContext)
    ).thenReturn(() => AvailabilityStore.NumberOfRecords(10L))

    when(outputStore.getLastConsecutiveBlock(traceContext)).thenReturn(() => Some(latestBlock))

    createPruningModule[ProgrammableUnitTestEnv](
      epochStore = epochStore,
      outputStore = outputStore,
      availabilityStore = availabilityStore,
    )
  }

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

      "do nothing on startup if no previous pruning point exists" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        when(outputStore.getLowerBound()(traceContext)).thenReturn(() => None)

        module.receiveInternal(Pruning.Start)

        context.runPipedMessages() shouldBe empty
      }

      "kickstart pruning by finding the latest block" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        when(outputStore.getLastConsecutiveBlock(traceContext)).thenReturn(() => Some(latestBlock))

        module.receiveInternal(
          Pruning.KickstartPruning(retentionPeriod, minNumberOfBlocksToKeep, None)
        )

        context.runPipedMessages() should contain only Pruning.ComputePruningPoint(
          latestBlock,
          retentionPeriod,
          minNumberOfBlocksToKeep,
        )
      }

      "compute pruning point based on retention period and min blocks to keep" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]

        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        val pruningTimestamp = latestBlock.blockBftTime.minus(retentionPeriod.toJava)
        when(outputStore.getLatestBlockAtOrBefore(pruningTimestamp)(traceContext))
          .thenReturn(() => blockAtEpoch50)

        val number = BlockNumber(latestBlock.blockNumber - minNumberOfBlocksToKeep)
        when(outputStore.getBlock(number)(traceContext)).thenReturn(() => blockAtEpoch40)

        module.receiveInternal(
          Pruning.ComputePruningPoint(latestBlock, retentionPeriod, minNumberOfBlocksToKeep)
        )

        context.runPipedMessages() should contain only Pruning.SaveNewLowerBound(EpochNumber(40))
      }

      "when missing pruning point from retentionPeriod, prune nothing" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]

        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        // missing
        val pruningTimestamp = latestBlock.blockBftTime.minus(retentionPeriod.toJava)
        when(outputStore.getLatestBlockAtOrBefore(pruningTimestamp)(traceContext))
          .thenReturn(() => None)

        val number = BlockNumber(latestBlock.blockNumber - minNumberOfBlocksToKeep)
        when(outputStore.getBlock(number)(traceContext)).thenReturn(() => blockAtEpoch50)

        module.receiveInternal(
          Pruning.ComputePruningPoint(latestBlock, retentionPeriod, minNumberOfBlocksToKeep)
        )

        context.runPipedMessages() shouldBe empty
      }

      "when missing pruning point from minNumberOfBlocksToKeep, prune nothing" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]

        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        val pruningTimestamp = latestBlock.blockBftTime.minus(retentionPeriod.toJava)
        when(outputStore.getLatestBlockAtOrBefore(pruningTimestamp)(traceContext))
          .thenReturn(() => blockAtEpoch50)

        // missing
        val number = BlockNumber(latestBlock.blockNumber - minNumberOfBlocksToKeep)
        when(outputStore.getBlock(number)(traceContext)).thenReturn(() => None)

        module.receiveInternal(
          Pruning.ComputePruningPoint(latestBlock, retentionPeriod, minNumberOfBlocksToKeep)
        )

        context.runPipedMessages() shouldBe empty
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
        val module = moduleReadyToBePruned()
        module.receiveInternal(Pruning.PerformPruning(EpochNumber(40)))
        context.runPipedMessages() shouldBe empty
      }

      "schedule pruning should do nothing if pruning schedule has not been initialized" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val module = createPruningModule[ProgrammableUnitTestEnv]()
        module.receiveInternal(Pruning.SchedulePruning)
        context.lastDelayedMessage shouldBe empty
      }

      "should schedule when initializing schedule and reschedule when receiving schedule message" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val module = createPruningModule[ProgrammableUnitTestEnv]()
        module.receiveInternal(Pruning.StartPruningSchedule(pruningSchedule))
        context.lastDelayedMessage should contain((1, KickstartPruning(30 seconds, 100, None)))

        module.receiveInternal(Pruning.SchedulePruning)
        context.lastDelayedMessage should contain((2, KickstartPruning(30 seconds, 100, None)))
      }

      "schedule pruning after performing pruning on a schedule" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val module = moduleReadyToBePruned()

        module.receiveInternal(Pruning.StartPruningSchedule(pruningSchedule))
        context.lastDelayedMessage should contain((1, KickstartPruning(30 seconds, 100, None)))
        // kick-starting pruning without a promise indicates this is a scheduled operation
        module.receiveInternal(KickstartPruning(30 seconds, 100, None))
        module.receiveInternal(Pruning.PerformPruning(EpochNumber(40)))

        context.runPipedMessages() should contain(SchedulePruning)
      }

      "grpc requested pruning should complete the request promise with the operation description" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val epochStore: EpochStore[ProgrammableUnitTestEnv] =
          mock[EpochStore[ProgrammableUnitTestEnv]]
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
        val availabilityStore: AvailabilityStore[ProgrammableUnitTestEnv] =
          mock[AvailabilityStore[ProgrammableUnitTestEnv]]
        val module = createPruningModule[ProgrammableUnitTestEnv](
          epochStore = epochStore,
          outputStore = outputStore,
          availabilityStore = availabilityStore,
        )

        when(outputStore.getLastConsecutiveBlock(traceContext)).thenReturn(() => Some(latestBlock))

        val requestPromise = Promise[String]()

        module.receiveInternal(
          Pruning.KickstartPruning(retentionPeriod, minNumberOfBlocksToKeep, Some(requestPromise))
        )

        context.runPipedMessages() should contain only Pruning.ComputePruningPoint(
          latestBlock,
          retentionPeriod,
          minNumberOfBlocksToKeep,
        )

        when(epochStore.prune(EpochNumber(40))(traceContext)).thenReturn(() =>
          EpochStore.NumberOfRecords(10L, 10L, 0)
        )
        when(outputStore.prune(EpochNumber(40))(traceContext)).thenReturn(() =>
          OutputMetadataStore.NumberOfRecords(10L, 10L)
        )
        when(
          availabilityStore.prune(
            EpochNumber(40 - OrderingRequestBatch.BatchValidityDurationEpochs + 1L)
          )(traceContext)
        ).thenReturn(() => AvailabilityStore.NumberOfRecords(10L))

        module.receiveInternal(Pruning.PerformPruning(EpochNumber(40)))
        context.runPipedMessages() shouldBe empty

        requestPromise.isCompleted shouldBe true
        requestPromise.future.value should contain(
          Success(
            """Pruning at epoch 40 complete.
            |EpochStore: pruned 10 epochs, 10 pbft messages.
            |OutputStore: pruned 10 epochs and 10 blocks.
            |AvailabilityStore: pruned 10 batches.""".stripMargin
          )
        )
      }

      "not allow concurrent pruning operations" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val module = createPruningModule[ProgrammableUnitTestEnv]()

        val requestPromise = Promise[String]()
        module.receiveInternal(
          Pruning.KickstartPruning(retentionPeriod, minNumberOfBlocksToKeep, Some(requestPromise))
        )
        requestPromise.isCompleted shouldBe (false)

        val requestPromise2 = Promise[String]()
        module.receiveInternal(
          Pruning.KickstartPruning(retentionPeriod, minNumberOfBlocksToKeep, Some(requestPromise2))
        )

        requestPromise2.future.value should contain(
          Success(
            "Pruning won't continue because an operation is already taking place"
          )
        )
      }
    }

    "requesting status" should {
      "complete the request promise with the pruning lower bound" in {
        implicit val context: ProgrammableUnitTestContext[Pruning.Message] =
          new ProgrammableUnitTestContext()
        val outputStore: OutputMetadataStore[ProgrammableUnitTestEnv] =
          mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
        val module = createPruningModule[ProgrammableUnitTestEnv](outputStore = outputStore)

        val lowerBound = OutputMetadataStore.LowerBound(EpochNumber(5L), BlockNumber(50L))
        when(outputStore.getLowerBound()(traceContext)).thenReturn(() => Some(lowerBound))
        when(outputStore.getLastConsecutiveBlock(traceContext)).thenReturn(() => Some(latestBlock))

        val requestPromise = Promise[BftPruningStatus]()
        module.receiveInternal(Pruning.PruningStatusRequest(requestPromise))
        context.runPipedMessages() shouldBe empty

        requestPromise.isCompleted shouldBe true
        requestPromise.future.value should contain(
          Success(
            BftPruningStatus(
              latestBlock.epochNumber,
              latestBlock.blockNumber,
              latestBlock.blockBftTime,
              EpochNumber(5L),
              BlockNumber(50L),
            )
          )
        )
      }
    }
  }

  private def createPruningModule[E <: Env[E]](
      epochStore: EpochStore[E] = mock[EpochStore[E]],
      outputStore: OutputMetadataStore[E] = mock[OutputMetadataStore[E]],
      availabilityStore: AvailabilityStore[E] = mock[AvailabilityStore[E]],
  ): PruningModule[E] = {
    val stores = BftOrderingStores[E](
      mock[P2PEndpointsStore[E]],
      availabilityStore,
      epochStore,
      mock[EpochStoreReader[E]],
      outputStore,
      mock[BftOrdererPruningSchedulerStore[E]],
    )
    val pruning = new PruningModule[E](
      stores,
      new SimClock(loggerFactory = loggerFactory),
      loggerFactory,
      timeouts,
    )
    pruning
  }

}
