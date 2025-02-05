// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.block.BlockFormat.OrderedRequest
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.OrderedBlocksReader
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.GenericInMemoryEpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.{
  DefaultRequestInspector,
  RequestInspector,
  StartupState,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.memory.GenericInMemoryOutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.{
  OutputModule,
  PekkoBlockSubscription,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequest,
  OrderingRequestBatch,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Output,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  BlockSubscription,
  EmptyBlockSubscription,
  ModuleRef,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext}
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.Sink
import org.mockito.Mockito.clearInvocations
import org.scalatest.wordspec.AsyncWordSpecLike

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.jdk.DurationConverters.*
import scala.util.Try

import OutputMetadataStore.{OutputBlockMetadata, OutputEpochMetadata}
import BftTime.MinimumBlockTimeGranularity

class OutputModuleTest
    extends AsyncWordSpecLike
    with BaseTest
    with HasActorSystem
    with HasExecutionContext {

  import OutputModuleTest.*

  "OutputModule" should {
    val initialBlock = anOrderedBlockForOutput()

    "fetch block from availability" when {
      "a block hasn't been provided yet and is not being fetched" in {
        implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
          IgnoringUnitTestContext()

        val store = createOutputMetadataStore[IgnoringUnitTestEnv]
        val availabilityRef = mock[ModuleRef[Availability.Message[IgnoringUnitTestEnv]]]
        val output =
          createOutputModule[IgnoringUnitTestEnv](
            store = store,
            availabilityRef = availabilityRef,
          )()

        output.receive(Output.Start)
        output.receive(Output.BlockOrdered(initialBlock))

        verify(availabilityRef, times(1)).asyncSend(
          Availability.LocalOutputFetch.FetchBlockData(initialBlock)
        )
        succeed
      }
    }

    "do nothing" when {
      "a block hasn't been provided yet but is already being fetched" in {
        implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
          IgnoringUnitTestContext()

        val store = createOutputMetadataStore[IgnoringUnitTestEnv]
        val availabilityRef = mock[ModuleRef[Availability.Message[IgnoringUnitTestEnv]]]
        val output =
          createOutputModule[IgnoringUnitTestEnv](
            store = store,
            availabilityRef = availabilityRef,
          )()

        output.receive(Output.Start)
        output.receive(Output.BlockOrdered(initialBlock))

        verify(availabilityRef, times(1)).asyncSend(
          Availability.LocalOutputFetch.FetchBlockData(initialBlock)
        )

        clearInvocations(availabilityRef)
        output.receive(Output.BlockOrdered(initialBlock))

        verify(availabilityRef, never).asyncSend(
          Availability.LocalOutputFetch.FetchBlockData(initialBlock)
        )
        succeed
      }

      "a block has already been provided" in {
        implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
          IgnoringUnitTestContext()

        val store = createOutputMetadataStore[IgnoringUnitTestEnv]
        val availabilityRef = mock[ModuleRef[Availability.Message[IgnoringUnitTestEnv]]]
        val output =
          createOutputModule[IgnoringUnitTestEnv](
            store = store,
            availabilityRef = availabilityRef,
          )()

        output.receive(Output.Start)
        output.receive(Output.BlockOrdered(initialBlock))
        output.receiveInternal(Output.BlockDataFetched(CompleteBlockData(initialBlock, Seq.empty)))

        clearInvocations(availabilityRef)
        output.receive(Output.BlockOrdered(initialBlock))

        verify(availabilityRef, never).asyncSend(
          Availability.LocalOutputFetch.FetchBlockData(initialBlock)
        )
        succeed
      }
    }

    "store ordered block" in {
      val cell =
        new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](
          None
        )
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cell)

      val store = createOutputMetadataStore[FakePipeToSelfCellUnitTestEnv]
      val output = createOutputModule[FakePipeToSelfCellUnitTestEnv](store = store)()

      val completeBlockData = CompleteBlockData(initialBlock, batches = Seq.empty)

      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(completeBlockData))
      cell.get().getOrElse(fail("BlockDataStored not received")).apply() // store block

      val blocks =
        store.getBlockFromInclusive(initialBlockNumber = BlockNumber.First)(
          traceContext
        )()
      blocks.size shouldBe 1
      assertBlock(
        blocks.head,
        expectedBlockNumber = BlockNumber.First,
        expectedTimestamp = aTimestamp,
      )
    }

    "store 2 blocks out of order" in {
      implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext(resolveAwaits = true)

      val store = createOutputMetadataStore[ProgrammableUnitTestEnv]
      val output =
        createOutputModule[ProgrammableUnitTestEnv](store = store)()

      val outOfOrderBlockData =
        CompleteBlockData(
          anOrderedBlockForOutput(blockNumber = secondBlockNumber),
          batches = Seq.empty,
        )
      val initialBlockData = CompleteBlockData(initialBlock, batches = Seq.empty)

      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(outOfOrderBlockData))
      context.sizeOfPipedMessages shouldBe 0

      val emptyBlocks =
        store.getBlockFromInclusive(initialBlockNumber = BlockNumber.First)(
          traceContext
        )()
      output.receive(Output.BlockDataFetched(initialBlockData))
      context.sizeOfPipedMessages shouldBe 2
      context.runPipedMessagesAndReceiveOnModule(output) // store blocks

      val blocks =
        store.getBlockFromInclusive(initialBlockNumber = BlockNumber.First)(
          traceContext
        )()

      emptyBlocks should be(empty)
      blocks.size shouldBe 2

      assertBlock(
        blocks.head,
        expectedBlockNumber = BlockNumber.First,
        expectedTimestamp = aTimestamp,
      )
      assertBlock(
        blocks(1),
        expectedBlockNumber = secondBlockNumber,
        expectedTimestamp = aTimestamp.add(MinimumBlockTimeGranularity.toJava),
      )
    }

    "restart with a non-zero initial block number" in {
      implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext(resolveAwaits = true)

      val store = createOutputMetadataStore[ProgrammableUnitTestEnv]
      val output =
        createOutputModule[ProgrammableUnitTestEnv](store = store)()
      val completeInitialBlock = CompleteBlockData(initialBlock, batches = Seq.empty)

      // To test that the epoch metadata store is correctly loaded on restart
      store
        .insertEpochIfMissing(
          OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
        )
        .apply()

      // Start the output module with an empty store and simulate the arrival of block data and store it.
      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(completeInitialBlock))
      context.runPipedMessagesAndReceiveOnModule(output) // store block

      // Restart the output module with a non-zero initial block number.
      val availabilityCell =
        new AtomicReference[Option[Availability.Message[ProgrammableUnitTestEnv]]](None)
      val orderedBlocksReader = mock[OrderedBlocksReader[ProgrammableUnitTestEnv]]
      when(
        orderedBlocksReader.loadOrderedBlocks(initialBlockNumber = BlockNumber.First)(traceContext)
      ).thenReturn(() => Seq(initialBlock))
      val outputAfterRestart =
        createOutputModule[ProgrammableUnitTestEnv](
          initialHeight = secondBlockNumber,
          availabilityRef = fakeCellModule(availabilityCell),
          store = store,
          orderedBlocksReader = orderedBlocksReader,
        )()
      outputAfterRestart.receive(Output.Start)

      // The output module should rehydrate the pending topology changes flag
      outputAfterRestart.currentEpochCouldAlterOrderingTopology shouldBe true

      // The output module should re-process the last stored block and fetch its data from availability.
      context.selfMessages should contain only Output.BlockOrdered(initialBlock)
      context.selfMessages.foreach(outputAfterRestart.receive)
      availabilityCell.get().getOrElse(fail("Availability not requested for block data")) shouldBe
        Availability.LocalOutputFetch.FetchBlockData(initialBlock)

      // Simulate the arrival of the next block's data and store it (idempotent).
      outputAfterRestart.receive(Output.BlockDataFetched(completeInitialBlock))
      val nextBlockData =
        CompleteBlockData(
          anOrderedBlockForOutput(blockNumber = secondBlockNumber),
          batches = Seq.empty,
        )
      outputAfterRestart.receive(Output.BlockDataFetched(nextBlockData))
      context.runPipedMessagesAndReceiveOnModule(output) // store block

      // The store should contain both blocks.
      val blocks =
        store.getBlockFromInclusive(initialBlockNumber = BlockNumber.First)(
          traceContext
        )()
      blocks.size shouldBe 2
      assertBlock(
        blocks.head,
        expectedBlockNumber = BlockNumber.First,
        expectedTimestamp = aTimestamp,
      )
      assertBlock(
        blocks(1),
        expectedBlockNumber = secondBlockNumber,
        expectedTimestamp = aTimestamp.add(MinimumBlockTimeGranularity.toJava),
      )
    }

    "restart from the correct block" when {
      val recoverFromBlockNumber = BlockNumber(secondBlockNumber)

      "it is behind the subscription" in {
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)

        val store = mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
        when(store.getEpoch(EpochNumber.First)(traceContext)).thenReturn(() =>
          Some(
            OutputEpochMetadata(
              epochNumber = EpochNumber.First,
              couldAlterOrderingTopology = true,
            )
          )
        )
        when(store.getLastConsecutiveBlock(traceContext)).thenReturn(() =>
          Some(
            OutputBlockMetadata(
              epochNumber = secondEpochNumber,
              blockNumber = recoverFromBlockNumber,
              blockBftTime = aTimestamp,
            )
          )
        )
        val lastStoredBlock = anOrderedBlockForOutput(blockNumber = recoverFromBlockNumber)
        // The output module will recover from the last stored block (< last acknowledged block) to rebuilt its
        //  volatile state, and it will then wait for further blocks from consensus.
        val orderedBlocksReader = mock[OrderedBlocksReader[ProgrammableUnitTestEnv]]
        when(
          orderedBlocksReader.loadOrderedBlocks(initialBlockNumber = recoverFromBlockNumber)(
            traceContext
          )
        ).thenReturn(() => Seq(lastStoredBlock))
        // The previous block's BFT time will be rehydrated for BFT time computation.
        val previousStoredBlockNumber = BlockNumber(recoverFromBlockNumber - 1)
        val previousStoredBlockBftTime = aTimestamp.minusSeconds(1)
        when(store.getBlock(previousStoredBlockNumber)(traceContext)).thenReturn(() =>
          Some(
            OutputBlockMetadata(
              secondEpochNumber,
              previousStoredBlockNumber,
              previousStoredBlockBftTime,
            )
          )
        )
        val availabilityCell =
          new AtomicReference[Option[Availability.Message[ProgrammableUnitTestEnv]]](None)
        val output = createOutputModule[ProgrammableUnitTestEnv](
          initialHeight = 2L,
          availabilityRef = fakeCellModule(availabilityCell),
          store = store,
          orderedBlocksReader = orderedBlocksReader,
        )()
        output.receive(Output.Start)

        verify(store, times(1)).getLastConsecutiveBlock(traceContext)
        verify(orderedBlocksReader, times(1))
          .loadOrderedBlocks(initialBlockNumber = recoverFromBlockNumber)(
            traceContext
          )
        context.selfMessages should contain only Output.BlockOrdered(lastStoredBlock)
        output.previousStoredBlock.getBlockNumberAndBftTime should contain(
          previousStoredBlockNumber -> previousStoredBlockBftTime
        )
        output.currentEpochCouldAlterOrderingTopology shouldBe true
      }

      "it is ahead of the subscription" in {
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)
        val lastStoredBlock = anOrderedBlockForOutput(blockNumber = recoverFromBlockNumber)
        val store = mock[OutputMetadataStore[ProgrammableUnitTestEnv]]
        when(store.getEpoch(EpochNumber.First)(traceContext)).thenReturn(() =>
          Some(
            OutputEpochMetadata(
              epochNumber = EpochNumber.First,
              couldAlterOrderingTopology = true,
            )
          )
        )
        when(store.getLastConsecutiveBlock(traceContext)).thenReturn(() =>
          Some(
            OutputBlockMetadata(
              epochNumber = secondEpochNumber,
              blockNumber = BlockNumber(3L),
              blockBftTime = aTimestamp,
            )
          )
        )
        val orderedBlocksReader = mock[OrderedBlocksReader[ProgrammableUnitTestEnv]]
        // The output module will recover from the last acknowledged block = initial height - 1 (< last stored block)
        //  to rebuilt its volatile state and let the subscription catch up.
        when(
          orderedBlocksReader.loadOrderedBlocks(initialBlockNumber = recoverFromBlockNumber)(
            traceContext
          )
        )
          .thenReturn(() => Seq(lastStoredBlock))
        // The previous block's BFT time will be rehydrated for BFT time computation.
        val previousStoredBlockNumber = BlockNumber(recoverFromBlockNumber - 1)
        val previousStoredBlockBftTime = aTimestamp.minusSeconds(1)
        when(store.getBlock(previousStoredBlockNumber)(traceContext)).thenReturn(() =>
          Some(
            OutputBlockMetadata(
              secondEpochNumber,
              previousStoredBlockNumber,
              previousStoredBlockBftTime,
            )
          )
        )
        val availabilityCell =
          new AtomicReference[Option[Availability.Message[ProgrammableUnitTestEnv]]](None)
        val output = createOutputModule[ProgrammableUnitTestEnv](
          initialHeight = 2L,
          availabilityRef = fakeCellModule(availabilityCell),
          store = store,
          orderedBlocksReader = orderedBlocksReader,
        )()
        output.receive(Output.Start)

        verify(store, times(1)).getLastConsecutiveBlock(traceContext)
        verify(orderedBlocksReader, times(1))
          .loadOrderedBlocks(initialBlockNumber = recoverFromBlockNumber)(
            traceContext
          )
        context.selfMessages should contain only Output.BlockOrdered(lastStoredBlock)
        output.previousStoredBlock.getBlockNumberAndBftTime should contain(
          previousStoredBlockNumber -> previousStoredBlockBftTime
        )
        output.currentEpochCouldAlterOrderingTopology shouldBe true
      }
    }

    "allow subscription from the initial block" in {
      val cell =
        new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](
          None
        )
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cell)

      val store = createOutputMetadataStore[FakePipeToSelfCellUnitTestEnv]
      val blockSubscription =
        new PekkoBlockSubscription[FakePipeToSelfCellUnitTestEnv](
          initialHeight = BlockNumber.First,
          timeouts,
          loggerFactory,
        )(fail(_))
      val output =
        createOutputModule[FakePipeToSelfCellUnitTestEnv](store = store)(
          blockSubscription
        )

      val initialBlockData =
        CompleteBlockData(
          initialBlock,
          batches = Seq(
            OrderingRequestBatch(
              Seq(Traced(OrderingRequest(aTag, ByteString.EMPTY)))
            )
          ).map(x => BatchId.from(x) -> x),
        )
      val nextBlockData =
        CompleteBlockData(
          anOrderedBlockForOutput(blockNumber = secondBlockNumber),
          batches = Seq.empty,
        )
      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(initialBlockData))
      output.receive(
        cell
          .get()
          .getOrElse(fail("BlockDataStored not received"))()
          .getOrElse(fail("Callback didn't send message"))
      )
      output.receive(Output.BlockDataFetched(nextBlockData))
      output.receive(
        cell
          .get()
          .getOrElse(fail("BlockDataStored not received"))()
          .getOrElse(fail("Callback didn't send message"))
      )

      blockSubscription.subscription().take(2).runWith(Sink.seq).map { blocks =>
        blocks.size shouldBe 2
        val initialBlock = blocks.head
        initialBlock.blockHeight shouldBe BlockNumber.First
        initialBlock.requests.size shouldBe 1
        initialBlock.requests.head shouldBe Traced(
          OrderedRequest(aTimestamp.toMicros, aTag, ByteString.EMPTY)
        )
        val nextBlock = blocks(1)
        nextBlock.blockHeight shouldBe secondBlockNumber
        nextBlock.requests should be(empty)
      }
    }

    "allow subscription from a non-0 height" in {
      val cell =
        new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](
          None
        )
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cell)

      val store = createOutputMetadataStore[FakePipeToSelfCellUnitTestEnv]

      val blockSubscription =
        new PekkoBlockSubscription[FakePipeToSelfCellUnitTestEnv](
          secondBlockNumber,
          timeouts,
          loggerFactory,
        )(fail(_))
      val output = createOutputModule[FakePipeToSelfCellUnitTestEnv](
        store = store,
        initialHeight = secondBlockNumber,
      )(blockSubscription)

      val initialBlockData = CompleteBlockData(initialBlock, batches = Seq.empty)
      val nextBlockData =
        CompleteBlockData(
          anOrderedBlockForOutput(blockNumber = secondBlockNumber),
          batches = Seq.empty,
        )
      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(initialBlockData))
      cell.get().getOrElse(fail("BlockDataStored not received")).apply() // store block
      output.receive(Output.BlockDataFetched(nextBlockData))
      output.receive(
        cell
          .get()
          .getOrElse(fail("BlockDataStored not received"))()
          .getOrElse(fail("didn't send message"))
      )

      blockSubscription.subscription().runWith(Sink.head).map { block =>
        block.blockHeight shouldBe secondBlockNumber
        block.requests should be(empty)
      }
    }

    "not send a block upstream if the number is lower than the initial height" in {
      implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
        IgnoringUnitTestContext()

      val store = createOutputMetadataStore[IgnoringUnitTestEnv]

      val initialHeight = BlockNumber(2L)
      val blockSubscription =
        new PekkoBlockSubscription[IgnoringUnitTestEnv](initialHeight, timeouts, loggerFactory)(
          fail(_)
        )
      val output = createOutputModule[IgnoringUnitTestEnv](
        store = store,
        initialHeight = initialHeight,
      )(blockSubscription)
      output.receive(Output.Start)

      val blocks =
        (secondBlockNumber to initialHeight)
          .map(blockNumber =>
            anOrderedBlockForOutput(
              blockNumber = blockNumber,
              commitTimestamp = CantonTimestamp.MinValue, /* irrelevant for the test */
            )
          )
          .map(CompleteBlockData(_, batches = Seq.empty))
      blocks.foreach(blockData =>
        output.receive(
          Output.BlockDataStored(
            blockData,
            blockData.orderedBlockForOutput.orderedBlock.metadata.blockNumber,
            CantonTimestamp.MinValue, // irrelevant for the test case
            epochCouldAlterOrderingTopology = false, // irrelevant for the test case
          )
        )
      )

      blockSubscription.subscription().runWith(Sink.head).map { block =>
        block.blockHeight shouldBe initialHeight
        block.requests should be(empty)
      }
    }

    "set the potential topology change flag to `true`, " +
      "request to tick the topology, " +
      "fetch a new topology, " +
      "if not in the middle of state transfer send the topology to consensus and " +
      "set up the new topology including the potential topology changes flag if pending changes are reported" when {

        "at least one block in the just completed epoch has requests to all members of synchronizer" in {
          val topologyActivationTime = TopologyActivationTime(anotherTimestamp.immediateSuccessor)

          Table(
            ("Pending Canton topology changes", "first block mode", "last block mode"),
            (
              false,
              OrderedBlockForOutput.Mode.FromConsensus,
              OrderedBlockForOutput.Mode.FromConsensus,
            ),
            (
              true,
              OrderedBlockForOutput.Mode.FromConsensus,
              OrderedBlockForOutput.Mode.FromConsensus,
            ),
            (
              false,
              OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock,
              OrderedBlockForOutput.Mode.StateTransfer.LastBlock,
            ),
            (
              true,
              OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock,
              OrderedBlockForOutput.Mode.StateTransfer.LastBlock,
            ),
            (
              false,
              OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock,
              OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock,
            ),
            (
              true,
              OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock,
              OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock,
            ),
          ).forEvery { case (pendingChanges, firstBlockMode, lastBlockMode) =>
            val store = spy(createOutputMetadataStore[ProgrammableUnitTestEnv])
            val topologyProviderMock = mock[OrderingTopologyProvider[ProgrammableUnitTestEnv]]
            val consensusRef = mock[ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]]]
            val newOrderingTopology =
              OrderingTopology(
                peers = Set.empty,
                SequencingParameters.Default,
                topologyActivationTime,
                areTherePendingCantonTopologyChanges = pendingChanges,
              )
            val newCryptoProvider = fakeCryptoProvider[ProgrammableUnitTestEnv]
            when(topologyProviderMock.getOrderingTopologyAt(topologyActivationTime))
              .thenReturn(() => Some((newOrderingTopology, newCryptoProvider)))
            val subscriptionBlocks = mutable.Queue.empty[BlockFormat.Block]
            val output = createOutputModule[ProgrammableUnitTestEnv](
              store = store,
              consensusRef = consensusRef,
              orderingTopologyProvider = topologyProviderMock,
              requestInspector = new AccumulatingRequestInspector,
            )(blockSubscription = new EnqueueingBlockSubscription(subscriptionBlocks))
            implicit val context
                : ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
              new ProgrammableUnitTestContext(resolveAwaits = true)

            val blockData1 = // lastInEpoch = false, isRequestToAllMembersOfSynchronizer = true
              completeBlockData(
                BlockNumber.First,
                commitTimestamp = aTimestamp,
                mode = firstBlockMode,
              )
            val blockData2 = // lastInEpoch = true, isRequestToAllMembersOfSynchronizer = false
              completeBlockData(
                BlockNumber(BlockNumber.First + 1L),
                commitTimestamp = anotherTimestamp,
                lastInEpoch = true,
                mode = lastBlockMode,
              )

            output.receive(Output.Start)
            output.receive(Output.BlockDataFetched(blockData1))

            val piped1 = context.runPipedMessages()
            piped1 should contain only Output.BlockDataStored(
              blockData1,
              BlockNumber.First,
              aTimestamp,
              epochCouldAlterOrderingTopology = true,
            )
            piped1.foreach(output.receive) // Store first block's metadata

            output.currentEpochCouldAlterOrderingTopology shouldBe true

            output.receive(Output.BlockDataFetched(blockData2))

            val piped2 = context.runPipedMessages()
            piped2 should contain only Output.BlockDataStored(
              blockData2,
              BlockNumber(1L),
              anotherTimestamp,
              epochCouldAlterOrderingTopology = true,
            )
            piped2.foreach(output.receive) // Store last block's metadata

            // The epoch metadata should be stored only once, i.e.,
            //  only for the first block after epochCouldAlterOrderingTopology is set
            verify(store, times(1)).insertEpochIfMissing(
              OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
            )

            val shouldSendTopologyToConsensus = lastBlockMode.mustSendTopologyToConsensus
            val piped3 = context.runPipedMessages()
            piped3 should contain only Output.TopologyFetched(
              shouldSendTopologyToConsensus,
              EpochNumber(1L), // Epoch number
              newOrderingTopology,
              newCryptoProvider,
            )

            // All blocks have now been output to the subscription
            subscriptionBlocks should have size 2
            val block1 = subscriptionBlocks.dequeue()
            block1.blockHeight shouldBe BlockNumber.First
            block1.tickTopologyAtMicrosFromEpoch shouldBe None
            val block2 = subscriptionBlocks.dequeue()
            block2.blockHeight shouldBe BlockNumber(1)
            // We should tick even during state transfer if the epoch has potential sequencer topology changes
            block2.tickTopologyAtMicrosFromEpoch shouldBe Some(anotherTimestamp.toMicros)

            verify(topologyProviderMock, times(1)).getOrderingTopologyAt(topologyActivationTime)
            // Update the last block if needed and set up the new topology
            piped3.foreach(output.receive)

            if (pendingChanges) { // Then the last block will be updated
              val piped4 = context.runPipedMessages()
              piped4 should matchPattern {
                case Seq(
                      Output.MetadataStoredForNewEpoch(
                        `shouldSendTopologyToConsensus`,
                        1L, // Epoch number
                        `newOrderingTopology`,
                        _, // A fake crypto provider instance
                      )
                    ) =>
              }
              piped4.foreach(output.receive)
            }

            // The topology alteration flag should be set if needed
            output.currentEpochCouldAlterOrderingTopology shouldBe pendingChanges

            if (output.currentEpochCouldAlterOrderingTopology) {
              // Store epoch metadata
              val piped5 = context.runPipedMessages()
              piped5 should be(empty)
            }

            // We should send a new ordering topology to consensus only during consensus
            //  and when finishing state transfer, never in the middle of state transfer
            //  as consensus is inactive then.
            if (lastBlockMode.mustSendTopologyToConsensus) {
              verify(consensusRef, times(1)).asyncSend(
                Consensus.NewEpochTopology(
                  secondEpochNumber,
                  newOrderingTopology,
                  any[CryptoProvider[ProgrammableUnitTestEnv]],
                )
              )
            } else {
              verify(consensusRef, never).asyncSend(
                any[Consensus.ConsensusMessage]
              )
            }

            succeed
          }
        }
      }

    "not process a block from a future epoch" when {
      "when receiving multiple state-transferred blocks" in {
        val subscriptionBlocks = mutable.Queue.empty[BlockFormat.Block]
        val output = createOutputModule[ProgrammableUnitTestEnv](requestInspector =
          (_, _, _, _) => true // All requests are topology transactions
        )(
          blockSubscription = new EnqueueingBlockSubscription(subscriptionBlocks)
        )
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)

        val blockData1 =
          completeBlockData(
            BlockNumber.First,
            aTimestamp,
            lastInEpoch = false, // Do not complete the epoch!
            EpochNumber.First,
            mode = OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock,
          )
        val blockData2 =
          completeBlockData(
            BlockNumber(BlockNumber.First + 1L),
            anotherTimestamp,
            epochNumber = EpochNumber(EpochNumber.First + 1L),
            mode = OrderedBlockForOutput.Mode.StateTransfer.MiddleBlock,
          )

        output.receive(Output.Start)
        output.receive(Output.BlockDataFetched(blockData1))

        val piped1 = context.runPipedMessages()
        piped1 should contain only Output.BlockDataStored(
          blockData1,
          BlockNumber.First,
          aTimestamp,
          epochCouldAlterOrderingTopology = true,
        )
        piped1.foreach(output.receive) // Store first block's metadata

        output.receive(Output.BlockDataFetched(blockData2))

        // Only the first block has now been output to the subscription after its metadata has been stored
        context.runPipedMessages() shouldBe empty
        subscriptionBlocks should have size 1

        // The topology alteration flag should not be reset
        output.currentEpochCouldAlterOrderingTopology shouldBe true
      }
    }

    "not try to issue a new topology but still send a topology to consensus" when {
      "no block in the epoch has requests to all members of synchronizer" in {
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)
        val topologyProviderSpy =
          spy(new FakeOrderingTopologyProvider[ProgrammableUnitTestEnv])
        val consensusRef = mock[ModuleRef[Consensus.Message[ProgrammableUnitTestEnv]]]
        val output = createOutputModule[ProgrammableUnitTestEnv](
          orderingTopologyProvider = topologyProviderSpy,
          consensusRef = consensusRef,
          requestInspector = (_, _, _, _) => false, // No request is for all members of synchronizer
        )()

        val blockData =
          completeBlockData(BlockNumber.First, commitTimestamp = aTimestamp, lastInEpoch = true)

        output.receive(Output.Start)
        output.receive(Output.BlockDataFetched(blockData))
        output.currentEpochCouldAlterOrderingTopology shouldBe false
        context.runPipedMessages().foreach(output.receive) // Fetch the topology

        verify(topologyProviderSpy, never).getOrderingTopologyAt(any[TopologyActivationTime])(
          any[TraceContext]
        )
        verify(consensusRef, times(1)).asyncSend(
          Consensus.NewEpochTopology(
            secondEpochNumber,
            OrderingTopology(peers = Set.empty),
            any[CryptoProvider[ProgrammableUnitTestEnv]],
          )
        )
        succeed
      }
    }

    "get sequencer snapshot additional info" in {
      val cell =
        new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](
          None
        )
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cell)

      val store = createOutputMetadataStore[FakePipeToSelfCellUnitTestEnv]
      val sequencerNodeRef = mock[ModuleRef[SequencerNode.SnapshotMessage]]
      val peer1 = fakeSequencerId("peer1")
      val peer2 = fakeSequencerId("peer2")
      val peer2FirstKnownAtTime = TopologyActivationTime(aTimestamp)
      val firstBlockBftTime =
        peer2FirstKnownAtTime.value.minusMillis(1)
      val peer1FirstKnownAtTime = TopologyActivationTime(peer2FirstKnownAtTime.value.minusMillis(2))
      val topology = OrderingTopology(
        peersActiveAt = Map(
          peer1 -> peer1FirstKnownAtTime,
          peer2 -> peer2FirstKnownAtTime,
          fakeSequencerId("peer from the future") -> TopologyActivationTime(
            peer2FirstKnownAtTime.value.plusMillis(1)
          ),
        ),
        SequencingParameters.Default,
        TopologyActivationTime(CantonTimestamp.MinValue),
        areTherePendingCantonTopologyChanges = false,
      )
      store
        .insertEpochIfMissing(
          OutputEpochMetadata(EpochNumber.First, couldAlterOrderingTopology = true)
        )
        .apply()
      val output =
        createOutputModule[FakePipeToSelfCellUnitTestEnv](
          initialOrderingTopology = topology,
          store = store,
        )()

      output.receive(Output.Start)
      output.receive(
        Output.BlockDataFetched(
          CompleteBlockData(
            anOrderedBlockForOutput(commitTimestamp = firstBlockBftTime),
            batches = Seq.empty,
          )
        )
      )
      cell.get().getOrElse(fail("BlockDataStored not received")).apply() // store block
      output.receive(
        Output.BlockDataFetched(
          CompleteBlockData(
            anOrderedBlockForOutput(
              blockNumber = 1L,
              commitTimestamp = peer2FirstKnownAtTime.value,
            ),
            batches = Seq.empty,
          )
        )
      )
      cell.get().getOrElse(fail("BlockDataStored not received")).apply() // store block

      output.receive(
        Output.SequencerSnapshotMessage
          .GetAdditionalInfo(timestamp = peer2FirstKnownAtTime.value, sequencerNodeRef)
      )

      cell
        .get()
        .getOrElse(
          fail(
            "The snapshot provider didn't schedule the \"first known at\" block metadata queries"
          )
        )
        .apply() shouldBe None // run queries, assert no message

      output.receive(
        cell
          .get()
          .getOrElse(
            fail("The snapshot provider didn't schedule the additional block metadata queries")
          )
          .apply() // run queries
          .getOrElse(fail("The snapshot provider didn't return a message"))
      )

      verify(sequencerNodeRef, times(1)).asyncSend(
        SequencerNode.SnapshotMessage.AdditionalInfo(
          v30.BftSequencerSnapshotAdditionalInfo(
            Map(
              peer1.toProtoPrimitive ->
                v30.BftSequencerSnapshotAdditionalInfo
                  .PeerActiveAt(Some(peer1FirstKnownAtTime.value.toMicros), None, None, None, None),
              peer2.toProtoPrimitive ->
                v30.BftSequencerSnapshotAdditionalInfo
                  .PeerActiveAt(
                    Some(peer2FirstKnownAtTime.value.toMicros),
                    Some(EpochNumber(0L)),
                    firstBlockNumberInEpoch = Some(BlockNumber(0L)),
                    epochCouldAlterOrderingTopology = Some(true),
                    previousBftTime = None,
                  ),
            )
          )
        )
      )
      succeed
    }
  }

  "adjust time for a state-transferred block based on the previous BFT time" in {
    val cell =
      new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](None)
    implicit val context
        : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
      FakePipeToSelfCellUnitTestContext(cell)

    val blockNumber = BlockNumber(2L)
    val availabilityRef = mock[ModuleRef[Availability.Message[FakePipeToSelfCellUnitTestEnv]]]
    val store = createOutputMetadataStore[FakePipeToSelfCellUnitTestEnv]
    val previousBftTimeForOnboarding = Some(aTimestamp)
    val output =
      createOutputModule[FakePipeToSelfCellUnitTestEnv](
        initialHeight = blockNumber,
        availabilityRef = availabilityRef,
        store = store,
        previousBftTimeForOnboarding = previousBftTimeForOnboarding,
      )()

    // Needs to be earlier than the previous block BFT time so that we test monotonicity adjustment.
    val newBlockCommitTime = aTimestamp.minusSeconds(1)
    val block = anOrderedBlockForOutput(
      blockNumber = blockNumber,
      commitTimestamp = newBlockCommitTime,
    )
    output.receive(Output.Start)
    output.receive(Output.BlockOrdered(block))

    verify(availabilityRef, times(1)).asyncSend(
      Availability.LocalOutputFetch.FetchBlockData(block)
    )
    val completeBlockData = CompleteBlockData(block, batches = Seq.empty)
    output.receive(Output.BlockDataFetched(completeBlockData))
    cell.get().getOrElse(fail("BlockDataStored not received")).apply() // store block

    val blocks = store.getBlockFromInclusive(initialBlockNumber = blockNumber)(traceContext)()
    blocks.size shouldBe 1
    assertBlock(
      blocks.head,
      expectedBlockNumber = blockNumber,
      expectedTimestamp = aTimestamp.plus(BftTime.MinimumBlockTimeGranularity.toJava),
    )
    succeed
  }

  "set that there are pending topology changes based on onboarding state" in {
    val output =
      createOutputModule[FakePipeToSelfCellUnitTestEnv](
        areTherePendingTopologyChangesInOnboardingEpoch = true
      )()

    output.currentEpochCouldAlterOrderingTopology shouldBe true
  }

  private def completeBlockData(
      blockNumber: BlockNumber,
      commitTimestamp: CantonTimestamp,
      lastInEpoch: Boolean = false,
      epochNumber: EpochNumber = EpochNumber.First,
      mode: OrderedBlockForOutput.Mode = OrderedBlockForOutput.Mode.FromConsensus,
  ): CompleteBlockData =
    CompleteBlockData(
      anOrderedBlockForOutput(
        epochNumber,
        blockNumber,
        commitTimestamp,
        lastInEpoch,
        mode,
      ),
      batches = Seq(
        OrderingRequestBatch(
          Seq(Traced(OrderingRequest(aTag, ByteString.EMPTY)))
        )
      ).map(x => BatchId.from(x) -> x),
    )

  private def assertBlock(
      actualBlock: OutputMetadataStore.OutputBlockMetadata,
      expectedBlockNumber: BlockNumber,
      expectedTimestamp: CantonTimestamp,
  ) = {
    actualBlock.blockNumber shouldBe expectedBlockNumber
    actualBlock.blockBftTime shouldBe expectedTimestamp
  }

  private def createOutputModule[E <: BaseIgnoringUnitTestEnv[E]](
      initialHeight: Long = BlockNumber.First,
      initialOrderingTopology: OrderingTopology = OrderingTopology(peers = Set.empty),
      availabilityRef: ModuleRef[Availability.Message[E]] = fakeModuleExpectingSilence,
      consensusRef: ModuleRef[Consensus.Message[E]] = fakeModuleExpectingSilence,
      store: OutputMetadataStore[E] = createOutputMetadataStore[E],
      orderedBlocksReader: OrderedBlocksReader[E] = createEpochStore[E],
      orderingTopologyProvider: OrderingTopologyProvider[E] = new FakeOrderingTopologyProvider[E],
      previousBftTimeForOnboarding: Option[CantonTimestamp] = None,
      areTherePendingTopologyChangesInOnboardingEpoch: Boolean = false,
      requestInspector: RequestInspector = DefaultRequestInspector,
  )(
      blockSubscription: BlockSubscription = new EmptyBlockSubscription
  ): OutputModule[E] = {
    val startupState =
      StartupState[E](
        BlockNumber(initialHeight),
        previousBftTimeForOnboarding,
        areTherePendingTopologyChangesInOnboardingEpoch,
        fakeCryptoProvider,
        initialOrderingTopology,
      )
    new OutputModule(
      startupState,
      orderingTopologyProvider,
      store,
      orderedBlocksReader,
      blockSubscription,
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      testedProtocolVersion,
      availabilityRef,
      consensusRef,
      loggerFactory,
      timeouts,
      requestInspector,
    )(MetricsContext.Empty)
  }

  private class TestOutputMetadataStore[E <: BaseIgnoringUnitTestEnv[E]]
      extends GenericInMemoryOutputMetadataStore[E] {

    override protected def createFuture[T](action: String)(value: () => Try[T]): () => T =
      () => value().getOrElse(fail())

    override def close(): Unit = ()

    override protected def reportError(errorMessage: String)(implicit
        traceContext: TraceContext
    ): Unit = fail(errorMessage)
  }

  private def createOutputMetadataStore[E <: BaseIgnoringUnitTestEnv[E]] =
    new TestOutputMetadataStore[E]

  private def createEpochStore[E <: BaseIgnoringUnitTestEnv[E]] =
    new GenericInMemoryEpochStore[E] {

      override protected def createFuture[T](action: String)(value: () => Try[T]): () => T =
        () => value().getOrElse(fail())

      override def close(): Unit = ()
    }
}

object OutputModuleTest {

  private class AccumulatingRequestInspector extends RequestInspector {
    // Ensure that `currentEpochCouldAlterOrderingTopology` accumulates correctly by processing
    //  more than one block and alternating the outcome starting from `true`.
    private var outcome = true

    override def isRequestToAllMembersOfSynchronizer(
        _request: OrderingRequest,
        _protocolVersion: ProtocolVersion,
        _logger: TracedLogger,
        _traceContext: TraceContext,
    ): Boolean = {
      val result = outcome
      outcome = !outcome
      result
    }
  }

  private class EnqueueingBlockSubscription(
      subscriptionBlocks: mutable.Queue[BlockFormat.Block]
  ) extends EmptyBlockSubscription {

    override def receiveBlock(block: BlockFormat.Block)(implicit
        traceContext: TraceContext
    ): Unit =
      subscriptionBlocks.enqueue(block)
  }

  private class FakeOrderingTopologyProvider[E <: BaseIgnoringUnitTestEnv[E]]
      extends OrderingTopologyProvider[E] {

    override def getOrderingTopologyAt(activationTime: TopologyActivationTime)(implicit
        traceContext: TraceContext
    ): E#FutureUnlessShutdownT[Option[(OrderingTopology, CryptoProvider[E])]] = createFuture(None)

    private def createFuture[A](a: A): E#FutureUnlessShutdownT[A] = () => a
  }

  private val aTag = "tag"
  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))
  private val anotherTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:01:00.000Z"))

  private val secondEpochNumber = EpochNumber(1L)
  private val secondBlockNumber = BlockNumber(1L)

  private def anOrderedBlockForOutput(
      epochNumber: Long = EpochNumber.First,
      blockNumber: Long = BlockNumber.First,
      commitTimestamp: CantonTimestamp = aTimestamp,
      lastInEpoch: Boolean = false,
      mode: OrderedBlockForOutput.Mode = OrderedBlockForOutput.Mode.FromConsensus,
  ) =
    OrderedBlockForOutput(
      OrderedBlock(
        BlockMetadata(EpochNumber(epochNumber), BlockNumber(blockNumber)),
        batchRefs = Seq.empty,
        CanonicalCommitSet(
          Set(
            Commit
              .create(
                BlockMetadata.mk(epochNumber = -1L /* whatever */, blockNumber - 1),
                viewNumber = ViewNumber.First,
                Hash
                  .digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
                commitTimestamp,
                from = fakeSequencerId(""),
              )
              .fakeSign
          )
        ),
      ),
      fakeSequencerId(""),
      lastInEpoch,
      mode,
    )
}
