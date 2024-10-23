// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.BlockFormat.OrderedRequest
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.OrderedBlocksReader
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.memory.GenericInMemoryEpochStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.OutputModule.{
  DefaultRequestInspector,
  RequestInspector,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.memory.GenericInMemoryOutputBlockMetadataStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.time.BftTime.MinimumBlockTimeGranularity
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.{
  OutputModule,
  PekkoBlockSubscription,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  OrderingTopologyProvider,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.fakeSequencerId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.BatchId
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.{
  OrderingTopology,
  SequencingParameters,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequest,
  OrderingRequestBatch,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.Commit
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Output,
  SequencerNode,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  BlockSubscription,
  EmptyBlockSubscription,
  ModuleRef,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.unit.modules.*
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencer.admin.v30
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext}
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.wordspec.AsyncWordSpecLike

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.DurationConverters.*
import scala.util.Try

class OutputModuleTest
    extends AsyncWordSpecLike
    with BaseTest
    with HasActorSystem
    with HasExecutionContext {

  import OutputModuleTest.*

  "OutputModule" should {
    val initialBlock = anOrderedBlockForOutput()

    "fetch block from availability" in {
      implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
        IgnoringUnitTestContext()

      val availabilityRef = mock[ModuleRef[Availability.Message[IgnoringUnitTestEnv]]]
      val output =
        createOutputModule[IgnoringUnitTestEnv](availabilityRef = availabilityRef)()

      output.receive(Output.BlockOrdered(initialBlock))

      verify(availabilityRef, times(1)).asyncSend(
        Availability.LocalOutputFetch.FetchBlockData(initialBlock)
      )
      succeed
    }

    "store ordered block" in {
      val cell =
        new AtomicReference[Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]](
          None
        )
      implicit val context
          : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
        FakePipeToSelfCellUnitTestContext(cell)

      val outputBlockMetadataStore = createOutputBlockMetadataStore[FakePipeToSelfCellUnitTestEnv]
      val output = createOutputModule[FakePipeToSelfCellUnitTestEnv](blockMetadataStore =
        outputBlockMetadataStore
      )()

      val completeBlockData = CompleteBlockData(initialBlock, batches = Seq.empty)

      output.receive(Output.Start)
      output.receive(Output.BlockDataFetched(completeBlockData))
      cell.get().getOrElse(fail("BlockDataStored not received")).apply() // store block

      val blocks =
        outputBlockMetadataStore.getFromInclusive(initialBlockNumber = BlockNumber.First)(
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

      val outputBlockMetadataStore = createOutputBlockMetadataStore[ProgrammableUnitTestEnv]
      val output =
        createOutputModule[ProgrammableUnitTestEnv](blockMetadataStore = outputBlockMetadataStore)()

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
        outputBlockMetadataStore.getFromInclusive(initialBlockNumber = BlockNumber.First)(
          traceContext
        )()
      output.receive(Output.BlockDataFetched(initialBlockData))
      context.sizeOfPipedMessages shouldBe 2
      context.runPipedMessagesAndReceiveOnModule(output) // store blocks

      val blocks =
        outputBlockMetadataStore.getFromInclusive(initialBlockNumber = BlockNumber.First)(
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

      val outputBlockMetadataStore = createOutputBlockMetadataStore[ProgrammableUnitTestEnv]
      val output =
        createOutputModule[ProgrammableUnitTestEnv](blockMetadataStore = outputBlockMetadataStore)()
      val completeInitialBlock = CompleteBlockData(initialBlock, batches = Seq.empty)

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
          blockMetadataStore = outputBlockMetadataStore,
          orderedBlocksReader = orderedBlocksReader,
        )()
      outputAfterRestart.receive(Output.Start)

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
        outputBlockMetadataStore.getFromInclusive(initialBlockNumber = BlockNumber.First)(
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

        val lastStoredBlock = anOrderedBlockForOutput(blockNumber = recoverFromBlockNumber)
        val outputBlockMetadataStore = mock[OutputBlockMetadataStore[ProgrammableUnitTestEnv]]
        when(outputBlockMetadataStore.getLastConsecutive(traceContext)).thenReturn(() =>
          Some(
            OutputBlockMetadata(
              epochNumber = secondEpochNumber,
              blockNumber = recoverFromBlockNumber,
              blockBftTime = aTimestamp,
              epochCouldAlterSequencingTopology = true,
            )
          )
        )
        val orderedBlocksReader = mock[OrderedBlocksReader[ProgrammableUnitTestEnv]]
        // The output module will recover from the last stored block (< last acknowledged block) to rebuilt its
        //  volatile state, and it will then wait for further blocks from consensus.
        when(
          orderedBlocksReader.loadOrderedBlocks(initialBlockNumber = recoverFromBlockNumber)(
            traceContext
          )
        )
          .thenReturn(() => Seq(lastStoredBlock))
        val availabilityCell =
          new AtomicReference[Option[Availability.Message[ProgrammableUnitTestEnv]]](None)
        val output = createOutputModule[ProgrammableUnitTestEnv](
          initialHeight = 2L,
          availabilityRef = fakeCellModule(availabilityCell),
          blockMetadataStore = outputBlockMetadataStore,
          orderedBlocksReader = orderedBlocksReader,
        )()
        output.receive(Output.Start)

        verify(outputBlockMetadataStore, times(1)).getLastConsecutive(traceContext)
        verify(orderedBlocksReader, times(1))
          .loadOrderedBlocks(initialBlockNumber = recoverFromBlockNumber)(
            traceContext
          )
        context.selfMessages should contain only Output.BlockOrdered(lastStoredBlock)
        output.getCurrentEpochCouldAlterSequencingTopology shouldBe true
      }

      "it is ahead of the subscription" in {
        implicit val context: ProgrammableUnitTestContext[Output.Message[ProgrammableUnitTestEnv]] =
          new ProgrammableUnitTestContext(resolveAwaits = true)
        val lastStoredBlock = anOrderedBlockForOutput(blockNumber = recoverFromBlockNumber)
        val outputBlockMetadataStore = mock[OutputBlockMetadataStore[ProgrammableUnitTestEnv]]
        when(outputBlockMetadataStore.getLastConsecutive(traceContext)).thenReturn(() =>
          Some(
            OutputBlockMetadata(
              epochNumber = secondEpochNumber,
              blockNumber = BlockNumber(3L),
              blockBftTime = aTimestamp,
              epochCouldAlterSequencingTopology = true,
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
        val availabilityCell =
          new AtomicReference[Option[Availability.Message[ProgrammableUnitTestEnv]]](None)
        val output = createOutputModule[ProgrammableUnitTestEnv](
          initialHeight = 2L,
          availabilityRef = fakeCellModule(availabilityCell),
          blockMetadataStore = outputBlockMetadataStore,
          orderedBlocksReader = orderedBlocksReader,
        )()
        output.receive(Output.Start)

        verify(outputBlockMetadataStore, times(1)).getLastConsecutive(traceContext)
        verify(orderedBlocksReader, times(1))
          .loadOrderedBlocks(initialBlockNumber = recoverFromBlockNumber)(
            traceContext
          )
        context.selfMessages should contain only Output.BlockOrdered(lastStoredBlock)
        output.getCurrentEpochCouldAlterSequencingTopology shouldBe true

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

      val outputBlockMetadataStore = createOutputBlockMetadataStore[FakePipeToSelfCellUnitTestEnv]
      val blockSubscription =
        new PekkoBlockSubscription[FakePipeToSelfCellUnitTestEnv](
          initialHeight = BlockNumber.First,
          timeouts,
          loggerFactory,
        )
      val output =
        createOutputModule[FakePipeToSelfCellUnitTestEnv](blockMetadataStore =
          outputBlockMetadataStore
        )(
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

      val outputBlockMetadataStore = createOutputBlockMetadataStore[FakePipeToSelfCellUnitTestEnv]

      val blockSubscription =
        new PekkoBlockSubscription[FakePipeToSelfCellUnitTestEnv](
          secondBlockNumber,
          timeouts,
          loggerFactory,
        )
      val output = createOutputModule[FakePipeToSelfCellUnitTestEnv](
        blockMetadataStore = outputBlockMetadataStore,
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

      val store = createOutputBlockMetadataStore[IgnoringUnitTestEnv]

      val initialHeight = BlockNumber(2L)
      val blockSubscription =
        new PekkoBlockSubscription[IgnoringUnitTestEnv](initialHeight, timeouts, loggerFactory)
      val output = createOutputModule[IgnoringUnitTestEnv](
        blockMetadataStore = store,
        initialHeight = initialHeight,
      )(blockSubscription)

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
            CantonTimestamp.MinValue, /* irrelevant for the test */
          )
        )
      )

      blockSubscription.subscription().runWith(Sink.head).map { block =>
        block.blockHeight shouldBe initialHeight
        block.requests should be(empty)
      }
    }

    "set the potential topology change flag to `true`, fetch a new topology and send it to consensus" when {
      "at least one block in the current epoch has requests to all members of domain" in {
        Table("Pending Canton topology changes", false, true).forEvery {
          val topologySnapshotEffectiveTime =
            EffectiveTime(anotherTimestamp.immediateSuccessor)

          pendingChanges =>
            val topologyProviderMock = mock[OrderingTopologyProvider[FakePipeToSelfCellUnitTestEnv]]
            val consensusRef = mock[ModuleRef[Consensus.Message[FakePipeToSelfCellUnitTestEnv]]]
            val newOrderingTopology =
              OrderingTopology(
                peers = Set.empty,
                SequencingParameters.Default,
                topologySnapshotEffectiveTime = topologySnapshotEffectiveTime,
                areTherePendingCantonTopologyChanges = pendingChanges,
              )
            when(
              topologyProviderMock.getOrderingTopologyAt(
                topologySnapshotEffectiveTime,
                assumePendingTopologyChanges = false,
              )
            )
              .thenReturn(() => Some((newOrderingTopology, fakeCryptoProvider)))
            val output = createOutputModule[FakePipeToSelfCellUnitTestEnv](
              orderingTopologyProvider = topologyProviderMock,
              consensusRef = consensusRef,
              requestInspector = new RequestInspector {
                // Ensure that `currentEpochCouldAlterSequencingTopology` accumulates correctly by processing
                //  more than one block and alternating the outcome starting from `true`.
                private var outcome = true

                override def isRequestToAllMembersOfDomain(
                    _request: OrderingRequest,
                    _protocolVersion: ProtocolVersion,
                    _logger: TracedLogger,
                    _traceContext: TraceContext,
                ): Boolean = {
                  val result = outcome
                  outcome = !outcome
                  result
                }
              },
            )()
            val cell =
              new AtomicReference[
                Option[() => Option[Output.Message[FakePipeToSelfCellUnitTestEnv]]]
              ]
            implicit val context
                : FakePipeToSelfCellUnitTestContext[Output.Message[FakePipeToSelfCellUnitTestEnv]] =
              FakePipeToSelfCellUnitTestContext(cell)

            val blockData1 =
              completeBlockData(
                BlockNumber.First,
                commitTimestamp = aTimestamp,
                lastInEpoch = false,
              )
            val blockData2 = completeBlockData(
              BlockNumber(BlockNumber.First + 1L),
              commitTimestamp = anotherTimestamp,
              lastInEpoch = true,
            )
            output.receive(Output.Start)
            output.receive(Output.BlockDataFetched(blockData1))
            output.receive(Output.BlockDataFetched(blockData2))
            output.getCurrentEpochCouldAlterSequencingTopology shouldBe true

            verify(topologyProviderMock, times(1)).getOrderingTopologyAt(
              topologySnapshotEffectiveTime,
              assumePendingTopologyChanges = false,
            )

            output.receive(
              Output.TopologyFetched(
                secondEpochNumber,
                newOrderingTopology,
                fakeCryptoProvider,
              )
            )

            output.getCurrentEpochCouldAlterSequencingTopology shouldBe pendingChanges
            verify(consensusRef, times(1)).asyncSend(
              Consensus.NewEpochTopology(
                secondEpochNumber,
                newOrderingTopology,
                any[CryptoProvider[FakePipeToSelfCellUnitTestEnv]],
              )
            )

            succeed
        }
      }
    }

    "not try to issue a new topology but still send a topology to consensus" when {
      "no block in the epoch has requests to all members of domain" in {
        implicit val context: IgnoringUnitTestContext[Output.Message[IgnoringUnitTestEnv]] =
          IgnoringUnitTestContext()
        val topologyProviderSpy =
          spy(new FakeOrderingTopologyProvider[IgnoringUnitTestEnv])
        val consensusRef = mock[ModuleRef[Consensus.Message[IgnoringUnitTestEnv]]]
        val output = createOutputModule[IgnoringUnitTestEnv](
          orderingTopologyProvider = topologyProviderSpy,
          consensusRef = consensusRef,
          requestInspector = (_, _, _, _) => false, // No request is for all members of domain
        )()

        val blockData =
          completeBlockData(BlockNumber.First, commitTimestamp = aTimestamp, lastInEpoch = true)

        output.receive(Output.Start)
        output.receive(Output.BlockDataFetched(blockData))
        output.getCurrentEpochCouldAlterSequencingTopology shouldBe false

        verify(topologyProviderSpy, never).getOrderingTopologyAt(any[EffectiveTime], any[Boolean])(
          any[TraceContext]
        )
        verify(consensusRef, times(1)).asyncSend(
          Consensus.NewEpochTopology(
            secondEpochNumber,
            OrderingTopology(peers = Set.empty),
            any[CryptoProvider[IgnoringUnitTestEnv]],
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

      val sequencerNodeRef = mock[ModuleRef[SequencerNode.SnapshotMessage]]
      val peer1 = fakeSequencerId("peer1")
      val peer2 = fakeSequencerId("peer2")
      val peer2FirstKnownAtTime = EffectiveTime(aTimestamp)
      val firstBlockBftTime =
        peer2FirstKnownAtTime.value.minusMillis(1)
      val peer1FirstKnownAtTime = EffectiveTime(peer2FirstKnownAtTime.value.minusMillis(2))
      val topology = OrderingTopology(
        peersFirstKnownAt = Map(
          peer1 -> peer1FirstKnownAtTime,
          peer2 -> peer2FirstKnownAtTime,
          fakeSequencerId("peer from the future") -> EffectiveTime(
            peer2FirstKnownAtTime.value.plusMillis(1)
          ),
        ),
        SequencingParameters.Default,
        EffectiveTime(CantonTimestamp.MinValue),
        areTherePendingCantonTopologyChanges = false,
      )
      val output =
        createOutputModule[FakePipeToSelfCellUnitTestEnv](initialOrderingTopology = topology)()

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
              epochNumber = 1L,
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
                  .FirstKnownAt(Some(peer1FirstKnownAtTime.value.toMicros), None, None, None),
              peer2.toProtoPrimitive ->
                v30.BftSequencerSnapshotAdditionalInfo
                  .FirstKnownAt(
                    Some(peer2FirstKnownAtTime.value.toMicros),
                    Some(EpochNumber(1L)),
                    firstBlockNumberInEpoch = Some(BlockNumber(1L)),
                    previousBftTime = Some(firstBlockBftTime.toMicros),
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
    val store = createOutputBlockMetadataStore[FakePipeToSelfCellUnitTestEnv]
    val previousBftTimeForOnboarding = Some(aTimestamp)
    val output =
      createOutputModule[FakePipeToSelfCellUnitTestEnv](
        initialHeight = blockNumber,
        availabilityRef = availabilityRef,
        blockMetadataStore = store,
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

    val blocks = store.getFromInclusive(initialBlockNumber = blockNumber)(traceContext)()
    blocks.size shouldBe 1
    assertBlock(
      blocks.head,
      expectedBlockNumber = blockNumber,
      expectedTimestamp = aTimestamp.plus(BftTime.MinimumBlockTimeGranularity.toJava),
    )
    succeed
  }

  private def completeBlockData(
      blockNumber: BlockNumber,
      commitTimestamp: CantonTimestamp,
      lastInEpoch: Boolean,
  ): CompleteBlockData =
    CompleteBlockData(
      anOrderedBlockForOutput(
        blockNumber = blockNumber,
        commitTimestamp = commitTimestamp,
        lastInEpoch = lastInEpoch,
      ),
      batches = Seq(
        OrderingRequestBatch(
          Seq(Traced(OrderingRequest(aTag, ByteString.EMPTY)))
        )
      ).map(x => BatchId.from(x) -> x),
    )

  private def assertBlock(
      actualBlock: OutputBlockMetadataStore.OutputBlockMetadata,
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
      blockMetadataStore: OutputBlockMetadataStore[E] = createOutputBlockMetadataStore[E],
      orderedBlocksReader: OrderedBlocksReader[E] = createEpochStore[E],
      orderingTopologyProvider: OrderingTopologyProvider[E] = new FakeOrderingTopologyProvider[E],
      previousBftTimeForOnboarding: Option[CantonTimestamp] = None,
      requestInspector: RequestInspector = DefaultRequestInspector,
  )(
      blockSubscription: BlockSubscription = new EmptyBlockSubscription
  ): OutputModule[E] =
    new OutputModule(
      BlockNumber(initialHeight),
      testedProtocolVersion,
      previousBftTimeForOnboarding,
      fakeCryptoProvider,
      initialOrderingTopology,
      orderingTopologyProvider,
      blockMetadataStore,
      orderedBlocksReader,
      blockSubscription,
      SequencerMetrics.noop(getClass.getSimpleName).bftOrdering,
      availabilityRef,
      consensusRef,
      loggerFactory,
      timeouts,
      requestInspector,
    )(MetricsContext.Empty)

  private def createOutputBlockMetadataStore[E <: BaseIgnoringUnitTestEnv[E]] =
    new GenericInMemoryOutputBlockMetadataStore[E] {
      override protected def createFuture[T](action: String)(value: () => Try[T]): () => T =
        () => value().getOrElse(fail())
    }

  private def createEpochStore[E <: BaseIgnoringUnitTestEnv[E]] =
    new GenericInMemoryEpochStore[E] {
      override protected def createFuture[T](action: String)(value: () => Try[T]): () => T =
        () => value().getOrElse(fail())
    }
}

object OutputModuleTest {
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
  ) =
    OrderedBlockForOutput(
      OrderedBlock(
        BlockMetadata(EpochNumber(epochNumber), BlockNumber(blockNumber)),
        batchRefs = Seq.empty,
        CanonicalCommitSet(
          Set(
            Commit.create(
              BlockMetadata.mk(epochNumber = -1L /* whatever */, blockNumber - 1),
              viewNumber = ViewNumber.First,
              Hash.digest(HashPurpose.BftOrderingPbftBlock, ByteString.EMPTY, HashAlgorithm.Sha256),
              commitTimestamp,
              from = fakeSequencerId(""),
            )
          )
        ),
      ),
      fakeSequencerId(""),
      lastInEpoch,
    )
}

class FakeOrderingTopologyProvider[E <: BaseIgnoringUnitTestEnv[E]]
    extends OrderingTopologyProvider[E] {

  override def getOrderingTopologyAt(
      timestamp: EffectiveTime,
      assumePendingTopologyChanges: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): E#FutureUnlessShutdownT[Option[(OrderingTopology, CryptoProvider[E])]] = createFuture(None)

  private def createFuture[A](a: A): E#FutureUnlessShutdownT[A] = () => a
}
