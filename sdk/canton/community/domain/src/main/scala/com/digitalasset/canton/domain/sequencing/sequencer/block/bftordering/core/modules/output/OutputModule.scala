// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.BlockFormat
import com.digitalasset.canton.domain.block.BlockFormat.OrderedRequest
import com.digitalasset.canton.domain.block.LedgerBlockEvent.deserializeSignedOrderingRequest
import com.digitalasset.canton.domain.metrics.BftOrderingMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultDatabaseReadTimeout
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.data.OrderedBlocksReader
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.OutputModule.{
  DefaultRequestInspector,
  PreviousStoredBlock,
  RequestInspector,
  StartupState,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.OutputModuleMetrics.emitRequestsOrderingStats
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.data.OutputBlockMetadataStore.OutputBlockMetadata
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.snapshot.SequencerSnapshotAdditionalInfoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.NumberIdentifiers.{
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.OrderingTopology
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequest,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Output.SequencerSnapshotMessage.{
  AdditionalInfo,
  AdditionalInfoRetrievalError,
  GetAdditionalInfo,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Output.{
  LastBlockUpdated,
  SequencerSnapshotMessage,
  TopologyFetched,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Output,
  SequencerNode,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.{
  BlockSubscription,
  Env,
  ModuleRef,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.sequencing.protocol.AllMembersOfDomain
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.util.{Failure, Success}

/** A module responsible for calculating the [[com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.output.time.BftTime]],
  * querying the topology at epoch ends (if needed), and sending blocks to the sequencer runtime (via the block
  * subscription).
  * It leverages topology ticks that are needed for epochs that could change the topology to make sure we can then query
  * the topology client at the end of an epoch. An epoch potentially changes a topology if sequencer-addressed
  * submissions have been ordered during the epoch, or if the previous epoch had pending topology changes.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class OutputModule[E <: Env[E]](
    startupState: StartupState[E],
    orderingTopologyProvider: OrderingTopologyProvider[E],
    store: OutputBlockMetadataStore[E],
    orderedBlocksReader: OrderedBlocksReader[E],
    blockSubscription: BlockSubscription,
    metrics: BftOrderingMetrics,
    protocolVersion: ProtocolVersion,
    override val availability: ModuleRef[Availability.Message[E]],
    override val consensus: ModuleRef[Consensus.Message[E]],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    requestInspector: RequestInspector = DefaultRequestInspector, // For testing
)(implicit mc: MetricsContext)
    extends Output[E] {

  private val lastAcknowledgedBlockNumber =
    if (startupState.initialHeight == BlockNumber.First) None
    else Some(BlockNumber(startupState.initialHeight - 1))

  // We use a Peano queue to ensure that we can process blocks in order and deterministically produce the correct
  //  BFT time, even if they arrive from Consensus, and/or we finish retrieving their data from availability,
  //  out of order.
  //  There is a further, distinct Peano queue, part of the block subscription, whose job instead is to ensure
  //  that blocks are received in order by the sequencer runtime.
  private var maybeCompletedBlocksProcessingPeanoQueue: Option[PeanoQueue[CompleteBlockData]] = None
  private def completedBlocksPeanoQueue: PeanoQueue[CompleteBlockData] =
    maybeCompletedBlocksProcessingPeanoQueue.getOrElse(
      throw new IllegalStateException("Peano queue not initialized: Start message not received")
    )

  private val previousStoredBlock = new PreviousStoredBlock
  startupState.previousBftTimeForOnboarding.foreach { time =>
    previousStoredBlock.update(BlockNumber(startupState.initialHeight - 1), time)
  }

  private var currentEpochOrderingTopology: OrderingTopology = startupState.initialOrderingTopology
  private var currentEpochCryptoProvider: CryptoProvider[E] = startupState.initialCryptoProvider
  private var currentEpochCouldAlterSequencingTopology =
    startupState.areTherePendingTopologyChangesInOnboardingEpoch

  private val snapshotAdditionalInfoProvider =
    new SequencerSnapshotAdditionalInfoProvider[E](store, loggerFactory)

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  override def receiveInternal(message: Output.Message[E])(implicit
      context: E#ActorContextT[Output.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {

      case Output.Start =>
        val lastStoredOutputBlockMetadata =
          context.blockingAwait(store.getLastConsecutive, DefaultDatabaseReadTimeout)

        lastStoredOutputBlockMetadata.foreach(outputBlockMetadata =>
          currentEpochCouldAlterSequencingTopology =
            outputBlockMetadata.epochCouldAlterSequencingTopology
        )

        // The logic to compute `recoverFromBlockNumber` takes into account the following scenarios:
        //
        // - `lastAcknowledgedBlockNumber` is `None` and `lastStoredOutputBlockMetadata` is also `None`: the node is
        //   an initial node starting from scratch.
        // - `lastAcknowledgedBlockNumber` is `None` and `lastStoredOutputBlockMetadata` is defined or
        //    `lastAcknowledgedBlockNumber` is less than `lastStoredOutputBlockMetadata`: the sequencer
        //   runtime is behind w.r.t. the blocks already processed by the output module.
        // - `lastAcknowledgedBlockNumber` is defined and `lastStoredOutputBlockMetadata` is `None` or
        //   `lastStoredOutputBlockMetadata` is less than `lastAcknowledgedBlockNumber`: data loss has occurred in
        //    the output module or the sequencer runtime is interested in later blocks w.r.t. the blocks already
        //    processed by the output module; barring data loss, this should not happen, as we first complete
        //    processing and only then provide blocks to the sequencer runtime, so we expect the sequencer
        //    runtime to be behind or aligned with the output module. However, this case is supported defensively
        //    and to avoid adding `BftBlockOrderer` API restrictions on the subscriber.
        //
        // Note that, in the common case of the sequencer runtime being behind, to keep things simple we still go
        //  through the normal processing flow and just re-process some already processed blocks, leveraging idempotent
        //  storage and deterministic processing of this module.
        //
        // Furthermore, we also load and reprocess one more block before the first one that has to be recovered,
        //  i.e. the last either stored or acknowledged block, to restore the correct volatile state of this module,
        //  thus ensuring that:
        //
        // - We produce the correct BFT time and store the correct last topology timestamp for the unprocessed blocks.
        // - If the system halted after the last block in an epoch was ordered, its output metadata stored, and it was
        //   also sent to the sequencer, but before the consensus module processed the topology for the
        //   new epoch, a topology is sent to consensus, unblocking it.
        //
        val recoverFromBlockNumber =
          Seq(
            lastAcknowledgedBlockNumber,
            lastStoredOutputBlockMetadata.map(_.blockNumber),
          ).flatten.minOption.getOrElse(BlockNumber.First)

        // If we are onboarding, rather than an initial node, there will be no actual blocks stored and the
        //  genesis will be returned, but we`ll have a truncated log and we`ll need to start from the initial height,
        //  which will be set correctly by the sequencer runtime as the first block height that we are expected to
        //  serve, not from the genesis height.
        maybeCompletedBlocksProcessingPeanoQueue = Some(
          new PeanoQueue(
            if (startupState.previousBftTimeForOnboarding.isDefined) startupState.initialHeight
            else recoverFromBlockNumber
          )
        )
        if (startupState.previousBftTimeForOnboarding.isEmpty) {
          val orderedBlocksToProcess =
            context.blockingAwait(
              orderedBlocksReader.loadOrderedBlocks(recoverFromBlockNumber),
              DefaultDatabaseReadTimeout,
            )
          orderedBlocksToProcess.foreach(orderedBlockForOutput =>
            context.self.asyncSend(Output.BlockOrdered(orderedBlockForOutput))
          )
        }

      // From local consensus
      case Output.BlockOrdered(
            orderedBlockForOutput @ OrderedBlockForOutput(
              orderedBlock,
              _,
              _,
              mode,
            )
          ) =>
        logger.debug(
          s"output received from local consensus ordered block (mode = $mode) " +
            s"with batch IDs ${orderedBlock.batchRefs}, retrieving data from local availability"
        )

        // Block batches will be fetched by the availability module either from the local store or,
        //  if unavailable, from remote peers.
        //  We need to fetch the batches to provide requests, and their BFT sequencing time,
        //  to the sequencer runtime, but this also ensures that all batches are stored locally
        //  when the epoch ends, so that we can provide past block data (e.g. to a re-subscription from
        //  the sequencer runtime after a crash) even if the topology changes drastically.
        availability.asyncSend(
          Availability.LocalOutputFetch.FetchBlockData(orderedBlockForOutput)
        )

      // From availability
      case Output.BlockDataFetched(completedBlockData) =>
        val orderedBlock = completedBlockData.orderedBlockForOutput.orderedBlock
        val blockNumber = orderedBlock.metadata.blockNumber
        logger.debug(
          s"output received completed block; epoch: ${orderedBlock.metadata.epochNumber}, " +
            s"blockID: $blockNumber, batchIDs: ${completedBlockData.batches.map(_._1)}"
        )

        logger.debug(
          s"Inserting block $blockNumber into Peano queue (head=${completedBlocksPeanoQueue.head})"
        )

        val orderedBlocks = completedBlocksPeanoQueue.insertAndPoll(blockNumber, completedBlockData)

        logger.debug(
          s"Polled blocks from Peano queue ${orderedBlocks.map(_.orderedBlockForOutput.orderedBlock.metadata)}"
        )

        // This is the main processing loop where we process blocks in order,
        //  so it is generally safe to use the module's mutable state in it.
        orderedBlocks.foreach { orderedBlockData =>
          val orderedBlock = orderedBlockData.orderedBlockForOutput.orderedBlock
          val orderedBlockNumber = orderedBlock.metadata.blockNumber
          val orderedBlockBftTime = previousStoredBlock.computeBlockBftTime(orderedBlock)

          if (potentiallyAltersSequencersTopology(orderedBlockData)) {
            logger.debug(
              s"Found potential changes of the sequencing topology in ordered block $orderedBlockNumber " +
                s"in epoch ${orderedBlock.metadata.epochNumber}"
            )
            currentEpochCouldAlterSequencingTopology |= true
          }

          val outputBlockMetadata =
            OutputBlockMetadata(
              orderedBlock.metadata.epochNumber,
              orderedBlockNumber,
              orderedBlockBftTime,
              currentEpochCouldAlterSequencingTopology,
            )

          logger.debug(
            s"Assigned BFT time $orderedBlockBftTime to block $orderedBlockNumber " +
              s"in epoch ${orderedBlock.metadata.epochNumber}, previous block was $previousStoredBlock"
          )

          previousStoredBlock.update(orderedBlockNumber, orderedBlockBftTime)

          // Capture and pass the relevant mutable state along to prevent
          //  that message handlers running after async calls race on it.
          val couldAlterSequencingTopology = currentEpochCouldAlterSequencingTopology

          // We start storing the metadata for fully-fetched blocks in order, but the completion of
          //  the storage operations can happen in any order; this allows to optimize performance
          //  and the Peano queue in `BlockSubscription` will ensure they are emitted them in the
          //  correct order.
          //  However, we cannot assume any ordering in the `BlockDataStored` handler below, so
          //  we must pass the value of any relevant mutable state along with the message.
          logger.debug(s"Storing $outputBlockMetadata")
          pipeToSelf(
            store.insertIfMissing(outputBlockMetadata)
          ) {
            case Failure(exception) =>
              abort(s"Failed to add block $orderedBlockNumber", exception)
            case Success(_) =>
              Output.BlockDataStored(
                orderedBlockData,
                orderedBlockNumber,
                orderedBlockBftTime,
                couldAlterSequencingTopology,
              )
          }
        }

      // Blocks metadata persistence can complete in any order, so relying on mutable state
      //  is generally unsafe in this handler.
      case Output.BlockDataStored(
            orderedBlockData,
            orderedBlockNumber,
            orderedBlockBftTime,
            epochCouldAlterSequencingTopology,
          ) =>
        emitRequestsOrderingStats(metrics, orderedBlockData)

        // Since consensus will wait for the topology before starting the new epoch, and we send it only when all
        //  blocks, including the last block of the previous epoch, are fully fetched, all blocks can always be read
        //  locally, which is essential because all other peers could (in principle, although this is definitely
        //  not sensible governance) be swapped in the new epoch, so they would have no past data and would thus
        //  be unable to provide it to us.
        // We fetch the topology once the last block is stored as, based on the returned topology, the last block
        //  might need to be updated with pending topology changes.
        if (orderedBlockData.orderedBlockForOutput.isLastInEpoch)
          fetchNewEpochTopologyIfNeeded(
            orderedBlockData,
            orderedBlockBftTime,
            epochCouldAlterSequencingTopology,
          )

        // This is just a defensive check, as the block subscription will have the head correctly set to the
        //  initial height and will ignore blocks before that, but we cannot check nor enforce this assumption
        //  in this module due to the generic Peano queue type needed for simulation testing support.
        if (lastAcknowledgedBlockNumber.forall(orderedBlockNumber > _)) {
          val isBlockLastInEpoch = orderedBlockData.orderedBlockForOutput.isLastInEpoch
          // We tick the topology even during state transfer; this is not needed by the Output module,
          //  because during state transfer we don't query the topology (as consensus is not active),
          //  but it ensures that the newly onboarded sequencer sequences (and stores) the same events
          //  as the other sequencers, which in turn makes counters (and snapshots) consistent,
          //  avoiding possible future problems e.g. with pruning and/or BFT onboarding from multiple
          //  sequencer snapshots.
          val tickTopology = isBlockLastInEpoch && epochCouldAlterSequencingTopology
          logger.debug(
            s"Sending block $orderedBlockNumber " +
              s"(current epoch = ${orderedBlockData.orderedBlockForOutput.orderedBlock.metadata.epochNumber}, " +
              s"block's BFT time = $orderedBlockBftTime, " +
              s"block size = ${orderedBlockData.requestsView.size}, " +
              s"is last in epoch = $isBlockLastInEpoch, " +
              s"could alter sequencing topology = $epochCouldAlterSequencingTopology, " +
              s"tick topology = $tickTopology) " +
              "to sequencer subscription"
          )

          blockSubscription.receiveBlock(
            BlockFormat.Block(
              orderedBlockNumber,
              blockDataToOrderedRequests(orderedBlockData, orderedBlockBftTime),
              tickTopologyAtMicrosFromEpoch = Option.when(tickTopology)(
                BftTime.epochEndBftTime(orderedBlockBftTime, orderedBlockData).toMicros
              ),
            )
          )
        }

      case TopologyFetched(
            lastCompletedBlockNumber,
            lastCompletedBlockMode,
            newEpochNumber,
            orderingTopology,
            cryptoProvider,
          ) =>
        logger.debug(s"Fetched topology $orderingTopology for new epoch $newEpochNumber")
        if (orderingTopology.areTherePendingCantonTopologyChanges)
          pipeToSelf(
            store.setPendingChangesInNextEpoch(
              lastCompletedBlockNumber,
              orderingTopology.areTherePendingCantonTopologyChanges,
            )
          ) {
            case Failure(exception) =>
              abort(
                s"Failed to set pending changes in next epoch for block $lastCompletedBlockNumber",
                exception,
              )
            case Success(_) =>
              LastBlockUpdated(
                lastCompletedBlockNumber,
                lastCompletedBlockMode,
                newEpochNumber,
                orderingTopology,
                cryptoProvider,
              )
          }
        else
          setupNewEpoch(
            newEpochNumber,
            Some(orderingTopology -> cryptoProvider),
            lastCompletedBlockMode,
          )

      case LastBlockUpdated(
            lastCompletedBlockNumber,
            lastCompletedBlockMode,
            newEpochNumber,
            orderingTopology,
            cryptoProvider,
          ) =>
        logger.debug(s"Updated last block $lastCompletedBlockNumber")
        setupNewEpoch(
          newEpochNumber,
          Some(orderingTopology -> cryptoProvider),
          lastCompletedBlockMode,
        )

      case snapshotMessage: SequencerSnapshotMessage =>
        handleSnapshotMessage(snapshotMessage)

      case Output.AsyncException(exception) =>
        abort(s"Failed to retrieve new epoch's topology", exception)

      case Output.NoTopologyAvailable =>
        logger.info("No topology snapshot available due to either shutting down or testing")
    }

  private def handleSnapshotMessage(
      message: SequencerSnapshotMessage
  )(implicit context: E#ActorContextT[Output.Message[E]], traceContext: TraceContext): Unit =
    message match {
      case GetAdditionalInfo(timestamp, from) =>
        snapshotAdditionalInfoProvider.provide(timestamp, currentEpochOrderingTopology, from)

      case AdditionalInfo(requester, info) =>
        requester.asyncSend(SequencerNode.SnapshotMessage.AdditionalInfo(info.toProto))

      case AdditionalInfoRetrievalError(requester, errorMessage) =>
        requester.asyncSend(
          SequencerNode.SnapshotMessage.AdditionalInfoRetrievalError(errorMessage)
        )
    }

  private def potentiallyAltersSequencersTopology(
      orderedBlockData: CompleteBlockData
  ): Boolean =
    orderedBlockData.requestsView.toSeq.findLast {
      case tracedOrderingRequest @ Traced(orderingRequest) =>
        requestInspector.isRequestToAllMembersOfDomain(
          orderingRequest,
          protocolVersion,
          logger,
          tracedOrderingRequest.traceContext,
        )
    }.isDefined

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  private def fetchNewEpochTopologyIfNeeded(
      lastBlockInEpoch: CompleteBlockData,
      epochLastBlockBftTime: CantonTimestamp,
      epochCouldAlterSequencingTopology: Boolean,
  )(implicit context: E#ActorContextT[Output.Message[E]], traceContext: TraceContext): Unit = {
    val lastBlockForOutput = lastBlockInEpoch.orderedBlockForOutput
    val blockMetadata = lastBlockForOutput.orderedBlock.metadata

    if (!lastBlockForOutput.isLastInEpoch)
      abort(s"Block ${blockMetadata.blockNumber} not last in epoch ${blockMetadata.epochNumber}")

    val completedEpochNumber = blockMetadata.epochNumber
    val lastCompletedBlockNumber = blockMetadata.blockNumber
    logger.debug(
      s"Last ordered block $lastCompletedBlockNumber in epoch $completedEpochNumber fully processed"
    )

    val epochEndBftTime = BftTime.epochEndBftTime(epochLastBlockBftTime, lastBlockInEpoch)

    val lastBlockMode = lastBlockForOutput.mode
    if (epochCouldAlterSequencingTopology) {
      logger.debug(
        s"Completed epoch $completedEpochNumber that could alter sequencing topology: " +
          s"last block mode = $lastBlockMode; querying for an updated Canton topology effective after ticking " +
          s"the topology processor with epoch's last sequencing time $epochEndBftTime)"
      )
      // Once a topology processor observes (processes) a sequenced request with sequencing time `t`,
      //  which is considered the "end-of-epoch" sequencing time, the topology processor can safely serve
      //  topology snapshots, at a minimum when the delay is 0, up to effective time `t.immediateSuccessor`.
      //  We want the ordering layer to observe topology changes timely, so we can safely
      //  query for a topology snapshot at effective time `t.immediateSuccessor`.
      //  When the topology change delay is 0, this allows running a subsequent epoch
      //  using an ordering topology that includes the potential effects a topology transaction
      //  that was successfully sequenced as the last request in the preceding epoch
      //  (and successfully processed and applied by the topology processor).
      pipeToSelf(
        orderingTopologyProvider.getOrderingTopologyAt(
          TopologyActivationTime(epochEndBftTime.immediateSuccessor)
        ),
        metrics.topology.queryLatency,
      ) {
        case Failure(exception) => Output.AsyncException(exception)
        case Success(Some((orderingTopology, cryptoProvider))) =>
          Output.TopologyFetched(
            lastCompletedBlockNumber,
            lastBlockMode,
            EpochNumber(completedEpochNumber + 1),
            orderingTopology,
            cryptoProvider,
          )
        case Success(None) =>
          Output.NoTopologyAvailable
      }
    } else {
      logger.debug(s"Completed epoch $completedEpochNumber that did not change the topology")
      setupNewEpoch(EpochNumber(completedEpochNumber + 1), None, lastBlockMode)
    }
  }

  private def setupNewEpoch(
      newEpochNumber: EpochNumber,
      newOrderingTopologyAndCryptoProvider: Option[(OrderingTopology, CryptoProvider[E])],
      lastCompletedBlockMode: OrderedBlockForOutput.Mode,
  )(implicit traceContext: TraceContext): Unit = {
    // It is safe to use mutable state in this function because:
    // - During state transfer the system can receive blocks while the new epoch is being set up, but since
    //   consensus is inactive, this function is not called after querying the topology but directly from
    //   the main blocks processing loop, i.e. it is called sequentially in block order.
    // - During consensus, it is called after the async query to the topology completes,
    //   but there are no races because the system won't proceed until the topology is fetched.
    currentEpochCouldAlterSequencingTopology = false

    newOrderingTopologyAndCryptoProvider.foreach { case (newOrderingTopology, newCryptoProvider) =>
      currentEpochOrderingTopology = newOrderingTopology
      currentEpochCryptoProvider = newCryptoProvider
      currentEpochCouldAlterSequencingTopology =
        newOrderingTopology.areTherePendingCantonTopologyChanges
    }

    if (lastCompletedBlockMode.shouldSendTopologyToConsensus) {
      metrics.topology.validators.updateValue(currentEpochOrderingTopology.peers.size)
      logger.debug(s"Sending new epoch's topology $currentEpochOrderingTopology to it")
      consensus.asyncSend(
        Consensus.NewEpochTopology(
          newEpochNumber,
          currentEpochOrderingTopology,
          currentEpochCryptoProvider,
        )
      )
    }
  }

  private def blockDataToOrderedRequests(
      blockData: CompleteBlockData,
      blockBftTime: CantonTimestamp,
  ): Seq[Traced[OrderedRequest]] =
    blockData.requestsView.zipWithIndex.map {
      case (tracedRequest @ Traced(OrderingRequest(tag, body, _)), index) =>
        val timestamp = BftTime.requestBftTime(blockBftTime, index)
        Traced(OrderedRequest(timestamp.toMicros, tag, body))(tracedRequest.traceContext)
    }.toSeq

  @VisibleForTesting private[bftordering] def getCurrentEpochCouldAlterSequencingTopology =
    currentEpochCouldAlterSequencingTopology
}

object OutputModule {

  final case class StartupState[E <: Env[E]](
      initialHeight: BlockNumber,
      previousBftTimeForOnboarding: Option[CantonTimestamp],
      areTherePendingTopologyChangesInOnboardingEpoch: Boolean,
      initialCryptoProvider: CryptoProvider[E],
      initialOrderingTopology: OrderingTopology,
  )

  private final class PreviousStoredBlock {

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var blockNumberAndBftTime: Option[(BlockNumber, CantonTimestamp)] = None

    override def toString: String =
      blockNumberAndBftTime
        .map(b => s"(block number = ${b._1}, BFT time = ${b._2})")
        .getOrElse("undefined")

    def update(blockNumber: BlockNumber, blockBftTime: CantonTimestamp): Unit =
      blockNumberAndBftTime = Some(blockNumber -> blockBftTime)

    def computeBlockBftTime(orderedBlock: OrderedBlock): CantonTimestamp =
      BftTime.blockBftTime(
        orderedBlock.canonicalCommitSet,
        previousBlockBftTime = blockNumberAndBftTime.map(_._2).getOrElse(CantonTimestamp.Epoch),
      )
  }

  trait RequestInspector {
    def isRequestToAllMembersOfDomain(
        request: OrderingRequest,
        protocolVersion: ProtocolVersion,
        logger: TracedLogger,
        traceContext: TraceContext,
    ): Boolean
  }

  @VisibleForTesting
  private[bftordering] object DefaultRequestInspector extends RequestInspector {

    override def isRequestToAllMembersOfDomain(
        request: OrderingRequest,
        protocolVersion: ProtocolVersion,
        logger: TracedLogger,
        traceContext: TraceContext,
    ): Boolean =
      // TODO(#21615) we should avoid a further deserialization downstream
      deserializeSignedOrderingRequest(protocolVersion)(request.payload) match {
        case Right(signedSubmissionRequest) =>
          signedSubmissionRequest.content.content.content.batch.allRecipients
            .contains(AllMembersOfDomain)
        case Left(error) =>
          logger.info(
            s"Skipping ordering request while looking for sequencer events as it failed to deserialize: $error"
          )(traceContext)
          false
      }
  }
}
