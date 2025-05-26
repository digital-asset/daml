// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  MaxRequestSizeToDeserialize,
}
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.block.BlockFormat.OrderedRequest
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.deserializeSignedOrderingRequest
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.HasDelayedInit
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.{
  DefaultDatabaseReadTimeout,
  DefaultLeaderSelectionPolicy,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStoreReader
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModule.{
  DefaultRequestInspector,
  PreviousStoredBlock,
  RequestInspector,
  StartupState,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.OutputModuleMetrics.emitRequestsOrderingStats
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.data.OutputMetadataStore.{
  OutputBlockMetadata,
  OutputEpochMetadata,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.snapshot.SequencerSnapshotAdditionalInfoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time.BftTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.{
  CryptoProvider,
  OrderingTopologyProvider,
  TopologyActivationTime,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequest,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.NewEpochTopology
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Output.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Output.SequencerSnapshotMessage.{
  AdditionalInfo,
  AdditionalInfoRetrievalError,
  GetAdditionalInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Output,
  SequencerNode,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  BlockSubscription,
  Env,
  ModuleRef,
  PureFun,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.mutable
import scala.util.{Failure, Success}

/** A module responsible for calculating the [[time.BftTime]], querying the topology at epoch ends
  * (if needed), and sending blocks to the sequencer runtime (via the block subscription). It
  * leverages topology ticks that are needed for epochs that could change the topology to make sure
  * we can then query the topology client at the end of an epoch. An epoch potentially changes a
  * topology if sequencer-addressed submissions have been ordered during the epoch, or if the
  * previous epoch had pending topology changes.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class OutputModule[E <: Env[E]](
    startupState: StartupState[E],
    orderingTopologyProvider: OrderingTopologyProvider[E],
    store: OutputMetadataStore[E],
    epochStoreReader: EpochStoreReader[E],
    blockSubscription: BlockSubscription,
    metrics: BftOrderingMetrics,
    override val availability: ModuleRef[Availability.Message[E]],
    override val consensus: ModuleRef[Consensus.Message[E]],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    requestInspector: RequestInspector = DefaultRequestInspector, // For testing
)(implicit
    override val config: BftBlockOrdererConfig,
    synchronizerProtocolVersion: ProtocolVersion,
    mc: MetricsContext,
) extends Output[E]
    with HasDelayedInit[Message[E]] {

  private val thisNode = startupState.thisNode

  private val lastAcknowledgedBlockNumber =
    if (startupState.initialHeightToProvide == BlockNumber.First) None
    else Some(BlockNumber(startupState.initialHeightToProvide - 1))

  // We use a Peano queue to ensure that we can process blocks in order and deterministically produce the correct
  //  BFT time, even if they arrive from Consensus, and/or we finish retrieving their data from availability,
  //  out of order.
  //  There is a further, distinct Peano queue, part of the block subscription, whose job instead is to ensure
  //  that blocks are received in order by the sequencer runtime.
  private val maybeCompletedBlocksProcessingPeanoQueue =
    new SingleUseCell[PeanoQueue[BlockNumber, CompleteBlockData]]
  private def completedBlocksPeanoQueue: PeanoQueue[BlockNumber, CompleteBlockData] =
    maybeCompletedBlocksProcessingPeanoQueue.getOrElse(
      throw new IllegalStateException(
        "Completed block processing Peano queue not initialized: Start message not received"
      )
    )

  // TODO(#24737) consider removing
  // We sequence NewEpochTopology messages because state transfer can process blocks from multiple epochs
  //  resulting in fetching multiple topologies concurrently.
  @VisibleForTesting
  private[bftordering] val maybeNewEpochTopologyMessagePeanoQueue =
    new SingleUseCell[PeanoQueue[EpochNumber, NewEpochTopology[E]]]
  private def newEpochTopologyMessagePeanoQueue: PeanoQueue[EpochNumber, NewEpochTopology[E]] =
    maybeNewEpochTopologyMessagePeanoQueue.getOrElse(
      throw new IllegalStateException(
        "NewEpochTopology message Peano queue not initialized: no new topologies were being fetched"
      )
    )

  @VisibleForTesting
  private[bftordering] val previousStoredBlock = new PreviousStoredBlock
  startupState.previousBftTimeForOnboarding.foreach { time =>
    previousStoredBlock.update(
      BlockNumber(startupState.initialHeightToProvide - 1),
      time,
    )
  }

  private var currentEpochOrderingTopology: OrderingTopology = startupState.initialOrderingTopology
  private var currentEpochCryptoProvider: CryptoProvider[E] = startupState.initialCryptoProvider
  @VisibleForTesting
  private[bftordering] var currentEpochCouldAlterOrderingTopology =
    startupState.onboardingEpochCouldAlterOrderingTopology

  // Storing metadata is idempotent but we try to avoid unnecessary writes
  private var currentEpochMetadataStored = false

  private val snapshotAdditionalInfoProvider =
    new SequencerSnapshotAdditionalInfoProvider[E](store, epochStoreReader, loggerFactory)

  private val blocksBeingFetched = mutable.Set[BlockNumber]()

  // Used to ensure ordered blocks from an epoch are processed only after the transition to that epoch
  //  has completed, so that epoch-related transient state in this module, which is updated
  //  during the processing of ordered blocks, remains consistent.
  //  It is initialized as soon as the first ordered block is processed, and it is never `None` after that.
  //  It is initialized lazily because the output module currently doesn't know the start epoch in case of
  //  onboarding.
  private var processingFetchedBlocksInEpoch: Option[EpochNumber] = None

  private val leaderSelectionPolicy = DefaultLeaderSelectionPolicy

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  override def receiveInternal(message: Message[E])(implicit
      context: E#ActorContextT[Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {

      case Start =>
        startupState.initialLowerBound.foreach { case (epochNumber, blockNumber) =>
          context
            .blockingAwait(
              store.saveOnboardedNodeLowerBound(epochNumber, blockNumber),
              DefaultDatabaseReadTimeout,
            )
            .fold(error => abort(error), _ => ())
        }

        val lastStoredOutputBlockMetadata =
          context.blockingAwait(store.getLastConsecutiveBlock, DefaultDatabaseReadTimeout)
        val lastStoredBlockNumber = lastStoredOutputBlockMetadata.map(_.blockNumber)

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
            lastAcknowledgedBlockNumber.getOrElse(BlockNumber.First),
            lastStoredBlockNumber.getOrElse(BlockNumber.First),
          ).min

        logger.debug(
          s"Output module bootstrap: last acknowledged block number = $lastAcknowledgedBlockNumber, " +
            s"last stored block number = $lastStoredBlockNumber => recover from block number = $recoverFromBlockNumber"
        )

        // If we are onboarding, rather than an initial node starting or restarting, there will be no actual blocks
        //  stored and the genesis will be returned, but we`ll have a truncated log and we`ll need to start from the
        //  initial height, which will be set correctly by the sequencer runtime as the first block height that we
        //  are expected to serve, not from the genesis height.
        maybeCompletedBlocksProcessingPeanoQueue
          .putIfAbsent(
            new PeanoQueue(
              if (startupState.previousBftTimeForOnboarding.isDefined) {
                val initialHeight = startupState.initialHeightToProvide
                logger.debug(
                  s"Output module bootstrap: onboarding, providing blocks from initial height $initialHeight"
                )
                initialHeight
              } else {
                logger.debug(
                  s"Output module bootstrap: [re-]starting, providing blocks from $recoverFromBlockNumber"
                )
                recoverFromBlockNumber
              }
            )(abort)
          )
          .foreach(_ => abort("Completed block processing Peano Queue has already been set"))
        if (startupState.previousBftTimeForOnboarding.isEmpty) {
          logger.debug(
            s"Output module bootstrap: [re-]starting, [re-]processing blocks from $recoverFromBlockNumber"
          )
          val orderedBlocksToProcess =
            context.blockingAwait(
              epochStoreReader.loadOrderedBlocks(recoverFromBlockNumber),
              DefaultDatabaseReadTimeout,
            )
          // Rehydrate the transient local state containing the previous stored block information (if any)
          //  to ensure that the BFT time is computed correctly even when restarting blocks with
          //  adjusted BFT time.
          context
            .blockingAwait(
              store.getBlock(BlockNumber(recoverFromBlockNumber - 1)),
              DefaultDatabaseReadTimeout,
            )
            .foreach { previousBlock =>
              previousStoredBlock.update(
                previousBlock.blockNumber,
                previousBlock.blockBftTime,
              )
            }
          val startEpochNumber = orderedBlocksToProcess.headOption
            .map(_.orderedBlock.metadata.epochNumber)
            .getOrElse(EpochNumber.First)
          val epochMetadata =
            context.blockingAwait(
              store.getEpoch(startEpochNumber),
              DefaultDatabaseReadTimeout,
            )
          // If an epoch's metadata was not recorded, then it had default values, so we can safely assume that
          //  the epoch could not alter the ordering topology.
          currentEpochCouldAlterOrderingTopology =
            epochMetadata.exists(_.couldAlterOrderingTopology)
          currentEpochMetadataStored = epochMetadata.isDefined
          orderedBlocksToProcess.foreach(orderedBlockForOutput =>
            context.self.asyncSend(BlockOrdered(orderedBlockForOutput))
          )
        }
        initCompleted(receiveInternal)

      case _ =>
        ifInitCompleted(message) {
          case Start =>

          // From local consensus
          case BlockOrdered(
                orderedBlockForOutput @ OrderedBlockForOutput(
                  orderedBlock,
                  _,
                  _,
                  _,
                  mode,
                )
              ) =>
            logger.debug(
              s"output received from local consensus ordered block (mode = $mode) with batch IDs ${orderedBlock.batchRefs}"
            )
            val blockNumber = orderedBlock.metadata.blockNumber
            if (completedBlocksPeanoQueue.alreadyInserted(blockNumber)) {
              // This can happen if we start catching up in the middle of an epoch, as state transfer has epoch granularity.
              logger.debug(s"Skipping block $blockNumber as it's been provided already")
            } else if (!blocksBeingFetched.contains(blockNumber)) {
              // Block batches will be fetched by the availability module either from the local store or,
              //  if unavailable, from remote nodes.
              //  We need to fetch the batches to provide requests, and their BFT sequencing time,
              //  to the sequencer runtime, but this also ensures that all batches are stored locally
              //  when the epoch ends, so that we can provide past block data (e.g. to a re-subscription from
              //  the sequencer runtime after a crash) even if the topology changes drastically afterward.
              context.withNewTraceContext { implicit traceContext =>
                logger.debug(s"Fetching data for block $blockNumber through local availability")
                availability.asyncSendTraced(
                  Availability.LocalOutputFetch.FetchBlockData(orderedBlockForOutput)
                )
              }
              blocksBeingFetched.add(blockNumber).discard
            } else {
              logger.debug(s"Block $blockNumber is already being fetched")
            }

          // From availability
          case BlockDataFetched(completedBlockData) =>
            val orderedBlock = completedBlockData.orderedBlockForOutput.orderedBlock
            val blockNumber = orderedBlock.metadata.blockNumber
            blocksBeingFetched.remove(blockNumber).discard
            logger.debug(
              s"output received completed block; epoch: ${orderedBlock.metadata.epochNumber}, " +
                s"blockID: $blockNumber, batchIDs: ${completedBlockData.batches.map(_._1)}"
            )
            logger.debug(
              s"Inserting block $blockNumber into Peano queue (head=${completedBlocksPeanoQueue.head})"
            )
            completedBlocksPeanoQueue.insert(blockNumber, completedBlockData)
            processFetchedBlocks()

          // Blocks metadata persistence can complete in any order, so relying on mutable state
          //  is generally unsafe in this handler.
          case BlockDataStored(
                orderedBlockData,
                orderedBlockNumber,
                orderedBlockBftTime,
                epochCouldAlterOrderingTopology,
              ) =>
            // If the epoch could alter the ordering topology as a result of this block data,
            //  the epoch metadata was stored before sending this message.
            currentEpochMetadataStored = epochCouldAlterOrderingTopology

            emitRequestsOrderingStats(metrics, orderedBlockData, orderedBlockBftTime)

            // Since consensus will wait for the topology before starting the new epoch, and we send it only when all
            //  blocks, including the last block of the previous epoch, are fully fetched, all blocks can always be read
            //  locally, which is essential because all other nodes could (in principle, although this is definitely
            //  not sensible governance) be swapped in the new epoch, so they would have no past data and would thus
            //  be unable to provide it to us.
            // We fetch the topology once the last block is stored as, based on the returned topology, the last block
            //  might need to be updated with pending topology changes.
            if (orderedBlockData.orderedBlockForOutput.isLastInEpoch)
              fetchNewEpochTopologyIfNeeded(
                orderedBlockData,
                orderedBlockBftTime,
                epochCouldAlterOrderingTopology,
              )

            // This is just a defensive check, as the block subscription will have the head correctly set to the
            //  initial height and will ignore blocks before that, but we cannot check nor enforce this assumption
            //  in this module due to the generic Peano queue type needed for simulation testing support.
            if (lastAcknowledgedBlockNumber.forall(orderedBlockNumber > _)) {
              val isBlockLastInEpoch = orderedBlockData.orderedBlockForOutput.isLastInEpoch
              // We tick the topology even during state transfer;
              //  it ensures that the newly onboarded sequencer sequences (and stores) the same events
              //  as the other sequencers, which in turn makes counters (and snapshots) consistent,
              //  avoiding possible future problems e.g. with pruning and/or BFT onboarding from multiple
              //  sequencer snapshots.
              val tickTopology = isBlockLastInEpoch && epochCouldAlterOrderingTopology
              logger.debug(
                s"Sending block $orderedBlockNumber " +
                  s"(current epoch = ${orderedBlockData.orderedBlockForOutput.orderedBlock.metadata.epochNumber}, " +
                  s"block's BFT time = $orderedBlockBftTime, " +
                  s"block size = ${orderedBlockData.requestsView.size}, " +
                  s"is last in epoch = $isBlockLastInEpoch, " +
                  s"could alter sequencing topology = $epochCouldAlterOrderingTopology, " +
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
                newEpochNumber,
                orderingTopology,
                cryptoProvider: CryptoProvider[E],
              ) =>
            logger.debug(s"Fetched topology $orderingTopology for new epoch $newEpochNumber")

            // We only store metadata for an epoch if it may alter the topology, i.e.,
            //  we never insert `false` and then change it; this avoids updates
            //  and allows leveraging idempotency for easier CFT support.
            if (orderingTopology.areTherePendingCantonTopologyChanges) {
              val outputEpochMetadata =
                OutputEpochMetadata(newEpochNumber, couldAlterOrderingTopology = true)
              logger.debug(s"Storing $outputEpochMetadata")
              pipeToSelf(store.insertEpochIfMissing(outputEpochMetadata)) {
                case Failure(exception) =>
                  abort(s"Failed to store $outputEpochMetadata", exception)
                case Success(_) =>
                  MetadataStoredForNewEpoch(
                    newEpochNumber,
                    orderingTopology,
                    cryptoProvider,
                  )
              }
            } else {
              setupNewEpoch(
                newEpochNumber,
                Some(orderingTopology -> cryptoProvider),
                epochMetadataStored = false,
              )
            }

          case MetadataStoredForNewEpoch(
                newEpochNumber,
                orderingTopology,
                cryptoProvider: CryptoProvider[E],
              ) =>
            logger.debug(
              s"Metadata for new epoch $newEpochNumber successfully stored, setting up the new epoch"
            )
            setupNewEpoch(
              newEpochNumber,
              Some(orderingTopology -> cryptoProvider),
              epochMetadataStored = true,
            )

          case snapshotMessage: SequencerSnapshotMessage =>
            handleSnapshotMessage(snapshotMessage)

          case AsyncException(exception) =>
            abort(s"Failed to retrieve new epoch's topology", exception)

          case NoTopologyAvailable =>
            logger.info("No topology snapshot available due to either shutting down or testing")
        }
    }

  private def processFetchedBlocks()(implicit
      context: E#ActorContextT[Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val orderedBlocks =
      completedBlocksPeanoQueue.pollAvailable {
        case Some(completeBlockData) =>
          val blockEpochNumber =
            completeBlockData.orderedBlockForOutput.orderedBlock.metadata.epochNumber
          val processingEpoch =
            processingFetchedBlocksInEpoch match {
              case None =>
                processingFetchedBlocksInEpoch = Some(blockEpochNumber)
                blockEpochNumber
              case Some(epochNumber) =>
                epochNumber
            }
          blockEpochNumber <= processingEpoch
        case _ => false
      }

    logger.debug(
      s"Polled blocks from Peano queue ${orderedBlocks.map(_.orderedBlockForOutput.orderedBlock.metadata)}"
    )

    // This is the main processing loop where we process blocks in order,
    //  so it is generally safe to use the module's mutable state in it.
    orderedBlocks.foreach { orderedBlockData =>
      val orderedBlock = orderedBlockData.orderedBlockForOutput.orderedBlock
      val orderedBlockNumber = orderedBlock.metadata.blockNumber
      val orderedBlockEpochNumber = orderedBlock.metadata.epochNumber
      val orderedBlockBftTime = previousStoredBlock.computeBlockBftTime(orderedBlock)

      if (
        !currentEpochCouldAlterOrderingTopology && potentiallyAltersSequencersTopology(
          orderedBlockData
        )
      ) {
        logger.debug(
          s"Found potential changes of the sequencing topology in ordered block $orderedBlockNumber " +
            s"in epoch $orderedBlockEpochNumber"
        )
        currentEpochCouldAlterOrderingTopology = true
      }

      val outputBlockMetadata =
        OutputBlockMetadata(
          orderedBlockEpochNumber,
          orderedBlockNumber,
          orderedBlockBftTime,
        )

      logger.debug(
        s"Assigned BFT time $orderedBlockBftTime to block $orderedBlockNumber " +
          s"in epoch $orderedBlockEpochNumber, previous block was $previousStoredBlock"
      )

      previousStoredBlock.update(orderedBlockNumber, orderedBlockBftTime)

      // Capture and pass the relevant mutable state along to prevent
      //  that message handlers running after async calls race on it.
      val couldAlterOrderingTopology = currentEpochCouldAlterOrderingTopology

      // We start storing the metadata for fully-fetched blocks in order, but the completion of
      //  the storage operations can happen in any order; this allows to optimize performance
      //  and the Peano queue in `BlockSubscription` will ensure they are emitted them in the
      //  correct order.
      //  However, we cannot assume any ordering in the `BlockDataStored` handler below, so
      //  we must pass the value of any relevant mutable state along with the message.
      logger.debug(s"Storing $outputBlockMetadata")
      pipeToSelf(
        // We only store metadata for an epoch if it may alter the topology, i.e.,
        //  we never insert `false` and then change it; this avoids updates
        //  and allows leveraging idempotency for easier CFT support.
        if (couldAlterOrderingTopology && !currentEpochMetadataStored) {
          val outputEpochMetadata =
            OutputEpochMetadata(orderedBlockEpochNumber, couldAlterOrderingTopology = true)
          logger.debug(s"Storing $outputEpochMetadata")
          // We store the epoch metadata before the block metadata to ensure that, in case of a crash
          //  and restart from the last stored block, we don't lose the information that the block
          //  could alter the topology.
          //  If the order was reversed, this information loss could happen in the case of a crash
          //  after the block metadata is stored but before the epoch metadata is stored.
          context.flatMapFuture(
            store.insertEpochIfMissing(outputEpochMetadata),
            PureFun.Const(store.insertBlockIfMissing(outputBlockMetadata)),
          )
        } else {
          store.insertBlockIfMissing(outputBlockMetadata)
        }
      ) {
        case Failure(exception) =>
          abort(s"Failed to add block $orderedBlockNumber", exception)
        case Success(_) =>
          BlockDataStored(
            orderedBlockData,
            orderedBlockNumber,
            orderedBlockBftTime,
            couldAlterOrderingTopology,
          )
      }
    }
  }

  private def handleSnapshotMessage(
      message: SequencerSnapshotMessage
  )(implicit context: E#ActorContextT[Message[E]], traceContext: TraceContext): Unit =
    message match {
      case GetAdditionalInfo(timestamp, from) =>
        snapshotAdditionalInfoProvider.provide(timestamp, currentEpochOrderingTopology, from)

      case AdditionalInfo(requester, info) =>
        requester.asyncSend(SequencerNode.SnapshotMessage.AdditionalInfo(info.toProto30))

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
        requestInspector.isRequestToAllMembersOfSynchronizer(
          orderingRequest,
          currentEpochOrderingTopology.maxRequestSizeToDeserialize,
          logger,
          tracedOrderingRequest.traceContext,
        )
    }.isDefined

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  private def fetchNewEpochTopologyIfNeeded(
      lastBlockInEpoch: CompleteBlockData,
      epochLastBlockBftTime: CantonTimestamp,
      epochCouldAlterOrderingTopology: Boolean,
  )(implicit context: E#ActorContextT[Message[E]], traceContext: TraceContext): Unit = {
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
    val newEpochNumber = EpochNumber(completedEpochNumber + 1)

    maybeNewEpochTopologyMessagePeanoQueue
      .putIfAbsent(new PeanoQueue(newEpochNumber)(abort))
      .discard

    if (epochCouldAlterOrderingTopology) {
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
        case Failure(exception) => AsyncException(exception)
        case Success(Some((orderingTopology, cryptoProvider))) =>
          TopologyFetched(newEpochNumber, orderingTopology, cryptoProvider)
        case Success(None) =>
          NoTopologyAvailable
      }
    } else {
      logger.debug(s"Completed epoch $completedEpochNumber that did not change the topology")
      setupNewEpoch(
        newEpochNumber,
        newOrderingTopologyAndCryptoProvider = None,
        epochMetadataStored = false,
      )
    }
  }

  private def setupNewEpoch(
      newEpochNumber: EpochNumber,
      newOrderingTopologyAndCryptoProvider: Option[(OrderingTopology, CryptoProvider[E])],
      epochMetadataStored: Boolean,
  )(implicit
      context: E#ActorContextT[Message[E]],
      traceContext: TraceContext,
  ): Unit = {

    val orderingTopology =
      newOrderingTopologyAndCryptoProvider.fold(currentEpochOrderingTopology)(_._1)
    val newEpochLeaders =
      leaderSelectionPolicy.getLeaders(orderingTopology, newEpochNumber)
    val newMembership = Membership(thisNode, orderingTopology, newEpochLeaders)
    val cryptoProvider =
      newOrderingTopologyAndCryptoProvider.fold(currentEpochCryptoProvider)(_._2)
    logger.debug(
      s"Inserting NewEpochTopology message for epoch $newEpochNumber into Peano queue, " +
        s"(head=$newEpochTopologyMessagePeanoQueue)"
    )
    newEpochTopologyMessagePeanoQueue.insert(
      newEpochNumber,
      Consensus.NewEpochTopology(newEpochNumber, newMembership, cryptoProvider),
    )
    val newEpochTopologyMessages = newEpochTopologyMessagePeanoQueue.pollAvailable()
    logger.debug(
      s"Polled NewEpochTopology messages: $newEpochTopologyMessages from Peano queue"
    )

    newEpochTopologyMessages.foreach { newEpochTopologyMessage =>
      // It is safe to use and change epoch-related mutable state in this block because:
      //  - New epoch messages are processed sequentially and in order.
      //  - Ordered blocks processing, which uses and changes epoch-related mutable state:
      //    - Also happens sequentially and in order.
      //    - Furthermore, only blocks for the current epoch are processed.
      logger.debug(s"Setting up new epoch ${newEpochTopologyMessage.epochNumber}")
      currentEpochCouldAlterOrderingTopology = false
      currentEpochMetadataStored = epochMetadataStored
      processingFetchedBlocksInEpoch = Some(newEpochTopologyMessage.epochNumber)

      currentEpochOrderingTopology = newEpochTopologyMessage.membership.orderingTopology
      currentEpochCryptoProvider = newEpochTopologyMessage.cryptoProvider
      val pendingTopologyChanges = currentEpochOrderingTopology.areTherePendingCantonTopologyChanges
      logger.debug(
        s"Pending topology changes in new ordering topology = $pendingTopologyChanges"
      )
      currentEpochCouldAlterOrderingTopology = pendingTopologyChanges

      metrics.topology.validators.updateValue(currentEpochOrderingTopology.nodes.size)
      logger.debug(
        s"Sending topology $currentEpochOrderingTopology of a new epoch ${newEpochTopologyMessage.epochNumber} " +
          "to a consensus behavior"
      )

      consensus.asyncSend(newEpochTopologyMessage)

      processFetchedBlocks()
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
}

object OutputModule {

  final case class StartupState[E <: Env[E]](
      thisNode: BftNodeId,
      initialHeightToProvide: BlockNumber,
      previousBftTimeForOnboarding: Option[CantonTimestamp],
      onboardingEpochCouldAlterOrderingTopology: Boolean,
      initialCryptoProvider: CryptoProvider[E],
      initialOrderingTopology: OrderingTopology,
      initialLowerBound: Option[(EpochNumber, BlockNumber)],
  )

  @VisibleForTesting
  private[bftordering] final class PreviousStoredBlock {

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var blockNumberAndBftTime: Option[(BlockNumber, CantonTimestamp)] = None

    @VisibleForTesting
    private[bftordering] def getBlockNumberAndBftTime =
      blockNumberAndBftTime

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

    def isRequestToAllMembersOfSynchronizer(
        request: OrderingRequest,
        maxRequestSizeToDeserialize: MaxRequestSizeToDeserialize,
        logger: TracedLogger,
        traceContext: TraceContext,
    )(implicit synchronizerProtocolVersion: ProtocolVersion): Boolean
  }

  object DefaultRequestInspector extends RequestInspector {

    override def isRequestToAllMembersOfSynchronizer(
        request: OrderingRequest,
        maxRequestSizeToDeserialize: MaxRequestSizeToDeserialize,
        logger: TracedLogger,
        traceContext: TraceContext,
    )(implicit synchronizerProtocolVersion: ProtocolVersion): Boolean =
      // TODO(#21615) we should avoid a further deserialization downstream, which would also eliminate
      //  a zip bomb vulnerability in the BUG that could be triggered by byzantine sequencers (#10428)
      deserializeSignedOrderingRequest(synchronizerProtocolVersion, maxRequestSizeToDeserialize)(
        request.payload
      ) match {
        case Right(signedSubmissionRequest) =>
          signedSubmissionRequest.content.content.content.batch.allRecipients
            .contains(AllMembersOfSynchronizer)
        case Left(error) =>
          logger.info(
            s"Skipping ordering request while looking for sequencer events as it failed to deserialize: $error"
          )(traceContext)
          false
      }
  }
}
