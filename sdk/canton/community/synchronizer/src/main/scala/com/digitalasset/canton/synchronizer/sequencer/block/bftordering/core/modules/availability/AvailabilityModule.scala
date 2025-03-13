// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  HasDelayedInit,
  shortType,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  BatchId,
  InProgressBatchMetadata,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequestBatch,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Availability.{
  LocalDissemination,
  RemoteProtocolMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.AvailabilityModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Availability,
  Consensus,
  Mempool,
  Output,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Random, Success, Try}

import AvailabilityModule.*
import AvailabilityModuleMetrics.{emitDisseminationStateStats, emitInvalidMessage}

/** Trantor-inspired availability implementation.
  *
  * @param random
  *   the random source used to select what node to download batches from
  */
final class AvailabilityModule[E <: Env[E]](
    initialMembership: Membership,
    initialCryptoProvider: CryptoProvider[E],
    availabilityStore: data.AvailabilityStore[E],
    config: AvailabilityModuleConfig,
    clock: Clock,
    random: Random,
    metrics: BftOrderingMetrics,
    override val dependencies: AvailabilityModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    disseminationProtocolState: DisseminationProtocolState = new DisseminationProtocolState(),
    outputFetchProtocolState: MainOutputFetchProtocolState = new MainOutputFetchProtocolState(),
)(implicit mc: MetricsContext)
    extends Availability[E]
    with HasDelayedInit[Availability.Message[E]] {

  private val thisNode = initialMembership.myId

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var activeMembership = initialMembership
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var activeCryptoProvider = initialCryptoProvider

  disseminationProtocolState.lastProposalTime = Some(clock.now)

  override def receiveInternal(
      message: Availability.Message[E]
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case Availability.Start =>
        context.self.asyncSend(Availability.Consensus.LocalClockTick)
        initiateMempoolPull(shortType(message))
        initCompleted(receiveInternal)

      case _ =>
        ifInitCompleted(message) {
          case Availability.Start =>

          case Availability.NoOp =>
          case message: Availability.RemoteProtocolMessage =>
            handleRemoteProtocolMessage(message)
          case message: Availability.LocalProtocolMessage[E] =>
            handleLocalProtocolMessage(message)
          case Availability.UnverifiedProtocolMessage(signedMessage) =>
            logger.debug(s"Start to verify message from ${signedMessage.from}")
            pipeToSelf(
              activeCryptoProvider.verifySignedMessage(
                signedMessage,
                AuthenticatedMessageType.BftSignedAvailabilityMessage,
              )
            ) {
              case Failure(exception) =>
                abort(
                  s"Can't verify signature for ${signedMessage.message} (signature ${signedMessage.signature})",
                  exception,
                )
              case Success(Left(exception)) =>
                // Info because it can also happen at epoch boundaries
                logger.info(
                  s"Skipping message since we can't verify signature for ${signedMessage.message} (signature ${signedMessage.signature}) reason=$exception"
                )
                emitInvalidMessage(metrics, signedMessage.from)
                Availability.NoOp
              case Success(Right(())) =>
                logger.debug(s"Verified message is from ${signedMessage.from}")
                signedMessage.message
            }
        }
    }

  private def handleLocalProtocolMessage(
      message: Availability.LocalProtocolMessage[E]
  )(implicit context: E#ActorContextT[Availability.Message[E]], traceContext: TraceContext): Unit =
    message match {
      case message: Availability.LocalDissemination =>
        handleLocalDisseminationMessage(message)
      case message: Availability.LocalOutputFetch =>
        handleLocalOutputFetchMessage(message)
      case message: Availability.Consensus[E] =>
        handleConsensusMessage(message)
    }

  private def handleRemoteProtocolMessage(
      message: Availability.RemoteProtocolMessage
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case message: Availability.RemoteDissemination =>
        handleRemoteDisseminationMessage(message)
      case message: Availability.RemoteOutputFetch =>
        handleRemoteOutputFetchMessage(message)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def handleLocalDisseminationMessage(
      disseminationMessage: Availability.LocalDissemination
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(disseminationMessage)

    disseminationMessage match {
      case Availability.LocalDissemination.LocalBatchCreated(batchId, batch) =>
        logger.debug(s"$messageType: received $batchId from local mempool")
        pipeToSelf(availabilityStore.addBatch(batchId, batch)) {
          case Failure(exception) =>
            abort(s"Failed to add batch $batchId", exception)
          case Success(_) =>
            Availability.LocalDissemination.LocalBatchStored(batchId, batch)
        }
      case Availability.LocalDissemination.LocalBatchStored(batchId, batch) =>
        logger.debug(s"$messageType: persisted local batch $batchId, now signing")
        pipeToSelf(
          activeCryptoProvider.signHash(
            AvailabilityAck.hashFor(batchId, batch.expirationTime, activeMembership.myId)
          )
        )(handleFailure(s"Can't sign batch $batchId") { signature =>
          LocalDissemination.LocalBatchStoredSigned(batchId, batch, signature)
        })

      case LocalDissemination.LocalBatchStoredSigned(batchId, batch, signature) =>
        logger.debug(s"$messageType: signed local batch $batchId")
        disseminationProtocolState.disseminationProgress.update(
          batchId,
          DisseminationProgress(
            activeMembership.orderingTopology,
            InProgressBatchMetadata(batchId, batch.stats, batch.expirationTime),
            Set(AvailabilityAck(thisNode, signature)),
          ),
        )
        updateOutputFetchStatus(batchId)
        // If F == 0, no other nodes are required to store the batch because there is no fault tolerance,
        //  so batches are ready for consensus immediately after being stored locally.
        if (
          updateBatchDisseminationProgress(
            signature,
            batchId,
            thisNode,
            messageType,
          )
        ) {
          shipAvailableConsensusProposals(messageType)
        }

        if (activeMembership.orderingTopology.nodes.nonEmpty) {
          multicast(
            Availability.RemoteDissemination.RemoteBatch.create(batchId, batch, from = thisNode),
            activeMembership.otherNodes,
          )
        }

      case Availability.LocalDissemination.RemoteBatchStored(batchId, expirationTime, from) =>
        logger.debug(s"$messageType: local store persisted $batchId from $from, signing")
        pipeToSelf(
          activeCryptoProvider.signHash(
            AvailabilityAck.hashFor(batchId, expirationTime, activeMembership.myId)
          )
        )(handleFailure(s"Failed to sign $batchId") { signature =>
          LocalDissemination.RemoteBatchStoredSigned(batchId, from, signature)
        })

      case LocalDissemination.RemoteBatchStoredSigned(batchId, from, signature) =>
        logger.debug(s"$messageType: signed $batchId from $from, sending ACK")
        updateOutputFetchStatus(batchId)
        send(
          Availability.RemoteDissemination.RemoteBatchAcknowledged
            .create(
              batchId,
              from = thisNode,
              signature,
            ),
          from,
        )
      case LocalDissemination.RemoteBatchAcknowledgeVerified(batchId, from, signature) =>
        logger.debug(
          s"$messageType: $from sent valid ACK for batch $batchId, " +
            "updating batches ready for ordering"
        )
        if (
          updateBatchDisseminationProgress(
            signature,
            batchId,
            from,
            messageType,
          )
        ) {
          shipAvailableConsensusProposals(messageType)
        }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def handleRemoteDisseminationMessage(
      disseminationMessage: Availability.RemoteDissemination
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(disseminationMessage)

    disseminationMessage match {
      case Availability.RemoteDissemination.RemoteBatch(batchId, batch, from) =>
        logger.debug(s"$messageType: received request from $from to store batch $batchId")
        validateBatch(batchId, batch, from).fold(
          error => logger.warn(error),
          _ =>
            pipeToSelf(availabilityStore.addBatch(batchId, batch)) {
              case Failure(exception) =>
                abort(s"Failed to add batch $batchId", exception)

              case Success(_) =>
                Availability.LocalDissemination
                  .RemoteBatchStored(batchId, batch.expirationTime, from)
            },
        )
      case Availability.RemoteDissemination.RemoteBatchAcknowledged(batchId, from, signature) =>
        disseminationProtocolState.disseminationProgress
          .get(batchId)
          .map(_.batchMetadata.expirationTime) match {
          case Some(expirationTime) =>
            pipeToSelf(
              activeCryptoProvider
                .verifySignature(
                  AvailabilityAck.hashFor(batchId, expirationTime, from),
                  from,
                  signature,
                )
            ) {
              case Failure(exception) =>
                abort(s"Failed to verify $batchId from $from signature: $signature", exception)
              case Success(Left(exception)) =>
                emitInvalidMessage(metrics, from)
                logger.warn(
                  s"$messageType: $from sent invalid ACK for batch $batchId " +
                    s"(signature $signature doesn't match), ignoring",
                  exception,
                )
                Availability.NoOp
              case Success(Right(())) =>
                LocalDissemination.RemoteBatchAcknowledgeVerified(batchId, from, signature)
            }
          case None =>
            logger.info(
              s"$messageType: got a remote ack for batch $batchId from $from " +
                "but the batch is unknown (potentially already proposed), ignoring"
            )
        }

    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def handleLocalOutputFetchMessage(
      outputFetchMessage: Availability.LocalOutputFetch
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(outputFetchMessage)

    outputFetchMessage match {

      case Availability.LocalOutputFetch.FetchBlockData(blockForOutput) =>
        val batchIdsToFind = blockForOutput.orderedBlock.batchRefs.map(_.batchId)
        val request = new BatchesRequest(blockForOutput, mutable.SortedSet.from(batchIdsToFind))
        outputFetchProtocolState.pendingBatchesRequests.append(request)
        fetchBatchesForOutputRequest(request)

      case Availability.LocalOutputFetch.FetchedBlockDataFromStorage(request, result) =>
        result match {
          case AvailabilityStore.MissingBatches(missingBatchIds) =>
            request.missingBatches.filterInPlace(missingBatchIds.contains)
            request.missingBatches.foreach { missingBatchId =>
              // we are missing batches, so for each batch we are missing we will request
              // it from another node until we get all of them again.
              outputFetchProtocolState
                .findProofOfAvailabilityForMissingBatchId(missingBatchId)
                .fold {
                  logger.warn(
                    s"we are missing proof of availability for $missingBatchId, so we don't know nodes to ask."
                  )
                } { proofOfAvailability =>
                  fetchBatchDataFromNodes(
                    messageType,
                    proofOfAvailability,
                    request.blockForOutput.mode,
                  )
                }
            }
            if (request.missingBatches.isEmpty) {
              // this case can happen if:
              // * we stored a missing batch after the fetch request
              // * but the response of the stored occur before the fetch
              fetchBatchesForOutputRequest(request)
            }
          case AvailabilityStore.AllBatches(batches) =>
            // We received all the batches that the output module requested
            // so we can send them to output module
            request.missingBatches.clear()
            dependencies.output.asyncSendTraced(
              Output.BlockDataFetched(CompleteBlockData(request.blockForOutput, batches))
            )
        }
        outputFetchProtocolState.removeRequestsWithNoMissingBatches()

      case Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(batchId, maybeBatch) =>
        maybeBatch match {
          case Some(batch) =>
            logger.debug(
              s"$messageType: $batchId provided by local store"
            )
            outputFetchProtocolState.incomingBatchRequests
              .get(batchId)
              .toList
              .flatMap(_.toSeq)
              .foreach { nodeId =>
                logger.debug(
                  s"$messageType: node '$nodeId' had requested $batchId, sending it"
                )
                send(
                  Availability.RemoteOutputFetch.RemoteBatchDataFetched
                    .create(thisNode, batchId, batch),
                  nodeId,
                )
              }
          case None =>
            logger.debug(s"$messageType: $batchId not found in local store")
        }
        logger.debug(s"$messageType: removing $batchId from incoming batch requests")
        outputFetchProtocolState.incomingBatchRequests.remove(batchId).discard

      case Availability.LocalOutputFetch.FetchedBatchStored(batchId) =>
        outputFetchProtocolState.localOutputMissingBatches.get(batchId) match {
          case Some(_) =>
            logger.info(s"$messageType: $batchId was missing and is now persisted")
            outputFetchProtocolState.localOutputMissingBatches.remove(batchId).discard
            updateOutputFetchStatus(batchId)
          case None =>
            logger.info(s"$messageType: $batchId was not missing")
        }

      case Availability.LocalOutputFetch.FetchRemoteBatchDataTimeout(batchId) =>
        val status = outputFetchProtocolState.localOutputMissingBatches.get(batchId) match {
          case Some(value) => value
          case None =>
            logger.debug(
              s"$messageType: got timeout for batch $batchId that nobody needs, ignoring"
            )
            return
        }
        val (node, remainingNodes) = status.remainingNodesToTry.headOption match {
          case None =>
            logger.warn(
              s"$messageType: got fetch timeout for $batchId but no nodes to try left, " +
                "restarting fetch from the beginning"
            )
            // We tried all nodes and all timed out so we retry all again in the hope that we are just
            //  experiencing temporarily network outage.
            //  We have to keep retrying because the output module is blocked until we get these batches.
            //  If these batches cannot be retrieved, e.g. because the topology has changed too much and/or
            //  the nodes in the PoA are unreachable indefinitely, we'll need to resort (possibly manually)
            //  to state transfer incl. the batch payloads (when it is implemented).
            if (status.mode.isStateTransfer)
              extractNodes(None, useCurrentTopology = true)
            else
              extractNodes(Some(status.originalProof.acks))

          case Some(node) =>
            logger.debug(s"$messageType: got fetch timeout for $batchId, trying fetch from $node")
            (node, status.remainingNodesToTry.drop(1))
        }
        outputFetchProtocolState.localOutputMissingBatches.update(
          batchId,
          MissingBatchStatus(
            batchId,
            status.originalProof,
            remainingNodes,
            status.mode,
          ),
        )
        startDownload(batchId, node)

      // This message is only used for tests
      case Availability.LocalOutputFetch.FetchBatchDataFromNodes(proofOfAvailability, mode) =>
        fetchBatchDataFromNodes(messageType, proofOfAvailability, mode)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def handleRemoteOutputFetchMessage(
      outputFetchMessage: Availability.RemoteOutputFetch
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(outputFetchMessage)

    outputFetchMessage match {
      case Availability.RemoteOutputFetch.FetchRemoteBatchData(batchId, from) =>
        outputFetchProtocolState.incomingBatchRequests
          .updateWith(batchId) {
            case Some(value) =>
              logger.debug(
                s"$messageType: $from also requested batch $batchId (already fetching), noting"
              )
              Some(value.+(from))
            case None =>
              logger.debug(
                s"$messageType received from $from first request for batch $batchId, loading it"
              )
              pipeToSelf(availabilityStore.fetchBatches(Seq(batchId))) {
                case Failure(exception) =>
                  abort(s"failed to fetch batch $batchId", exception)
                case Success(result) =>
                  result match {
                    case AvailabilityStore.MissingBatches(_) =>
                      Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(batchId, None)
                    case AvailabilityStore.AllBatches(Seq((_, result))) =>
                      Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(
                        batchId,
                        Some(result),
                      )
                    case AvailabilityStore.AllBatches(batches) =>
                      logger.error(s"Wrong batches fetched. Requested $batchId got $batches")
                      Availability.LocalOutputFetch.AttemptedBatchDataLoadForNode(batchId, None)
                  }
              }
              Some(Set(from))
          }
          .discard

      case Availability.RemoteOutputFetch.RemoteBatchDataFetched(from, batchId, batch) =>
        validateBatch(batchId, batch, from).fold(
          error => logger.warn(error),
          _ =>
            outputFetchProtocolState.localOutputMissingBatches.get(batchId) match {
              case Some(_) =>
                logger.debug(s"$messageType: received $batchId, persisting it")
                pipeToSelf(availabilityStore.addBatch(batchId, batch)) {
                  case Failure(exception) =>
                    abort(s"Failed to add batch $batchId", exception)
                  case Success(_) =>
                    Availability.LocalOutputFetch.FetchedBatchStored(batchId)
                }
              case None =>
                logger.debug(s"$messageType: received $batchId but nobody needs it, ignoring")
            },
        )
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def fetchBatchDataFromNodes(
      messageType: String,
      proofOfAvailability: ProofOfAvailability,
      mode: OrderedBlockForOutput.Mode,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    if (outputFetchProtocolState.localOutputMissingBatches.contains(proofOfAvailability.batchId)) {
      logger.debug(
        s"$messageType: already trying to download ${proofOfAvailability.batchId}, ignoring"
      )
      return
    }
    if (proofOfAvailability.acks.isEmpty) {
      logger.error(s"$messageType: proof of availability is missing, ignoring")
      return
    }
    val (node, remainingNodes) =
      if (mode.isStateTransfer)
        extractNodes(acks = None, useCurrentTopology = true)
      else
        extractNodes(Some(proofOfAvailability.acks))
    logger.debug(
      s"$messageType: fetch of ${proofOfAvailability.batchId} " +
        s"requested from local store, trying to fetch from $node"
    )
    outputFetchProtocolState.localOutputMissingBatches.update(
      proofOfAvailability.batchId,
      MissingBatchStatus(
        proofOfAvailability.batchId,
        proofOfAvailability,
        remainingNodes,
        mode,
      ),
    )
    startDownload(proofOfAvailability.batchId, node)
  }

  private def updateOutputFetchStatus(
      newlyReceivedBatchId: BatchId
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    outputFetchProtocolState.pendingBatchesRequests.foreach { request =>
      request.missingBatches.remove(newlyReceivedBatchId).discard
      if (request.missingBatches.isEmpty) {
        // we got the last batch needed from an output-module request,
        // but we don't have the data for the batches in memory so we ask the availability store
        fetchBatchesForOutputRequest(request)
      }
    }
    outputFetchProtocolState.removeRequestsWithNoMissingBatches()
  }

  private def fetchBatchesForOutputRequest(
      request: BatchesRequest
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val batchIds = request.blockForOutput.orderedBlock.batchRefs.map(_.batchId)
    pipeToSelf(availabilityStore.fetchBatches(batchIds)) {
      case Failure(exception) =>
        abort(s"Failed to load batches $batchIds", exception)
      case Success(result) =>
        Availability.LocalOutputFetch.FetchedBlockDataFromStorage(request, result)
    }
  }

  private def handleConsensusMessage(
      consensusMessage: Availability.Consensus[E]
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(consensusMessage)

    consensusMessage match {
      case Availability.Consensus.Ack(batchIds) =>
        removeAckedBatches(batchIds, messageType)

      case Availability.Consensus.CreateProposal(
            orderingTopology,
            cryptoProvider: CryptoProvider[E],
            forEpochNumber,
            acks,
          ) =>
        acks.map(_.batchIds).foreach(removeAckedBatches(_, messageType))

        logger.debug(s"$messageType: received block request from local consensus")
        if (activeMembership.orderingTopology != orderingTopology) {
          // Deal with past proposal requests from consensus; some of them may now be complete
          //  because the quorum may be lower in the new topology.
          logger.debug(
            s"$messageType: updating active ordering topology to $orderingTopology and re-checking progress"
          )
          activeMembership = activeMembership.copy(orderingTopology = orderingTopology)
          activeCryptoProvider = cryptoProvider
          reDisseminateBatchesWithStaleTopology()
          updateTopologyInDisseminationProgress()
          if (
            disseminationProtocolState.disseminationProgress
              .map { case (batchId, disseminationProgress) =>
                advanceBatchIfComplete(batchId, disseminationProgress)
              }
              .exists(_ == true)
          ) {
            shipAvailableConsensusProposals(messageType)
          }
        } else {
          logger.debug(s"$messageType: ordering topology is unchanged")
        }

        // Deal with the current proposal request from consensus
        if (disseminationProtocolState.batchesReadyForOrdering.isEmpty) {
          logger.debug(s"$messageType: no batches ready for ordering")
          disseminationProtocolState.toBeProvidedToConsensus enqueue ToBeProvidedToConsensus(
            config.maxBatchesPerProposal,
            forEpochNumber,
          )
          emitDisseminationStateStats(metrics, disseminationProtocolState)
        } else {
          assembleAndSendConsensusProposal(
            ToBeProvidedToConsensus(config.maxBatchesPerProposal, forEpochNumber),
            messageType,
          )
        }

      case Availability.Consensus.LocalClockTick =>
        // If there are no batches to be ordered, but the consensus module is waiting for a proposal and more time has passed since
        // the last one was created than `emptyBlockCreationInterval`, we propose an empty block to the consensus module.
        // That way the consensus module can potentially use it to fill a segment and unblock progress of subsequent segments
        // filled by other remote nodes.
        if (
          disseminationProtocolState.batchesReadyForOrdering.isEmpty &&
          disseminationProtocolState.toBeProvidedToConsensus.nonEmpty &&
          disseminationProtocolState.lastProposalTime
            .exists(ts => (clock.now - ts).toScala > config.emptyBlockCreationInterval)
        ) {
          logger.debug("LocalClockTick: proposing empty block to local consensus")
          val maxBatchesPerProposal =
            disseminationProtocolState.toBeProvidedToConsensus.dequeue()
          assembleAndSendConsensusProposal( // Will propose an empty block
            maxBatchesPerProposal,
            messageType,
          )
        }

        context.delayedEvent(ClockTickInterval, Availability.Consensus.LocalClockTick).discard
    }
  }

  private def removeAckedBatches(
      ackedBatchIds: Seq[BatchId],
      messageType: String,
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Batches $ackedBatchIds have been ordered and acked")
    ackedBatchIds.foreach(disseminationProtocolState.batchesReadyForOrdering.remove(_).discard)
    initiateMempoolPull(messageType)
    emitDisseminationStateStats(metrics, disseminationProtocolState)
  }

  private def startDownload(
      batchId: BatchId,
      node: BftNodeId,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    // We might consider doing parallel downloads in the future, as typically the network between nodes will have high
    //  bandwidth and should be able to support it. However, presently there is no evidence that this is a winning
    //  strategy in a majority of situations.
    context
      .delayedEventTraced(
        config.outputFetchTimeout,
        Availability.LocalOutputFetch.FetchRemoteBatchDataTimeout(batchId),
      )
      .discard
    send(
      Availability.RemoteOutputFetch.FetchRemoteBatchData.create(batchId, from = thisNode),
      node,
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  private def extractNodes(
      acks: Option[Seq[AvailabilityAck]],
      useCurrentTopology: Boolean = false,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): (BftNodeId, Seq[BftNodeId]) = {
    val nodes =
      if (useCurrentTopology) activeMembership.otherNodes.toSeq
      else acks.getOrElse(abort("No availability acks provided for extracting nodes")).map(_.from)
    val shuffled = random.shuffle(nodes)
    shuffled.head -> shuffled.tail
  }

  private def updateTopologyInDisseminationProgress(): Unit =
    disseminationProtocolState.disseminationProgress =
      disseminationProtocolState.disseminationProgress
        .map { case (batchId, disseminationProgress) =>
          val updatedDisseminationProgress =
            disseminationProgress.copy(orderingTopology = activeMembership.orderingTopology)
          batchId -> updatedDisseminationProgress
        }
        .to(mutable.SortedMap)

  private def updateBatchDisseminationProgress(
      signature: Signature,
      batchId: BatchId,
      nodeId: BftNodeId,
      actingOnMessageType: String,
  )(implicit traceContext: TraceContext): ReadyForOrdering = {
    val maybeUpdatedDisseminationProgress =
      disseminationProtocolState.disseminationProgress.updateWith(batchId) {
        case None =>
          logger.info(
            s"$actingOnMessageType: got a store-response for batch $batchId from $nodeId " +
              "but the batch is unknown (potentially already proposed), ignoring"
          )
          None
        case Some(status) =>
          Some(
            DisseminationProgress(
              status.orderingTopology,
              status.batchMetadata,
              status.votes.+(AvailabilityAck(nodeId, signature)),
            )
          )
      }
    maybeUpdatedDisseminationProgress.exists(advanceBatchIfComplete(batchId, _))
  }

  private def advanceBatchIfComplete(
      batchId: BatchId,
      disseminationProgress: DisseminationProgress,
  ): ReadyForOrdering =
    disseminationProgress.proofOfAvailability().fold(false) { proof =>
      // Dissemination completed: remove it now from the progress to avoids clashes with delayed / unneeded ACKs
      disseminationProtocolState.disseminationProgress.remove(batchId).discard
      disseminationProtocolState.batchesReadyForOrdering
        .put(
          batchId,
          disseminationProgress.batchMetadata.complete(proof.acks),
        )
        .discard
      true
    }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def shipAvailableConsensusProposals(
      actingOnMessageType: String
  )(implicit traceContext: TraceContext): Unit =
    while (
      disseminationProtocolState.batchesReadyForOrdering.nonEmpty &&
      disseminationProtocolState.toBeProvidedToConsensus.nonEmpty
    ) {
      val maxBatchesPerProposal =
        disseminationProtocolState.toBeProvidedToConsensus.dequeue()
      assembleAndSendConsensusProposal(
        maxBatchesPerProposal,
        actingOnMessageType,
      )
    }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def assembleAndSendConsensusProposal(
      toBeProvidedToConsensus: ToBeProvidedToConsensus,
      actingOnMessageType: String,
  )(implicit traceContext: TraceContext): Unit = {
    val batchesToBeProposed = // May be empty if no batches are ready for ordering
      disseminationProtocolState.batchesReadyForOrdering.take(
        toBeProvidedToConsensus.maxBatchesPerProposal.toInt
      )
    val proposal =
      Consensus.LocalAvailability.ProposalCreated(
        OrderingBlock(batchesToBeProposed.view.values.map(_.proofOfAvailability).toSeq),
        toBeProvidedToConsensus.forEpochNumber,
      )
    logger.debug(
      s"$actingOnMessageType: providing proposal with batch IDs " +
        s"${proposal.orderingBlock.proofs.map(_.batchId)} to local consensus"
    )
    dependencies.consensus.asyncSend(proposal)
    disseminationProtocolState.lastProposalTime = Some(clock.now)
    emitDisseminationStateStats(metrics, disseminationProtocolState)
  }

  // Currently we just re-disseminate "stale" batches from scratch because we assess that
  //  it's not worth adding complexity just to spare some sends.
  //
  //  Stale batches are in-progress batches with less than 50% probability of completing in the new topology,
  //  plus batches that had completed in the old topology but that are not complete anymore in the new topology.
  // TODO(#23635) Re-evaluate this strategy when we have more data on the actual impact of re-dissemination.
  private def reDisseminateBatchesWithStaleTopology()(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val currentOrderingTopology = activeMembership.orderingTopology
    val (goodInProgress, staleInProgress) =
      disseminationProtocolState.disseminationProgress.partition { case (_, progress) =>
        quorumProbability(
          currentOrderingTopology,
          previousOrderingTopology = progress.orderingTopology,
          votes = progress.votes.map(_.from),
        ) >= QuorumProbabilityRetentionThreshold
      }
    disseminationProtocolState.disseminationProgress = goodInProgress
    val (goodReady, staleReady) = disseminationProtocolState.batchesReadyForOrdering.partition {
      case (_, metadata) =>
        hasQuorum(currentOrderingTopology, metadata.proofOfAvailability.acks.view.map(_.from).toSet)
    }
    disseminationProtocolState.batchesReadyForOrdering = goodReady
    val staleBatchIds = staleReady.keySet ++ staleInProgress.keySet
    if (staleBatchIds.nonEmpty) {
      // Re-disseminate: we need to re-fetch the batches from the store and restart the process by self-sending `LocalBatchStored`
      staleBatchIds.foreach(batchId =>
        pipeToSelf(availabilityStore.fetchBatches(Seq(batchId))) {
          case Success(AvailabilityStore.AllBatches(Seq((_, batch)))) =>
            Availability.LocalDissemination.LocalBatchStored(batchId, batch)
          case other =>
            abort(s"Failed to load batch $batchId for re-dissemination: $other")
        }
      )
    }
  }

  private def initiateMempoolPull(
      messageType: String
  )(implicit traceContext: TraceContext): Unit = {
    // we tell mempool we want enough batches to fill up a proposal in order to make up for the one we just created
    // times the multiplier in order to try to disseminate-ahead batches for a following proposal
    val atMost = config.maxBatchesPerProposal * DisseminateAheadMultiplier -
      // if we have pending batches for ordering we subtract them in order for this buffer to not grow indefinitely
      (disseminationProtocolState.batchesReadyForOrdering.size + disseminationProtocolState.disseminationProgress.size)

    if (atMost > 0) {
      logger.debug(s"$messageType: requesting at most $atMost batches from local mempool")
      dependencies.mempool.asyncSend(Mempool.CreateLocalBatches(atMost.toShort))
    }
  }

  private def handleFailure[ErrorType, Result](
      errorString: => String
  )(handle: Result => Availability.Message[E])(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Try[Either[ErrorType, Result]] => Availability.Message[E] = {
    case Failure(exception) =>
      abort(errorString, exception)
    case Success(Left(exception)) =>
      abort(errorString ++ s": $exception")
    case Success(Right(value)) =>
      handle(value)
  }

  private def send(message: RemoteProtocolMessage, to: BftNodeId)(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    pipeToSelf(
      activeCryptoProvider.signMessage(
        message,
        AuthenticatedMessageType.BftSignedAvailabilityMessage,
      )
    )(
      handleFailure(s"Can't sign message $message") { signedMessage =>
        dependencies.p2pNetworkOut.asyncSendTraced(
          P2PNetworkOut.send(
            P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(signedMessage),
            to,
          )
        )
        Availability.NoOp
      }
    )

  private def multicast(message: RemoteProtocolMessage, nodes: Set[BftNodeId])(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    pipeToSelf(
      activeCryptoProvider.signMessage(
        message,
        AuthenticatedMessageType.BftSignedAvailabilityMessage,
      )
    )(
      handleFailure(s"Can't sign message $message") { signedMessage =>
        dependencies.p2pNetworkOut.asyncSendTraced(
          P2PNetworkOut.Multicast(
            P2PNetworkOut.BftOrderingNetworkMessage.AvailabilityMessage(signedMessage),
            nodes,
          )
        )
        Availability.NoOp
      }
    )

  private def validateBatch(
      batchId: BatchId,
      batch: OrderingRequestBatch,
      from: BftNodeId,
  ): Either[String, Unit] =
    for {
      _ <- Either.cond(
        BatchId.from(batch) == batchId,
        (), {
          emitInvalidMessage(metrics, from)
          s"BatchId doesn't match digest for remote batch from $from, skipping"
        },
      )

      _ <- Either.cond(
        batch.requests.sizeIs <= config.maxRequestsInBatch.toInt,
        (), {
          emitInvalidMessage(metrics, from)
          s"Batch $batchId from '$from' contains more requests (${batch.requests.size}) than allowed " +
            s"(${config.maxRequestsInBatch}), skipping"
        },
      )
    } yield ()
}

object AvailabilityModule {

  private type ReadyForOrdering = Boolean

  // The success probability threshold we use to decide whether to let an in-progress dissemination continue
  //  when the ordering topology has changed.
  //  This is a somewhat arbitrary middle-ground value that is based on an assessment of
  //  the most likely changes, i.e. be adding/removing one node.
  //  Such changes can change the probability in both ways (depending on whether the quorum size changes and/or nodes
  //  from the old/current topologies already voted), so a middle ground seems reasonable.
  private val QuorumProbabilityRetentionThreshold = 0.5

  private val ClockTickInterval = 100.milliseconds

  val DisseminateAheadMultiplier = 2

  def quorum(numberOfNodes: Int): Int = OrderingTopology.weakQuorumSize(numberOfNodes)

  def hasQuorum(orderingTopology: OrderingTopology, votes: Int): Boolean =
    orderingTopology.hasWeakQuorum(votes)

  def hasQuorum(
      orderingTopology: OrderingTopology,
      votes: Set[BftNodeId],
  ): Boolean =
    orderingTopology.hasWeakQuorum(votes)

  private def parseAvailabilityNetworkMessage(
      from: BftNodeId,
      message: v30.AvailabilityMessage,
      originalMessage: ByteString,
  ): ParsingResult[Availability.RemoteProtocolMessage] =
    message.message match {
      case v30.AvailabilityMessage.Message.Empty =>
        Left(ProtoDeserializationError.OtherError("Empty Received"))
      case v30.AvailabilityMessage.Message.Ping(_) =>
        Left(ProtoDeserializationError.OtherError("Ping Received"))
      case v30.AvailabilityMessage.Message.StoreRequest(value) =>
        Availability.RemoteDissemination.RemoteBatch.fromProtoV30(from, value)(originalMessage)
      case v30.AvailabilityMessage.Message.StoreResponse(value) =>
        Availability.RemoteDissemination.RemoteBatchAcknowledged.fromProtoV30(from, value)(
          originalMessage
        )
      case v30.AvailabilityMessage.Message.BatchRequest(value) =>
        Availability.RemoteOutputFetch.FetchRemoteBatchData.fromProtoV30(from, value)(
          originalMessage
        )
      case v30.AvailabilityMessage.Message.BatchResponse(value) =>
        Availability.RemoteOutputFetch.RemoteBatchDataFetched.fromProtoV30(from, value)(
          originalMessage
        )
    }

  def parseNetworkMessage(
      protoSignedMessage: v30.SignedMessage
  ): ParsingResult[Availability.UnverifiedProtocolMessage] =
    SignedMessage
      .fromProtoWithNodeId(v30.AvailabilityMessage)(from =>
        proto =>
          originalByteString => parseAvailabilityNetworkMessage(from, proto, originalByteString)
      )(protoSignedMessage)
      .map(Availability.UnverifiedProtocolMessage.apply)

  private def quorumProbability(
      currentOrderingTopology: OrderingTopology,
      previousOrderingTopology: OrderingTopology,
      votes: Set[BftNodeId],
  ): BigDecimal =
    currentOrderingTopology
      .successProbabilityOfStaleDissemination(
        previousOrderingTopology,
        votes,
      )
}
