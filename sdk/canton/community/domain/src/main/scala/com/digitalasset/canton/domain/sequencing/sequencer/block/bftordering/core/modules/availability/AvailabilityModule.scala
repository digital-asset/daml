// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{Signature, v30}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.metrics.BftOrderingMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1 as proto
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.AvailabilityModule.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.AvailabilityModuleMetrics.{
  emitDisseminationStateStats,
  emitInvalidMessage,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.modules.{
  HasDelayedInit,
  shortType,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  BatchId,
  InProgressBatchMetadata,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.ordering.OrderedBlockForOutput
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.data.{
  CompleteBlockData,
  OrderingRequestBatch,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.Availability.{
  LocalDissemination,
  RemoteProtocolMessage,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.framework.modules.dependencies.AvailabilityModuleDependencies
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.v1.UntypedVersionedMessage
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Random, Success, Try}

/** Trantor-inspired availability implementation.
  * @param random the random source used to select what peer to download batches from
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

  private val thisPeer = initialMembership.myId

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
          case Availability.UnverifiedProtocolMessage(underlyingMessage, signature) =>
            logger.debug(s"Start to verify message from ${underlyingMessage.from}")
            val hashToVerify = RemoteProtocolMessage.hashForSignature(
              underlyingMessage.from,
              underlyingMessage.getCryptographicEvidence,
            )
            pipeToSelf(
              activeCryptoProvider.verifySignature(
                hashToVerify,
                underlyingMessage.from,
                signature,
              )
            ) {
              case Failure(exception) =>
                abort(
                  s"Can't verify signature for $underlyingMessage (signature $signature)",
                  exception,
                )
              case Success(Left(exception)) =>
                logger.warn(
                  s"Skipping message since we can't verify signature for $underlyingMessage (signature $signature) reason=$exception"
                )
                emitInvalidMessage(metrics, underlyingMessage.from)
                Availability.NoOp
              case Success(Right(())) =>
                logger.debug(s"Verified message is from ${underlyingMessage.from}")
                underlyingMessage
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
          activeCryptoProvider.sign(AvailabilityAck.hashFor(batchId, activeMembership.myId))
        )(handleFailure(s"Can't sign batch $batchId") { signature =>
          LocalDissemination.LocalBatchStoredSigned(batchId, batch, signature)
        })

      case LocalDissemination.LocalBatchStoredSigned(batchId, batch, signature) =>
        logger.debug(s"$messageType: signed local batch $batchId")
        disseminationProtocolState.disseminationProgress.update(
          batchId,
          DisseminationProgress(
            activeMembership.orderingTopology,
            InProgressBatchMetadata(batchId, batch.stats),
            Set(AvailabilityAck(thisPeer, signature)),
          ),
        )
        updateOutputFetchStatus(batchId)
        // If F == 0, no other peers are required to store the batch because there is no fault tolerance,
        //  so batches are ready for consensus immediately after being stored locally.
        updateReadyForConsensus(signature, batchId, thisPeer, messageType)
        if (activeMembership.orderingTopology.peers.nonEmpty) {
          multicast(
            Availability.RemoteDissemination.RemoteBatch.create(batchId, batch, from = thisPeer),
            activeMembership.otherPeers,
          )
        }

      case Availability.LocalDissemination.RemoteBatchStored(batchId, from) =>
        logger.debug(s"$messageType: local store persisted $batchId from $from, signing")
        pipeToSelf(
          activeCryptoProvider.sign(AvailabilityAck.hashFor(batchId, activeMembership.myId))
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
              from = thisPeer,
              signature,
            ),
          from,
        )
      case LocalDissemination.RemoteBatchAcknowledgeVerified(batchId, from, signature) =>
        logger.debug(
          s"$messageType: $from sent valid ACK for batch $batchId, " +
            "updating batches ready for ordering"
        )
        updateReadyForConsensus(signature, batchId, from, messageType)
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
        if (!validateBatch(batchId, batch, from)) return
        pipeToSelf(availabilityStore.addBatch(batchId, batch)) {
          case Failure(exception) =>
            abort(s"Failed to add batch $batchId", exception)

          case Success(_) =>
            Availability.LocalDissemination.RemoteBatchStored(batchId, from)
        }
      case Availability.RemoteDissemination.RemoteBatchAcknowledged(batchId, from, signature) =>
        pipeToSelf(
          activeCryptoProvider
            .verifySignature(AvailabilityAck.hashFor(batchId, from), from, signature)
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
              // it from another peer until we get all of them again.
              outputFetchProtocolState
                .findProofOfAvailabilityForMissingBatchId(missingBatchId)
                .fold {
                  logger.warn(
                    s"we are missing proof of availability for $missingBatchId, so we don't know peers to ask."
                  )
                } { proofOfAvailability =>
                  fetchBatchDataFromPeers(
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
            dependencies.output.asyncSend(
              Output.BlockDataFetched(CompleteBlockData(request.blockForOutput, batches))
            )
        }
        outputFetchProtocolState.removeRequestsWithNoMissingBatches()

      case Availability.LocalOutputFetch.AttemptedBatchDataLoadForPeer(batchId, maybeBatch) =>
        maybeBatch match {
          case Some(batch) =>
            logger.debug(
              s"$messageType: $batchId provided by local store"
            )
            outputFetchProtocolState.incomingBatchRequests
              .get(batchId)
              .toList
              .flatMap(_.toSeq)
              .foreach { peerSequencerId =>
                logger.debug(
                  s"$messageType: peer $peerSequencerId had requested $batchId, sending it"
                )
                send(
                  Availability.RemoteOutputFetch.RemoteBatchDataFetched
                    .create(thisPeer, batchId, batch),
                  peerSequencerId,
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
        val (peer, remainingPeers) = status.remainingPeersToTry.headOption match {
          case None =>
            logger.warn(
              s"$messageType: got fetch timeout for $batchId but no peers to try left, " +
                "restarting fetch from the beginning"
            )
            // We tried all peers and all timed out so we retry all again in the hope that we are just
            //  experiencing temporarily network outage.
            //  We have to keep retrying because the output module is blocked until we get these batches.
            //  If these batches cannot be retrieved, e.g. because the topology has changed too much and/or
            //  the peers in the PoA are unreachable indefinitely, we'll need to resort (possibly manually)
            //  to state transfer incl. the batch payloads (when it is implemented).
            // TODO(#19661): Test it
            if (status.mode.isStateTransfer)
              extractPeers(None, useCurrentTopology = true)
            else
              extractPeers(Some(status.originalProof.acks))

          case Some(peer) =>
            logger.debug(s"$messageType: got fetch timeout for $batchId, trying fetch from $peer")
            (peer, status.remainingPeersToTry.drop(1))
        }
        outputFetchProtocolState.localOutputMissingBatches.update(
          batchId,
          MissingBatchStatus(
            batchId,
            status.originalProof,
            remainingPeers,
            status.mode,
          ),
        )
        startDownload(batchId, peer)

      // This message is only used for tests
      case Availability.LocalOutputFetch.FetchBatchDataFromPeers(proofOfAvailability, mode) =>
        fetchBatchDataFromPeers(messageType, proofOfAvailability, mode)
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
                      Availability.LocalOutputFetch.AttemptedBatchDataLoadForPeer(batchId, None)
                    case AvailabilityStore.AllBatches(Seq((_, result))) =>
                      Availability.LocalOutputFetch.AttemptedBatchDataLoadForPeer(
                        batchId,
                        Some(result),
                      )
                    case AvailabilityStore.AllBatches(batches) =>
                      logger.error(s"Wrong batches fetched. Requested $batchId got $batches")
                      Availability.LocalOutputFetch.AttemptedBatchDataLoadForPeer(batchId, None)
                  }
              }
              Some(Set(from))
          }
          .discard

      case Availability.RemoteOutputFetch.RemoteBatchDataFetched(from, batchId, batch) =>
        if (!validateBatch(batchId, batch, from)) return
        outputFetchProtocolState.localOutputMissingBatches.get(batchId) match {
          case Some(_) =>
            logger.debug(s"$messageType: received $batchId, persisting it")
            // The batch is currently trusted, but it will be verified once cryptography is in place
            pipeToSelf(availabilityStore.addBatch(batchId, batch)) {
              case Failure(exception) =>
                abort(s"Failed to add batch $batchId", exception)
              case Success(_) =>
                Availability.LocalOutputFetch.FetchedBatchStored(batchId)
            }
          case None =>
            logger.debug(s"$messageType: received $batchId but nobody needs it, ignoring")
        }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Return"))
  private def fetchBatchDataFromPeers(
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
      // TODO(i14894) we should do more checks that the proof is valid
      logger.warn(s"$messageType: proof of availability is missing, ignoring")
      return
    }
    val (peer, remainingPeers) =
      if (mode.isStateTransfer)
        extractPeers(acks = None, useCurrentTopology = true)
      else
        extractPeers(Some(proofOfAvailability.acks))
    logger.debug(
      s"$messageType: fetch of ${proofOfAvailability.batchId} " +
        s"requested from local store, trying to fetch from $peer"
    )
    outputFetchProtocolState.localOutputMissingBatches.update(
      proofOfAvailability.batchId,
      MissingBatchStatus(
        proofOfAvailability.batchId,
        proofOfAvailability,
        remainingPeers,
        mode,
      ),
    )
    startDownload(proofOfAvailability.batchId, peer)
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
          logger.debug(s"$messageType: updating active ordering topology to $orderingTopology")
          activeMembership = activeMembership.copy(orderingTopology = orderingTopology)
          activeCryptoProvider = cryptoProvider
          updateReadyForConsensus(messageType)
        } else {
          logger.debug(s"$messageType: ordering topology is unchanged")
        }

        resetBatchesWithStaleTopology()

        if (disseminationProtocolState.batchesReadyForOrdering.isEmpty) {
          logger.debug(s"$messageType: no batches ready for ordering")
          disseminationProtocolState.toBeProvidedToConsensus enqueue ToBeProvidedToConsensus(
            config.maxBatchesPerProposal,
            forEpochNumber,
          )
          emitDisseminationStateStats(metrics, disseminationProtocolState)
        } else {
          createAndSendProposal(
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
          createAndSendProposal( // Will propose an empty block
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
      peer: SequencerId,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    // We might consider doing parallel downloads in the future, as typically the network between nodes will have high
    //  bandwidth and should be able to support it. However, presently there is no evidence that this is a winning
    //  strategy in a majority of situations.
    context
      .delayedEvent(
        config.outputFetchTimeout,
        Availability.LocalOutputFetch.FetchRemoteBatchDataTimeout(batchId),
      )
      .discard
    send(
      Availability.RemoteOutputFetch.FetchRemoteBatchData.create(batchId, from = thisPeer),
      peer,
    )
  }

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  private def extractPeers(
      acks: Option[Seq[AvailabilityAck]],
      useCurrentTopology: Boolean = false,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): (SequencerId, Seq[SequencerId]) = {
    val peers =
      if (useCurrentTopology) activeMembership.otherPeers.toSeq
      else acks.getOrElse(abort("No availability acks provided for extracting peers")).map(_.from)
    val shuffled = random.shuffle(peers)
    shuffled.head -> shuffled.tail
  }

  private def updateReadyForConsensus(
      messageType: String
  )(implicit traceContext: TraceContext): Unit =
    disseminationProtocolState.disseminationProgress.foreach {
      case (batchId, disseminationProgress) =>
        val updatedDisseminationProgress =
          disseminationProgress.copy(orderingTopology = activeMembership.orderingTopology)
        completeDisseminationIfPoAAvailable(batchId, messageType, updatedDisseminationProgress)
    }

  private def updateReadyForConsensus(
      signature: Signature,
      batchId: BatchId,
      peer: SequencerId,
      messageType: String,
  )(implicit traceContext: TraceContext): Unit = {
    val maybeUpdatedProgressStatus =
      disseminationProtocolState.disseminationProgress.updateWith(batchId) {
        case None =>
          logger.info(
            s"$messageType: got a store-response for batch $batchId from $peer " +
              "but the batch is unknown (potentially already proposed), ignoring"
          )
          None
        case Some(status) =>
          Some(
            DisseminationProgress(
              status.orderingTopology,
              status.batchMetadata,
              status.votes.+(AvailabilityAck(peer, signature)),
            )
          )
      }
    maybeUpdatedProgressStatus.foreach(completeDisseminationIfPoAAvailable(batchId, messageType, _))
  }

  private def completeDisseminationIfPoAAvailable(
      batchId: BatchId,
      messageType: String,
      disseminationProgress: DisseminationProgress,
  )(implicit traceContext: TraceContext): Unit =
    disseminationProgress.proofOfAvailability().foreach { proof =>
      // Dissemination completed: remove it now from the progress to avoids clashes with delayed / unneeded ACKs
      disseminationProtocolState.disseminationProgress.remove(batchId).discard
      disseminationProtocolState.batchesReadyForOrdering
        .put(batchId, disseminationProgress.batchMetadata.complete(proof.acks))
        .discard
      createAndSendProposals(messageType)
    }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def createAndSendProposals(
      messageType: String
  )(implicit traceContext: TraceContext): Unit =
    while (
      disseminationProtocolState.batchesReadyForOrdering.nonEmpty &&
      disseminationProtocolState.toBeProvidedToConsensus.nonEmpty
    ) {
      val maxBatchesPerProposal =
        disseminationProtocolState.toBeProvidedToConsensus.dequeue()
      createAndSendProposal(
        maxBatchesPerProposal,
        messageType,
      )
    }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def createAndSendProposal(
      toBeProvidedToConsensus: ToBeProvidedToConsensus,
      messageType: String,
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
      s"$messageType: providing proposal with batch IDs " +
        s"${proposal.orderingBlock.proofs.map(_.batchId)} to local consensus"
    )
    dependencies.consensus.asyncSend(proposal)
    disseminationProtocolState.lastProposalTime = Some(clock.now)
    emitDisseminationStateStats(metrics, disseminationProtocolState)
  }

  private def resetBatchesWithStaleTopology()(implicit
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

  private def send(message: RemoteProtocolMessage, to: SequencerId)(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val (serializedMessage, hashToSign) = dependencies.serializer.messageForTransport(message)

    pipeToSelf(activeCryptoProvider.sign(hashToSign))(
      handleFailure(s"Can't sign message $message") { signature =>
        dependencies.p2pNetworkOut.asyncSend(
          P2PNetworkOut.send(
            RemoteProtocolMessage.toProto(serializedMessage),
            Some(signature.toProtoV30),
            to,
          )
        )
        Availability.NoOp
      }
    )
  }

  private def multicast(message: RemoteProtocolMessage, peers: Set[SequencerId])(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val (serializedMessage, hashToSign) = dependencies.serializer.messageForTransport(message)

    pipeToSelf(activeCryptoProvider.sign(hashToSign))(
      handleFailure(s"Can't sign message $message") { signature =>
        dependencies.p2pNetworkOut.asyncSend(
          P2PNetworkOut.Multicast(
            RemoteProtocolMessage.toProto(serializedMessage),
            Some(signature.toProtoV30),
            peers,
          )
        )
        Availability.NoOp
      }
    )
  }

  private def validateBatch(
      batchId: BatchId,
      batch: OrderingRequestBatch,
      from: SequencerId,
  )(implicit traceContext: TraceContext): Boolean =
    if (BatchId.from(batch) != batchId) {
      logger.warn(s"BatchId doesn't match digest for remote batch from $from, skipping")
      emitInvalidMessage(metrics, from)
      false
    } else {
      true
    }
}

object AvailabilityModule {

  // The success probability threshold we use to decide whether to let an in-progress dissemination continue
  //  when the ordering topology has changed.
  //  This is a somewhat arbitrary middle-ground value that is based on an assessment of
  //  the most likely changes, i.e. be adding/removing one node.
  //  Such changes can change the probability in both ways (depending on whether the quorum size changes and/or peers
  //  from the old/current topologies already voted), so a middle ground seems reasonable.
  private val QuorumProbabilityRetentionThreshold = 0.5

  private val ClockTickInterval = 100.milliseconds

  val DisseminateAheadMultiplier = 2

  def quorum(numberOfNodes: Int): Int = OrderingTopology.weakQuorumSize(numberOfNodes)

  def hasQuorum(orderingTopology: OrderingTopology, votes: Int): Boolean =
    orderingTopology.hasWeakQuorum(votes)

  def hasQuorum(
      orderingTopology: OrderingTopology,
      votes: Set[SequencerId],
  ): Boolean =
    orderingTopology.hasWeakQuorum(votes)

  private def parseAvailabilityNetworkMessage(
      from: SequencerId,
      message: proto.AvailabilityMessage,
      originalMessage: ByteString,
      protoSignature: Option[v30.Signature],
  ): ParsingResult[Availability.UnverifiedProtocolMessage] = for {
    underlyingMessage <- (message.message match {
      case proto.AvailabilityMessage.Message.Empty =>
        Left(ProtoDeserializationError.OtherError("Empty Received"))
      case proto.AvailabilityMessage.Message.Ping(_) =>
        Left(ProtoDeserializationError.OtherError("Ping Received"))
      case proto.AvailabilityMessage.Message.StoreRequest(value) =>
        Availability.RemoteDissemination.RemoteBatch.fromProtoV30(from, value)(originalMessage)
      case proto.AvailabilityMessage.Message.StoreResponse(value) =>
        Availability.RemoteDissemination.RemoteBatchAcknowledged.fromProtoV30(from, value)(
          originalMessage
        )
      case proto.AvailabilityMessage.Message.BatchRequest(value) =>
        Availability.RemoteOutputFetch.FetchRemoteBatchData.fromProtoV30(from, value)(
          originalMessage
        )
      case proto.AvailabilityMessage.Message.BatchResponse(value) =>
        Availability.RemoteOutputFetch.RemoteBatchDataFetched.fromProtoV30(from, value)(
          originalMessage
        )
    }): Either[ProtoDeserializationError, RemoteProtocolMessage]
    signature <- Signature.fromProtoV30(protoSignature.getOrElse(Signature.noSignature.toProtoV30))
  } yield Availability.UnverifiedProtocolMessage(underlyingMessage, signature)

  def parseNetworkMessage(
      from: SequencerId,
      message: ByteString,
      protoSignature: Option[v30.Signature],
  ): ParsingResult[Availability.UnverifiedProtocolMessage] =
    for {
      versionedMessage <- ProtoConverter.protoParser(UntypedVersionedMessage.parseFrom)(message)
      unVersionedBytes <- versionedMessage.wrapper.data.toRight(
        ProtoDeserializationError.OtherError("Missing data in UntypedVersionedMessage")
      )
      parsedMessage <- ProtoConverter.protoParser(proto.AvailabilityMessage.parseFrom)(
        unVersionedBytes
      )
      x <- parseAvailabilityNetworkMessage(
        from,
        parsedMessage,
        message,
        protoSignature,
      )
    } yield x

  private def quorumProbability(
      currentOrderingTopology: OrderingTopology,
      previousOrderingTopology: OrderingTopology,
      votes: Set[SequencerId],
  ): Double =
    currentOrderingTopology
      .successProbabilityOfStaleDissemination(
        previousOrderingTopology,
        votes,
      )
}
