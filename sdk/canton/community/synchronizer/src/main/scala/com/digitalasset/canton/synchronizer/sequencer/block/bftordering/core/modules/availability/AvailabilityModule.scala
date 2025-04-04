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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.FingerprintKeyId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.data.AvailabilityStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  HasDelayedInit,
  shortType,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.Env
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
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
  MessageAuthorizer,
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.utils.BftNodeShuffler
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.*
import scala.util.{Failure, Random, Success, Try}

import AvailabilityModuleMetrics.{emitDisseminationStateStats, emitInvalidMessage}

/** Trantor-inspired availability implementation.
  *
  * @param random
  *   the random source used to select what node to download batches from
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class AvailabilityModule[E <: Env[E]](
    initialMembership: Membership,
    initialEpochNumber: EpochNumber,
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
)(
    // Only passed in tests
    private var messageAuthorizer: MessageAuthorizer = initialMembership.orderingTopology
)(implicit
    mc: MetricsContext
) extends Availability[E]
    with HasDelayedInit[Availability.Message[E]] {

  import AvailabilityModule.*

  private val thisNode = initialMembership.myId
  private val nodeShuffler = new BftNodeShuffler(random)

  private var lastKnownEpochNumber = initialEpochNumber

  private var activeMembership = initialMembership
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

          case message: Availability.LocalProtocolMessage[E] =>
            handleLocalProtocolMessage(message)

          case message: Availability.RemoteProtocolMessage =>
            handleRemoteProtocolMessage(message)

          case Availability.UnverifiedProtocolMessage(signedMessage) =>
            val from = signedMessage.from
            val keyId = FingerprintKeyId.toBftKeyId(signedMessage.signature.signedBy)
            if (messageAuthorizer.isAuthorized(from, keyId)) {
              logger.debug(s"Start to verify message from '$from'")
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
                  emitInvalidMessage(metrics, from)
                  Availability.NoOp
                case Success(Right(())) =>
                  logger.debug(s"Verified message is from '$from''")
                  signedMessage.message
              }
            } else {
              logger.info(
                s"Received a message from '$from' signed with '$keyId' " +
                  "but it cannot be verified in the currently known " +
                  s"dissemination topology ${activeMembership.orderingTopology.nodesTopologyInfo}, dropping it"
              )
            }
        }
    }

  private def handleLocalProtocolMessage(
      message: Availability.LocalProtocolMessage[E]
  )(implicit context: E#ActorContextT[Availability.Message[E]], traceContext: TraceContext): Unit =
    message match {
      case message: Availability.LocalDissemination =>
        handleLocalDisseminationMessage(message)

      case message: Availability.Consensus[E] =>
        handleConsensusMessage(message)

      case message: Availability.LocalOutputFetch =>
        handleLocalOutputFetchMessage(message)
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
      case Availability.LocalDissemination.LocalBatchCreated(requests) =>
        val batch = OrderingRequestBatch.create(requests, lastKnownEpochNumber)
        val batchId = BatchId.from(batch)

        logger.debug(s"$messageType: received $batchId from local mempool")
        pipeToSelf(availabilityStore.addBatch(batchId, batch)) {
          case Failure(exception) =>
            abort(s"Failed to add batch $batchId", exception)
          case Success(_) =>
            Availability.LocalDissemination.LocalBatchesStored(Seq(batchId -> batch))
        }

      case Availability.LocalDissemination.LocalBatchesStored(batches) =>
        logger.debug(s"$messageType: persisted local batches ${batches.map(_._1)}, now signing")
        signLocalBatchesAndContinue(batches)

      case Availability.LocalDissemination.RemoteBatchStored(batchId, epochNumber, from) =>
        logger.debug(s"$messageType: local store persisted $batchId from $from, signing")
        signRemoteBatchAndContinue(batchId, epochNumber, from)

      case LocalDissemination.LocalBatchesStoredSigned(batches) =>
        disseminateLocalBatches(messageType, batches)

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
          to = from,
        )

      case LocalDissemination.RemoteBatchAcknowledgeVerified(batchId, from, signature) =>
        logger.debug(
          s"$messageType: $from sent valid ACK for batch $batchId, " +
            "updating batches ready for ordering"
        )
        updateAndAdvanceSingleDisseminationProgress(messageType, batchId, Some(from -> signature))
    }
  }

  private def signRemoteBatchAndContinue(
      batchId: BatchId,
      epochNumber: EpochNumber,
      from: BftNodeId,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    pipeToSelf(
      activeCryptoProvider.signHash(
        AvailabilityAck.hashFor(batchId, epochNumber, activeMembership.myId)
      )
    )(handleFailure(s"Failed to sign $batchId") { signature =>
      LocalDissemination.RemoteBatchStoredSigned(batchId, from, signature)
    })

  private def signLocalBatchesAndContinue(batches: Seq[(BatchId, OrderingRequestBatch)])(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    pipeToSelf(
      context.sequenceFuture(batches.map { case (batchId, batch) =>
        activeCryptoProvider.signHash(
          AvailabilityAck.hashFor(batchId, batch.epochNumber, activeMembership.myId)
        )
      })
    ) {
      case Failure(exception) =>
        abort("Failed to sign local batches", exception)
      case Success(results) =>
        val (errors, signatures) = results.partitionMap(identity)
        if (errors.nonEmpty) {
          abort(s"Failed to sign local batches: ${errors.map(_.toString)}")
        } else {
          Availability.LocalDissemination.LocalBatchesStoredSigned(
            batches.zip(signatures).map { case ((batchId, batch), signature) =>
              Availability.LocalDissemination
                .LocalBatchStoredSigned(batchId, batch, Right(signature))
            }
          )
        }
    }

  private def disseminateLocalBatches(
      actingOnMessageType: => String,
      batches: Seq[Availability.LocalDissemination.LocalBatchStoredSigned],
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    logger.debug(
      s"$actingOnMessageType: local batches ${batches.map(_.batchId)} ready to disseminate"
    )

    batches.foreach {
      case Availability.LocalDissemination.LocalBatchStoredSigned(
            batchId,
            batch,
            progressOrSignature,
          ) =>
        val progress =
          progressOrSignature.fold(
            identity,
            signature =>
              DisseminationProgress(
                activeMembership.orderingTopology,
                InProgressBatchMetadata(batchId, batch.epochNumber, batch.stats),
                Set(AvailabilityAck(thisNode, signature)),
              ),
          )
        logger.debug(s"$actingOnMessageType: progress of $batchId is $progress")
        disseminationProtocolState.disseminationProgress.put(batchId, progress).discard

        // If F == 0, no other nodes are required to store the batch because there is no fault tolerance,
        //  so batches are ready for consensus immediately after being stored locally.
        updateAndAdvanceSingleDisseminationProgress(
          actingOnMessageType,
          batchId,
          voteToAdd = None,
        )

        if (activeMembership.otherNodes.nonEmpty) {
          multicast(
            message =
              Availability.RemoteDissemination.RemoteBatch.create(batchId, batch, from = thisNode),
            nodes = activeMembership.otherNodes.diff(progress.acks.map(_.from)),
          )
        }
    }
  }

  private def updateAndAdvanceSingleDisseminationProgress(
      actingOnMessageType: => String,
      batchId: BatchId,
      voteToAdd: Option[(BftNodeId, Signature)],
  )(implicit
      traceContext: TraceContext
  ): Unit =
    if (
      updateBatchDisseminationProgress(
        actingOnMessageType,
        batchId,
        voteToAdd,
      )
    ) {
      shipAvailableConsensusProposals(actingOnMessageType)
    }

  private def updateBatchDisseminationProgress(
      actingOnMessageType: => String,
      batchId: BatchId,
      voteToAdd: Option[(BftNodeId, Signature)],
  )(implicit traceContext: TraceContext): ReadyForOrdering = {
    val maybeUpdatedDisseminationProgress =
      disseminationProtocolState.disseminationProgress.updateWith(batchId) {
        case None =>
          val fromNodeString = voteToAdd.map(_._1).map(node => s"'$node'").getOrElse("this node")
          logger.info(
            s"$actingOnMessageType: got a store-response for batch $batchId " +
              s"from $fromNodeString but the batch is unknown (potentially already proposed), ignoring"
          )
          None
        case Some(status) =>
          Some(
            DisseminationProgress(
              status.orderingTopology,
              status.batchMetadata,
              status.acks ++ voteToAdd.flatMap { case (from, signature) =>
                // Reliable deduplication: since we may be re-requesting votes, we need to
                // ensure that we don't add different valid signatures from the same node
                if (status.acks.map(_.from).contains(from))
                  None
                else
                  Some(AvailabilityAck(from, signature))
              },
            )
          )
      }
    maybeUpdatedDisseminationProgress.exists(
      advanceBatchIfComplete(actingOnMessageType, batchId, _)
    )
  }

  private def updateLastKnownEpochNumberAndForgetExpiredBatches(
      actingOnMessageType: => String,
      currentEpoch: EpochNumber,
  )(implicit
      traceContext: TraceContext,
      context: E#ActorContextT[Availability.Message[E]],
  ): Unit =
    if (currentEpoch < lastKnownEpochNumber) {
      abort(
        s"Trying to update lastKnownEpochNumber in Availability module to $currentEpoch which is lower than the current value $lastKnownEpochNumber"
      )
    } else if (lastKnownEpochNumber != currentEpoch) {
      lastKnownEpochNumber = currentEpoch

      def deleteExpiredBatches[M](
          map: mutable.Map[BatchId, M],
          mapName: String,
      )(getEpochNumber: M => EpochNumber): Unit = {
        def isBatchExpired(batchEpochNumber: EpochNumber) =
          batchEpochNumber <= lastKnownEpochNumber - OrderingRequestBatch.BatchValidityDurationEpochs
        val expiredBatchIds = map.collect {
          case (batchId, metadata) if isBatchExpired(getEpochNumber(metadata)) => batchId
        }
        if (expiredBatchIds.nonEmpty) {
          logger.warn(
            s"$actingOnMessageType: Discarding from $mapName the expired batches: $expiredBatchIds"
          )
          map --= expiredBatchIds
        }
      }

      deleteExpiredBatches(
        disseminationProtocolState.batchesReadyForOrdering,
        "batchesReadyForOrdering",
      )(_.epochNumber)

      deleteExpiredBatches(
        disseminationProtocolState.disseminationProgress,
        "disseminationProgress",
      )(_.batchMetadata.epochNumber)
    }

  private def handleConsensusMessage(
      consensusMessage: Availability.Consensus[E]
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    lazy val messageType = shortType(consensusMessage)

    consensusMessage match {
      case Availability.Consensus.Ordered(batchIds) =>
        removeOrderedBatchesAndPullFromMempool(messageType, batchIds)

      case Availability.Consensus.CreateProposal(
            orderingTopology,
            cryptoProvider: CryptoProvider[E],
            forEpochNumber,
            ordered,
          ) =>
        updateLastKnownEpochNumberAndForgetExpiredBatches(messageType, forEpochNumber)
        handleConsensusProposalRequest(
          messageType,
          orderingTopology,
          cryptoProvider,
          forEpochNumber,
          ordered,
        )

      case Availability.Consensus.LocalClockTick =>
        // If there are no batches to be ordered, but the consensus module is waiting for a proposal
        //  and more time has passed since the last one was created than `emptyBlockCreationInterval`,
        //  we propose an empty block to the consensus module.
        //  That way the consensus module can potentially use it to fill a segment and unblock progress
        //  of subsequent segments filled by other remote nodes.
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
            messageType,
            maxBatchesPerProposal,
          )
        }

        context.delayedEvent(ClockTickInterval, Availability.Consensus.LocalClockTick).discard
    }
  }

  private def handleConsensusProposalRequest(
      actingOnMessageType: => String,
      orderingTopology: OrderingTopology,
      cryptoProvider: CryptoProvider[E],
      forEpochNumber: EpochNumber,
      orderedBatchIds: Seq[BatchId],
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    removeOrderedBatchesAndPullFromMempool(actingOnMessageType, orderedBatchIds)

    logger.debug(
      s"$actingOnMessageType: recording block request from local consensus, " +
        s"updating active ordering topology to $orderingTopology and reviewing progress"
    )
    disseminationProtocolState.toBeProvidedToConsensus enqueue ToBeProvidedToConsensus(
      config.maxBatchesPerProposal,
      forEpochNumber,
    )
    activeMembership = activeMembership.copy(orderingTopology = orderingTopology)
    activeCryptoProvider = cryptoProvider
    messageAuthorizer = orderingTopology

    // Review and complete both in-progress and ready disseminations regardless of whether the topology
    //  has changed, so that we also try and complete ones that might have become stuck;
    //  note that a topology change may also cause in-progress disseminations to complete without
    //  further acks due to a quorum reduction.

    syncWithTopologyAllDisseminationProgress(actingOnMessageType)
    advanceAllDisseminationProgress(actingOnMessageType)

    emitDisseminationStateStats(metrics, disseminationProtocolState)
  }

  private def removeOrderedBatchesAndPullFromMempool(
      actingOnMessageType: => String,
      orderedBatches: Seq[BatchId],
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(
      s"$actingOnMessageType: batches $orderedBatches have been acked from consensus as ordered"
    )
    orderedBatches.foreach(disseminationProtocolState.batchesReadyForOrdering.remove(_).discard)
    initiateMempoolPull(actingOnMessageType)
    emitDisseminationStateStats(metrics, disseminationProtocolState)
  }

  private def syncWithTopologyAllDisseminationProgress(actingOnMessageType: => String)(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    val currentOrderingTopology = activeMembership.orderingTopology

    reviewInProgressBatches(actingOnMessageType)
    reviewBatchesReadyForOrdering(actingOnMessageType)

    val batchesThatNeedSigning = mutable.ListBuffer[BatchId]()
    val batchesThatNeedMoreVotes = mutable.ListBuffer[(BatchId, DisseminationProgress)]()

    // Review all in-progress disseminations
    disseminationProtocolState.disseminationProgress =
      disseminationProtocolState.disseminationProgress.flatMap {
        case (batchId, disseminationProgress) =>
          disseminationProgress
            .voteOf(thisNode) match {
            case None =>
              // Needs re-signing
              batchesThatNeedSigning.addOne(batchId)
              None
            case _ =>
              if (
                disseminationProgress.proofOfAvailability().isEmpty &&
                disseminationProgress.acks.sizeIs < currentOrderingTopology.size
              ) {
                // Needs more votes
                batchesThatNeedMoreVotes.addOne(batchId -> disseminationProgress)
              }
              Some(batchId -> disseminationProgress)
          }
      }

    if (batchesThatNeedSigning.sizeIs > 0)
      fetchBatchesAndThenSelfSend(batchesThatNeedSigning)(
        // Will trigger signing and then further dissemination
        Availability.LocalDissemination.LocalBatchesStored(_)
      )

    if (batchesThatNeedMoreVotes.sizeIs > 0)
      fetchBatchesAndThenSelfSend(batchesThatNeedMoreVotes.map(_._1)) { batches =>
        Availability.LocalDissemination.LocalBatchesStoredSigned(
          batches.zip(batchesThatNeedMoreVotes.map(_._2)).map { case ((batchId, batch), progress) =>
            // Will trigger further dissemination
            Availability.LocalDissemination
              .LocalBatchStoredSigned(batchId, batch, Left(progress))
          }
        )
      }
  }

  private def reviewInProgressBatches(
      actingOnMessageType: => String
  )(implicit traceContext: TraceContext): Unit = {
    val currentOrderingTopology = activeMembership.orderingTopology

    disseminationProtocolState.disseminationProgress =
      disseminationProtocolState.disseminationProgress.map { case (batchId, progress) =>
        val reviewedProgress = progress.review(currentOrderingTopology)
        debugLogReviewedProgressIfAny(
          actingOnMessageType,
          currentOrderingTopology,
          batchId,
          progress.acks,
          reviewedProgress.acks,
        )
        batchId -> reviewedProgress
      }
  }

  private def reviewBatchesReadyForOrdering(
      actingOnMessageType: => String
  )(implicit traceContext: TraceContext): Unit = {
    val currentOrderingTopology = activeMembership.orderingTopology

    // Consider everything as in progress again by converting batches that were
    //  previously ready for ordering back into in-progress dissemination state, and
    //  concatenating them into the single `disseminationProgress` map.
    disseminationProtocolState.disseminationProgress ++=
      disseminationProtocolState.batchesReadyForOrdering
        .map { case (batchId, disseminatedBatchMetadata) =>
          val reviewedProgress =
            DisseminationProgress.reviewReadyForOrdering(
              disseminatedBatchMetadata,
              currentOrderingTopology,
            )
          val originalAcks = disseminatedBatchMetadata.proofOfAvailability.acks.toSet
          debugLogReviewedProgressIfAny(
            actingOnMessageType,
            currentOrderingTopology,
            batchId,
            originalAcks,
            reviewedProgress.acks,
          )
          batchId -> reviewedProgress
        }
    disseminationProtocolState.batchesReadyForOrdering.clear()
  }

  private def fetchBatchesAndThenSelfSend(
      batchIds: Iterable[BatchId]
  )(f: Seq[(BatchId, OrderingRequestBatch)] => LocalDissemination)(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    pipeToSelf(availabilityStore.fetchBatches(batchIds.toSeq)) {
      case Failure(error) =>
        abort("Failed to fetch batches", error)
      case Success(AvailabilityStore.MissingBatches(missingBatchIds)) =>
        abort(s"Some batches couldn't be fetched: $missingBatchIds")
      case Success(AvailabilityStore.AllBatches(batches)) =>
        f(batches)
    }

  private def debugLogReviewedProgressIfAny(
      actingOnMessageType: => String,
      currentOrderingTopology: OrderingTopology,
      batchId: BatchId,
      originalAcks: Set[AvailabilityAck],
      reviewedAcks: Set[AvailabilityAck],
  )(implicit traceContext: TraceContext): Unit =
    if (reviewedAcks != originalAcks)
      logger.debug(
        s"$actingOnMessageType: updated dissemination acks for previously competed batch $batchId " +
          s"from $originalAcks to $reviewedAcks " +
          s"due to the new topology $currentOrderingTopology"
      )

  private def advanceAllDisseminationProgress(actingOnMessageType: => String)(implicit
      traceContext: TraceContext
  ): Unit = {
    val atLeastOneDisseminationWasCompleted =
      disseminationProtocolState.disseminationProgress
        .map { case (batchId, disseminationProgress) =>
          advanceBatchIfComplete(actingOnMessageType, batchId, disseminationProgress)
        }
        .exists(identity)

    if (atLeastOneDisseminationWasCompleted)
      shipAvailableConsensusProposals(actingOnMessageType)
  }

  private def advanceBatchIfComplete(
      actingOnMessageType: => String,
      batchId: BatchId,
      disseminationProgress: DisseminationProgress,
  )(implicit traceContext: TraceContext): ReadyForOrdering =
    disseminationProgress.proofOfAvailability().fold(false) { proof =>
      logger.debug(
        s"$actingOnMessageType: $batchId has completed dissemination in ${disseminationProgress.orderingTopology}"
      )
      // Dissemination completed: remove it now from the progress to avoids clashes with delayed / unneeded ACKs
      disseminationProtocolState.disseminationProgress.remove(batchId).discard
      disseminationProtocolState.batchesReadyForOrdering
        .put(batchId, disseminationProgress.batchMetadata.complete(proof.acks))
        .discard
      true
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
                  .RemoteBatchStored(batchId, batch.epochNumber, from)
            },
        )

      case Availability.RemoteDissemination.RemoteBatchAcknowledged(batchId, from, signature) =>
        disseminationProtocolState.disseminationProgress.get(batchId) match {
          case Some(disseminationProgress) =>
            // Best-effort deduplication: if a node receives an AvailabilityAck from a peer
            // for a batchId that already exists, we can drop that Ack immediately, without
            // bothering to check the signature using the active crypto provider.
            // However, a duplicate ack may be received while the first Ack's signature is
            // being verified (which means it's not in `disseminationProtocolState` yet), so we
            // also need (and have) a duplicate check later in the post-verify message processing.
            if (disseminationProgress.acks.map(_.from).contains(from))
              logger.debug(
                s"$messageType: duplicate remote ack for batch $batchId from $from, ignoring"
              )
            else {
              val epochNumber = disseminationProgress.batchMetadata.epochNumber
              pipeToSelf(
                activeCryptoProvider
                  .verifySignature(
                    AvailabilityAck.hashFor(batchId, epochNumber, from),
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
      actingOnMessageType: => String,
      proofOfAvailability: ProofOfAvailability,
      mode: OrderedBlockForOutput.Mode,
  )(implicit
      context: E#ActorContextT[Availability.Message[E]],
      traceContext: TraceContext,
  ): Unit = {
    if (outputFetchProtocolState.localOutputMissingBatches.contains(proofOfAvailability.batchId)) {
      logger.debug(
        s"$actingOnMessageType: already trying to download ${proofOfAvailability.batchId}, ignoring"
      )
      return
    }
    if (proofOfAvailability.acks.isEmpty) {
      logger.error(s"$actingOnMessageType: proof of availability is missing, ignoring")
      return
    }
    val (node, remainingNodes) =
      if (mode.isStateTransfer)
        extractNodes(acks = None, useCurrentTopology = true)
      else
        extractNodes(Some(proofOfAvailability.acks))
    logger.debug(
      s"$actingOnMessageType: fetch of ${proofOfAvailability.batchId} " +
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
    val shuffled = nodeShuffler.shuffle(nodes)
    val head = shuffled.headOption.getOrElse(abort("There should be at least one node to extract"))
    head -> shuffled.tail
  }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def shipAvailableConsensusProposals(
      actingOnMessageType: => String
  )(implicit traceContext: TraceContext): Unit =
    while (
      disseminationProtocolState.batchesReadyForOrdering.nonEmpty &&
      disseminationProtocolState.toBeProvidedToConsensus.nonEmpty
    ) {
      val maxBatchesPerProposal =
        disseminationProtocolState.toBeProvidedToConsensus.dequeue()
      assembleAndSendConsensusProposal(
        actingOnMessageType,
        maxBatchesPerProposal,
      )
    }

  @SuppressWarnings(Array("org.wartremover.warts.While"))
  private def assembleAndSendConsensusProposal(
      actingOnMessageType: => String,
      toBeProvidedToConsensus: ToBeProvidedToConsensus,
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

  private def initiateMempoolPull(
      actingOnMessageType: => String
  )(implicit traceContext: TraceContext): Unit = {
    // we tell mempool we want enough batches to fill up a proposal in order to make up for the one we just created
    // times the multiplier in order to try to disseminate-ahead batches for a following proposal
    val atMost = config.maxBatchesPerProposal * DisseminateAheadMultiplier -
      // if we have pending batches for ordering we subtract them in order for this buffer to not grow indefinitely
      (disseminationProtocolState.batchesReadyForOrdering.size + disseminationProtocolState.disseminationProgress.size)

    if (atMost > 0) {
      logger.debug(s"$actingOnMessageType: requesting at most $atMost batches from local mempool")
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

      _ <- Either.cond(
        batch.epochNumber > lastKnownEpochNumber - OrderingRequestBatch.BatchValidityDurationEpochs,
        (), {
          emitInvalidMessage(metrics, from)
          s"Batch $batchId from '$from' contains an expired batch at epoch number ${batch.epochNumber} " +
            s"which is ${OrderingRequestBatch.BatchValidityDurationEpochs} " +
            s"epochs or more older than last known epoch $lastKnownEpochNumber, skipping"
        },
      )

      _ <- Either.cond(
        batch.epochNumber < lastKnownEpochNumber + OrderingRequestBatch.BatchValidityDurationEpochs * 2,
        (), {
          emitInvalidMessage(metrics, from)
          s"Batch $batchId from '$from' contains a batch whose epoch number ${batch.epochNumber} is too far in the future " +
            s"compared to last known epoch $lastKnownEpochNumber, skipping"
        },
      )
    } yield ()
}

object AvailabilityModule {

  private type ReadyForOrdering = Boolean

  private val ClockTickInterval = 100.milliseconds

  val DisseminateAheadMultiplier = 2

  def quorum(numberOfNodes: Int): Int = OrderingTopology.weakQuorumSize(numberOfNodes)

  def hasQuorum(orderingTopology: OrderingTopology, votes: Int): Boolean =
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
}
