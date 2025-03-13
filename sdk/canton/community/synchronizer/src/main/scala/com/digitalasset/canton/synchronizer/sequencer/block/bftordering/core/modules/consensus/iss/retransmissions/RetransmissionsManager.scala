// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.EpochState
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.RetransmissionsMessage.RetransmissionsNetworkMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusStatus,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
  ModuleRef,
}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.*
import scala.util.{Failure, Success}

import RetransmissionsManager.{HowManyEpochsToKeep, RetransmissionRequestPeriod}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class RetransmissionsManager[E <: Env[E]](
    thisNode: BftNodeId,
    p2pNetworkOut: ModuleRef[P2PNetworkOut.Message],
    abort: String => Nothing,
    previousEpochsCommitCerts: Map[EpochNumber, Seq[CommitCertificate]],
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private var currentEpoch: Option[EpochState[E]] = None

  private var periodicStatusCancellable: Option[CancellableEvent] = None
  private var epochStatusBuilder: Option[EpochStatusBuilder] = None

  private val previousEpochsRetransmissionsTracker = new PreviousEpochsRetransmissionsTracker(
    HowManyEpochsToKeep,
    loggerFactory,
  )

  previousEpochsCommitCerts.foreach { case (epochNumber, commitCerts) =>
    previousEpochsRetransmissionsTracker.endEpoch(epochNumber, commitCerts)
  }

  def startEpoch(epochState: EpochState[E])(implicit
      traceContext: TraceContext
  ): Unit = currentEpoch match {
    case None =>
      currentEpoch = Some(epochState)

      // when we start an epoch, we immediately request retransmissions.
      // the subsequent requests are done periodically
      startRetransmissionsRequest()
    case Some(epoch) =>
      abort(
        s"Tried to start epoch ${epochState.epoch.info.number} when ${epoch.epoch.info.number} has not ended"
      )
  }

  def epochEnded(commitCertificates: Seq[CommitCertificate]): Unit =
    currentEpoch match {
      case Some(epoch) =>
        previousEpochsRetransmissionsTracker.endEpoch(epoch.epoch.info.number, commitCertificates)
        currentEpoch = None
        stopRequesting()
      case None =>
        abort("Tried to end epoch when there is none in progress")
    }

  private def stopRequesting(): Unit = {
    periodicStatusCancellable.foreach(_.cancel())
    epochStatusBuilder = None
  }

  def handleMessage(
      activeCryptoProvider: CryptoProvider[E],
      message: Consensus.RetransmissionsMessage,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = message match {
    case Consensus.RetransmissionsMessage.UnverifiedNetworkMessage(message) =>
      context.pipeToSelf(
        activeCryptoProvider.verifySignedMessage(
          message,
          AuthenticatedMessageType.BftSignedRetransmissionMessage,
        )
      ) {
        case Failure(exception) =>
          logger.error(
            s"Can't verify ${shortType(message.message)} from ${message.from}",
            exception,
          )
          None
        case Success(Left(errors)) =>
          // Info because it can also happen at epoch boundaries
          logger.info(
            s"Verification of ${shortType(message.message)} from ${message.from} failed: $errors"
          )
          None
        case Success(Right(())) =>
          Some(Consensus.RetransmissionsMessage.VerifiedNetworkMessage(message.message))
      }
    // message from the network from a node requesting retransmissions of messages
    case Consensus.RetransmissionsMessage.VerifiedNetworkMessage(msg) =>
      msg match {
        case Consensus.RetransmissionsMessage.RetransmissionRequest(epochStatus) =>
          currentEpoch.filter(_.epoch.info.number == epochStatus.epochNumber) match {
            case Some(currentEpoch) =>
              logger.info(
                s"Got a retransmission request from ${epochStatus.from} for current epoch ${currentEpoch.epoch.info}"
              )
              currentEpoch.processRetransmissionsRequest(epochStatus)
            case None =>
              val commitCertsToRetransmit =
                previousEpochsRetransmissionsTracker.processRetransmissionsRequest(epochStatus)

              if (commitCertsToRetransmit.nonEmpty) {
                logger.info(
                  s"Retransmitting ${commitCertsToRetransmit.size} commit certificates to ${epochStatus.from}"
                )
                retransmitCommitCertificates(
                  activeCryptoProvider,
                  epochStatus.from,
                  commitCertsToRetransmit,
                )
              }
          }
        case Consensus.RetransmissionsMessage.RetransmissionResponse(from, commitCertificates) =>
          currentEpoch match {
            case Some(epochState) =>
              val epochNumber = epochState.epoch.info.number
              val wrongEpochs =
                commitCertificates.view
                  .map(_.prePrepare.message.blockMetadata.epochNumber)
                  .filter(_ != epochNumber)
              if (wrongEpochs.isEmpty) {
                logger.debug(s"Got a retransmission response from $from at epoch $epochNumber")
                epochState.processRetransmissionResponse(from, commitCertificates)
              } else
                logger.debug(
                  s"Got a retransmission response for wrong epochs $wrongEpochs, while we're at $epochNumber, ignoring"
                )
            case None =>
              logger.debug(
                s"Received a retransmission response from $from while transitioning epochs, ignoring"
              )
          }
      }

    // periodic event where we broadcast our status in order to request retransmissions
    case Consensus.RetransmissionsMessage.PeriodicStatusBroadcast =>
      startRetransmissionsRequest()
    // each segment provides its own status so that the epoch status can be built
    case segStatus: Consensus.RetransmissionsMessage.SegmentStatus =>
      epochStatusBuilder.foreach(_.receive(segStatus))
      epochStatusBuilder.flatMap(_.epochStatus).foreach { epochStatus =>
        logger.info(
          s"Broadcasting epoch status at epoch ${epochStatus.epochNumber} in order to request retransmissions"
        )

        currentEpoch.foreach { e =>
          // after gathering the segment status from all segments,
          // we can broadcast our whole epoch status
          // and effectively request retransmissions of missing messages
          broadcastStatus(activeCryptoProvider, epochStatus, e.epoch.currentMembership.otherNodes)
        }

        epochStatusBuilder = None
        rescheduleStatusBroadcast(context)
      }
  }

  private def startRetransmissionsRequest()(implicit traceContext: TraceContext): Unit =
    currentEpoch.foreach { epoch =>
      logger.info(
        s"Started gathering segment status at epoch ${epoch.epoch.info.number} in order to broadcast epoch status"
      )
      epochStatusBuilder = Some(epoch.requestSegmentStatuses())
    }

  private def broadcastStatus(
      activeCryptoProvider: CryptoProvider[E],
      epochStatus: ConsensusStatus.EpochStatus,
      otherNodes: Set[BftNodeId],
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = signRetransmissionNetworkMessage(
    activeCryptoProvider,
    Consensus.RetransmissionsMessage.RetransmissionRequest.create(epochStatus),
  ) { signedMessage =>
    p2pNetworkOut.asyncSend(
      P2PNetworkOut.Multicast(
        P2PNetworkOut.BftOrderingNetworkMessage.RetransmissionMessage(signedMessage),
        otherNodes,
      )
    )
  }

  private def retransmitCommitCertificates(
      activeCryptoProvider: CryptoProvider[E],
      receiver: BftNodeId,
      commitCertificates: Seq[CommitCertificate],
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = signRetransmissionNetworkMessage(
    activeCryptoProvider,
    Consensus.RetransmissionsMessage.RetransmissionResponse.create(thisNode, commitCertificates),
  ) { signedMessage =>
    p2pNetworkOut.asyncSend(
      P2PNetworkOut.send(
        P2PNetworkOut.BftOrderingNetworkMessage.RetransmissionMessage(signedMessage),
        to = receiver,
      )
    )
  }

  private def signRetransmissionNetworkMessage[Message <: RetransmissionsNetworkMessage](
      activeCryptoProvider: CryptoProvider[E],
      message: Message,
  )(
      continuation: SignedMessage[Message] => Unit
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit =
    context.pipeToSelf(
      activeCryptoProvider.signMessage(
        message,
        AuthenticatedMessageType.BftSignedRetransmissionMessage,
      )
    ) {
      case Failure(exception) =>
        logger.error(s"Can't sign $message", exception)
        None
      case Success(Left(errors)) =>
        logger.error(s"Can't sign $message: $errors")
        None
      case Success(Right(signedMessage)) =>
        continuation(signedMessage)
        None
    }

  private def rescheduleStatusBroadcast(context: E#ActorContextT[Consensus.Message[E]]): Unit = {
    periodicStatusCancellable.foreach(_.cancel())
    periodicStatusCancellable = Some(
      context.delayedEvent(
        RetransmissionRequestPeriod,
        Consensus.RetransmissionsMessage.PeriodicStatusBroadcast,
      )
    )
  }
}

object RetransmissionsManager {
  val RetransmissionRequestPeriod: FiniteDuration = 10.seconds

  // TODO(#24443): unify this value with catch up and pass it as config
  val HowManyEpochsToKeep = 5
}
