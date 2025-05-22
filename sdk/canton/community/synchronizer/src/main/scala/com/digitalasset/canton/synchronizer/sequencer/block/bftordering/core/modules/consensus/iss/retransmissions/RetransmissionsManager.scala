// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions

import cats.syntax.either.*
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.RequireTypes.{NonNegativeNumeric, PositiveDouble}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModuleMetrics.emitNonCompliance
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.RetransmissionMessageValidator
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.validation.RetransmissionMessageValidator.RetransmissionResponseValidationError
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.{
  BftNodeRateLimiter,
  EpochState,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.shortType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.topology.CryptoProvider.AuthenticatedMessageType
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
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
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.*
import scala.util.{Failure, Success}

import RetransmissionsManager.{
  HowManyEpochsToKeep,
  MaxRetransmissionRequestBurstFactorPerNode,
  NodeRoundRobin,
  RetransmissionRequestPeriod,
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class RetransmissionsManager[E <: Env[E]](
    thisNode: BftNodeId,
    p2pNetworkOut: ModuleRef[P2PNetworkOut.Message],
    abort: String => Nothing,
    previousEpochsCommitCerts: Map[EpochNumber, Seq[CommitCertificate]],
    metrics: BftOrderingMetrics,
    clock: Clock,
    override val loggerFactory: NamedLoggerFactory,
)(implicit synchronizerProtocolVersion: ProtocolVersion, mc: MetricsContext)
    extends NamedLogging {
  private var currentEpoch: Option[EpochState[E]] = None
  private var validator: Option[RetransmissionMessageValidator] = None

  private var periodicStatusCancellable: Option[CancellableEvent] = None
  private var epochStatusBuilder: Option[EpochStatusBuilder] = None

  private val roundRobin = new NodeRoundRobin()

  private var incomingRetransmissionsRequestCount = 0
  private var outgoingRetransmissionsRequestCount = 0
  private var discardedWrongEpochRetransmissionsResponseCount = 0
  private var discardedRateLimitedRetransmissionRequestCount = 0

  private val requestRateLimiter =
    new BftNodeRateLimiter(
      clock,
      maxTasksPerSecond =
        NonNegativeNumeric.tryCreate(1.toDouble / RetransmissionRequestPeriod.toSeconds.toDouble),
      maxBurstFactor = PositiveDouble.tryCreate(MaxRetransmissionRequestBurstFactorPerNode),
    )

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
      validator = Some(new RetransmissionMessageValidator(epochState.epoch))

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
        validator = None
        stopRequesting()
        recordMetricsAndResetRequestCounts(epoch.epoch.info)
      case None =>
        abort("Tried to end epoch when there is none in progress")
    }

  private def stopRequesting(): Unit = {
    periodicStatusCancellable.foreach(_.cancel())
    epochStatusBuilder = None
  }

  private def recordMetricsAndResetRequestCounts(epoch: EpochInfo): Unit = {
    metrics.consensus.retransmissions.incomingRetransmissionsRequestsMeter
      .mark(incomingRetransmissionsRequestCount.toLong)(
        mc.withExtraLabels(
          metrics.consensus.votes.labels.Epoch -> epoch.toString
        )
      )
    metrics.consensus.retransmissions.outgoingRetransmissionsRequestsMeter
      .mark(outgoingRetransmissionsRequestCount.toLong)(
        mc.withExtraLabels(
          metrics.consensus.votes.labels.Epoch -> epoch.toString
        )
      )
    metrics.consensus.retransmissions.discardedWrongEpochRetransmissionResponseMeter
      .mark(discardedWrongEpochRetransmissionsResponseCount.toLong)(
        mc.withExtraLabels(
          metrics.consensus.votes.labels.Epoch -> epoch.toString
        )
      )
    metrics.consensus.retransmissions.discardedRateLimitedRetransmissionRequestMeter
      .mark(discardedRateLimitedRetransmissionRequestCount.toLong)(
        mc.withExtraLabels(
          metrics.consensus.votes.labels.Epoch -> epoch.toString
        )
      )
    incomingRetransmissionsRequestCount = 0
    outgoingRetransmissionsRequestCount = 0
    discardedWrongEpochRetransmissionsResponseCount = 0
    discardedRateLimitedRetransmissionRequestCount = 0
  }

  def handleMessage(
      activeCryptoProvider: CryptoProvider[E],
      message: Consensus.RetransmissionsMessage,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = message match {
    case Consensus.RetransmissionsMessage.UnverifiedNetworkMessage(message) =>
      // do cheap validations before checking signature to potentially save ourselves from doing the expensive signature check
      validateUnverifiedNetworkMessage(message.message) match {
        case Left(error) => logger.info(error)
        case Right(()) =>
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
              logger.info(
                s"Got a retransmission request from ${epochStatus.from} for a previous epoch ${epochStatus.epochNumber}"
              )
              previousEpochsRetransmissionsTracker.processRetransmissionsRequest(
                epochStatus
              ) match {
                case Right(commitCertsToRetransmit) =>
                  logger.info(
                    s"Retransmitting ${commitCertsToRetransmit.size} commit certificates to ${epochStatus.from}"
                  )
                  retransmitCommitCertificates(
                    activeCryptoProvider,
                    epochStatus.from,
                    commitCertsToRetransmit,
                  )
                case Left(logMsg) =>
                  logger.info(logMsg)
              }
          }
        case Consensus.RetransmissionsMessage.RetransmissionResponse(from, commitCertificates) =>
          currentEpoch match {
            case Some(epochState) =>
              val currentEpochNumber = epochState.epoch.info.number
              commitCertificates.headOption.foreach { commitCert =>
                val msgEpochNumber = commitCert.prePrepare.message.blockMetadata.epochNumber
                if (msgEpochNumber == epochState.epoch.info.number) {
                  logger.debug(
                    s"Got a retransmission response from $from at epoch $currentEpochNumber"
                  )
                  epochState.processRetransmissionResponse(from, commitCertificates)
                } else
                  logger.debug(
                    s"Got a retransmission response from $from for wrong epoch $msgEpochNumber, while we're at $currentEpochNumber, ignoring"
                  )
              }
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
          // we can send our whole epoch status
          // and effectively request retransmissions of missing messages
          sendStatus(activeCryptoProvider, epochStatus, e.epoch.currentMembership)
        }

        epochStatusBuilder = None
        rescheduleStatusBroadcast(context)
      }
  }

  private def validateUnverifiedNetworkMessage(
      msg: RetransmissionsNetworkMessage
  ): Either[String, Unit] =
    msg match {
      case req @ Consensus.RetransmissionsMessage.RetransmissionRequest(status) =>
        incomingRetransmissionsRequestCount += 1
        if (requestRateLimiter.checkAndUpdateRate(status.from)) {
          (currentEpoch.zip(validator)) match {
            case Some((epochState, validator))
                if (epochState.epoch.info.number == status.epochNumber) =>
              validator.validateRetransmissionRequest(req)
            case _ =>
              previousEpochsRetransmissionsTracker
                .processRetransmissionsRequest(status)
                .map(_ => ())
          }
        } else {
          discardedRateLimitedRetransmissionRequestCount += 1
          Left(
            s"Dropped a retransmission request from ${status.from} for epoch ${status.epochNumber} due to rate limiting"
          )
        }
      case response: Consensus.RetransmissionsMessage.RetransmissionResponse =>
        validator match {
          case Some(validator) =>
            val validationResult = validator.validateRetransmissionResponse(response)
            validationResult match {
              case Left(_: RetransmissionResponseValidationError.MalformedMessage) =>
                emitNonCompliance(metrics)(
                  response.from,
                  currentEpoch.map(_.epoch.info.number),
                  view = None,
                  block = None,
                  metrics.security.noncompliant.labels.violationType.values.RetransmissionResponseInvalidMessage,
                )
              case Left(_: RetransmissionResponseValidationError.WrongEpoch) =>
                discardedWrongEpochRetransmissionsResponseCount += 1
              case _ => ()
            }
            validationResult.leftMap(_.errorMsg)
          case None =>
            Left(
              s"Received a retransmission response from ${response.from} while transitioning epochs, ignoring"
            )
        }
    }

  private def startRetransmissionsRequest()(implicit traceContext: TraceContext): Unit =
    currentEpoch.foreach { epoch =>
      logger.info(
        s"Started gathering segment status at epoch ${epoch.epoch.info.number} in order to broadcast epoch status"
      )
      epochStatusBuilder = Some(epoch.requestSegmentStatuses())
    }

  private def sendStatus(
      activeCryptoProvider: CryptoProvider[E],
      epochStatus: ConsensusStatus.EpochStatus,
      membership: Membership,
  )(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = signRetransmissionNetworkMessage(
    activeCryptoProvider,
    Consensus.RetransmissionsMessage.RetransmissionRequest.create(epochStatus),
  ) { signedMessage =>
    outgoingRetransmissionsRequestCount += 1
    p2pNetworkOut.asyncSend(
      P2PNetworkOut.send(
        P2PNetworkOut.BftOrderingNetworkMessage.RetransmissionMessage(signedMessage),
        roundRobin.nextNode(membership),
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
  val RetransmissionRequestPeriod: FiniteDuration = 3.seconds
  val MaxRetransmissionRequestBurstFactorPerNode: Double = 6.toDouble

  // TODO(#24443): unify this value with catch up and pass it as config
  val HowManyEpochsToKeep = 5

  class NodeRoundRobin {
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var roundRobinCount = 0

    def nextNode(membership: Membership): BftNodeId = {
      roundRobinCount += 1
      // if the count would make us pick ourselves, we make it pick the next one
      if (roundRobinCount % membership.sortedNodes.size == 0) roundRobinCount = 1
      // we start from our own index as zero, so that all nodes start at different points
      val myIndex = membership.sortedNodes.indexOf(membership.myId)
      val currentIndex = (myIndex + roundRobinCount) % membership.sortedNodes.size
      membership.sortedNodes(currentIndex)
    }
  }
}
