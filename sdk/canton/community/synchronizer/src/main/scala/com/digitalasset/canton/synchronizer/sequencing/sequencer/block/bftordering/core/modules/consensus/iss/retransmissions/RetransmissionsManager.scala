// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.retransmissions

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.modules.consensus.iss.EpochState
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.modules.{
  Consensus,
  ConsensusStatus,
  P2PNetworkOut,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.framework.{
  CancellableEvent,
  Env,
  ModuleRef,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.duration.*

import RetransmissionsManager.RetransmissionRequestPeriod

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class RetransmissionsManager[E <: Env[E]](
    epochState: EpochState[E],
    otherPeers: Set[SequencerId],
    p2pNetworkOut: ModuleRef[P2PNetworkOut.Message],
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private val epochNumber = epochState.epoch.info.number
  private var periodicStatusCancellable: Option[CancellableEvent] = None
  private var epochStatusBuilder: Option[EpochStatusBuilder] = None

  def startRequesting()(implicit traceContext: TraceContext): Unit =
    // when we start, we immediately request retransmissions for the first time.
    // the subsequent requests are done periodically
    startRetransmissionsRequest()

  def stopRequesting(): Unit =
    periodicStatusCancellable.foreach(_.cancel())

  def handleMessage(message: Consensus.RetransmissionsMessage)(implicit
      context: E#ActorContextT[Consensus.Message[E]],
      traceContext: TraceContext,
  ): Unit = message match {
    // message from the network from a node requesting retransmissions of messages
    case Consensus.RetransmissionsMessage.NetworkMessage(msg) =>
      msg match {
        case Consensus.RetransmissionsMessage.RetransmissionRequest(epochStatus) =>
          if (epochStatus.epochNumber != epochNumber) {
            // TODO(#18788): support serving retransmission of commit certs to nodes in a previous epoch
            logger.info(
              s"We got a retransmission request for epoch ${epochStatus.epochNumber}," +
                s"but we can only serve retransmissions for epoch $epochNumber"
            )
          } else {
            logger.info(
              s"Got a retransmission request from ${epochStatus.from} at epoch $epochNumber"
            )
            epochState.processRetransmissionsRequest(epochStatus)
          }
        case Consensus.RetransmissionsMessage.RetransmissionResponse(from, commitCertificates) =>
          epochState.processRetransmissionResponse(from, commitCertificates)
      }

    // periodic event where we broadcast our status in order to request retransmissions
    case Consensus.RetransmissionsMessage.PeriodicStatusBroadcast =>
      startRetransmissionsRequest()
    // each segment provides its own status so that the epoch status can be built
    case segStatus: Consensus.RetransmissionsMessage.SegmentStatus =>
      epochStatusBuilder.foreach(_.receive(segStatus))
      epochStatusBuilder.flatMap(_.epochStatus).foreach { epochStatus =>
        logger.info(
          s"Broadcasting epoch status at epoch $epochNumber in order to request retransmissions"
        )
        // after gathering the segment status from all segments,
        // we can broadcast our whole epoch status
        // and effectively request retransmissions of missing messages
        broadcastStatus(epochStatus)
        epochStatusBuilder = None
        rescheduleStatusBroadcast(context)
      }
  }

  private def startRetransmissionsRequest()(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Started gathering segment status at epoch $epochNumber in order to broadcast epoch status"
    )
    epochStatusBuilder = Some(epochState.requestSegmentStatuses())
  }

  private def broadcastStatus(epochStatus: ConsensusStatus.EpochStatus): Unit =
    p2pNetworkOut.asyncSend(
      P2PNetworkOut.Multicast(
        P2PNetworkOut.BftOrderingNetworkMessage.RetransmissionMessage(
          SignedMessage(
            Consensus.RetransmissionsMessage.RetransmissionRequest.create(epochStatus),
            Signature.noSignature,
          ) // TODO(#20458) actually sign the message
        ),
        otherPeers,
      )
    )

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
}
