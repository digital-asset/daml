// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  PeerEndpointHealth,
  PeerEndpointHealthStatus,
  PeerEndpointStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultDatabaseReadTimeout
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.NetworkingMetrics.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.data.P2PEndpointsStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.OrderingTopology.strongQuorumSize
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.P2PNetworkOut.{
  Admin,
  BftOrderingNetworkMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.P2PNetworkOutModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  Env,
  ModuleRef,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessageBody,
  BftOrderingServiceReceiveRequest,
}
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.util.{Failure, Success}

private[bftordering] class KnownPeers {

  private val networkRefs =
    mutable.Map.empty[
      P2PEndpoint.Id,
      (P2PEndpoint, P2PNetworkRef[BftOrderingServiceReceiveRequest]),
    ]
  private val sequencerIds = mutable.Map.empty[P2PEndpoint.Id, SequencerId]
  private val endpoints = mutable.Map.empty[SequencerId, P2PEndpoint.Id]

  def isDefined(endpoint: P2PEndpoint.Id): Boolean = networkRefs.contains(endpoint)

  def actOn(peer: SequencerId, ifEmpty: => Unit)(
      action: P2PNetworkRef[BftOrderingServiceReceiveRequest] => Unit
  ): Unit =
    endpoints.get(peer).fold(ifEmpty)(networkRefs.get(_).map(_._2).fold(ifEmpty)(action))

  def add(
      endpoint: P2PEndpoint,
      ref: P2PNetworkRef[BftOrderingServiceReceiveRequest],
  ): Unit =
    networkRefs.addOne(endpoint.id -> (endpoint, ref))

  def getSequencerId(endpoint: P2PEndpoint.Id): Option[SequencerId] =
    sequencerIds.get(endpoint)

  def setSequencerId(endpointId: P2PEndpoint.Id, sequencerId: SequencerId): Unit = {
    sequencerIds.addOne(endpointId -> sequencerId)
    endpoints.addOne(sequencerId -> endpointId)
  }

  def getEndpoints: Seq[P2PEndpoint] = networkRefs.values.map(_._1).toSeq

  def authenticatedCount: Int = sequencerIds.size

  def delete(endpoint: P2PEndpoint.Id): Unit = {
    networkRefs.remove(endpoint).foreach(_._2.close())
    sequencerIds.remove(endpoint).foreach(endpoints.remove)
  }
}

final class BftP2PNetworkOut[E <: Env[E]](
    thisSequencerId: SequencerId,
    @VisibleForTesting private[bftordering] val p2pEndpointsStore: P2PEndpointsStore[E],
    metrics: BftOrderingMetrics,
    override val dependencies: P2PNetworkOutModuleDependencies[E],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    state: BftP2PNetworkOut.State = new BftP2PNetworkOut.State,
)(implicit mc: MetricsContext)
    extends P2PNetworkOut[E] {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var initialEndpointsCount = 1

  import state.*

  override def ready(self: ModuleRef[P2PNetworkOut.Message]): Unit =
    self.asyncSend(P2PNetworkOut.Start)

  override def receiveInternal(
      message: P2PNetworkOut.Message
  )(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): Unit =
    message match {
      case P2PNetworkOut.Start =>
        val endpoints =
          context.blockingAwait(p2pEndpointsStore.listEndpoints, DefaultDatabaseReadTimeout)
        initialEndpointsCount = endpoints.size + 1
        connectInitialPeers(endpoints)
        startModulesIfNeeded()

      case P2PNetworkOut.Internal.Connect(endpoint) =>
        val _ = connect(endpoint)

      case P2PNetworkOut.Internal.Disconnect(endpoint) =>
        disconnect(endpoint)

      case P2PNetworkOut.Network.Authenticated(endpointId, sequencerId) =>
        if (sequencerId == thisSequencerId) {
          emitIdentityEquivocation(metrics, endpointId, sequencerId)
          logger.warn(
            s"A peer authenticated from $endpointId with the sequencer ID of this very peer " +
              s"($thisSequencerId); this could indicate malicious behavior: disconnecting the peer"
          )
          disconnect(endpointId)
        } else {
          knownPeers.getSequencerId(endpointId) match {
            case Some(existingSequencerId) if existingSequencerId != sequencerId =>
              emitIdentityEquivocation(metrics, endpointId, existingSequencerId)
              logger.warn(
                s"On reconnection, a peer authenticated from endpoint $endpointId " +
                  s"with a different sequencer id $sequencerId, but it was already authenticated " +
                  s"as $existingSequencerId; this could indicate malicious behavior: disconnecting the peer"
              )
              disconnect(endpointId)
            case _ =>
              logger.debug(s"Authenticated peer $sequencerId at $endpointId")
              registerAuthenticated(endpointId, sequencerId)
          }
        }

      case P2PNetworkOut.Multicast(message, peers) =>
        peers.toSeq
          .sortBy(_.toProtoPrimitive) // For determinism
          .foreach(sendIfKnown(_, message))

      case admin: P2PNetworkOut.Admin =>
        processModuleAdmin(admin)
    }

  private def sendIfKnown(
      peer: SequencerId,
      message: BftOrderingNetworkMessage,
  )(implicit
      traceContext: TraceContext
  ): Unit =
    if (peer != thisSequencerId)
      networkSendIfKnown(peer, message)
    else
      dependencies.p2pNetworkIn.asyncSend(messageToSend(message.toProto))

  private def networkSendIfKnown(
      to: SequencerId,
      message: BftOrderingNetworkMessage,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    val serializedMessage = message.toProto
    knownPeers.actOn(
      to,
      ifEmpty = {
        val mc1 =
          sendMetricsContext(metrics, serializedMessage, to, droppedAsUnauthenticated = true)
        locally {
          implicit val mc: MetricsContext = mc1
          emitSendStats(metrics, serializedMessage)
        }
        logger.info(
          s"Dropping $message to $to because it is unknown (possibly unauthenticated as of yet)"
        )
      },
    ) { ref =>
      val mc1: MetricsContext =
        sendMetricsContext(metrics, serializedMessage, to, droppedAsUnauthenticated = false)
      locally {
        logger.debug(s"Sending network message to $to")
        logger.trace(s"Message to $to is: $message")
        implicit val mc: MetricsContext = mc1
        networkSend(ref, serializedMessage)
        emitSendStats(metrics, serializedMessage)
      }
    }
  }

  private def processModuleAdmin(
      admin: P2PNetworkOut.Admin
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    admin match {
      case Admin.AddEndpoint(endpoint, callback) =>
        if (knownPeers.isDefined(endpoint.id)) {
          callback(false)
        } else {
          context.pipeToSelf(p2pEndpointsStore.addEndpoint(endpoint)) {
            case Success(hasBeenAdded) =>
              callback(hasBeenAdded)
              if (hasBeenAdded)
                Some(P2PNetworkOut.Internal.Connect(endpoint))
              else
                None
            case Failure(exception) =>
              abort(s"Failed to add endpoint $endpoint", exception)
          }
        }
      case Admin.RemoveEndpoint(endpointId, callback) =>
        if (knownPeers.isDefined(endpointId)) {
          context.pipeToSelf(p2pEndpointsStore.removeEndpoint(endpointId)) {
            case Success(hasBeenRemoved) =>
              callback(hasBeenRemoved)
              if (hasBeenRemoved)
                Some(P2PNetworkOut.Internal.Disconnect(endpointId))
              else
                None
            case Failure(exception) =>
              abort(s"Failed to remove endpoint $endpointId", exception)
          }
        } else {
          callback(false)
        }
      case Admin.GetStatus(callback, endpointIds) =>
        callback(getStatus(endpointIds))
    }

  private def getStatus(endpointIds: Option[Iterable[P2PEndpoint.Id]] = None) =
    SequencerBftAdminData.PeerNetworkStatus(
      endpointIds
        .getOrElse(
          knownPeers.getEndpoints
            .map(_.id)
            .sorted // Sorted for output determinism and easier testing
        )
        .map { endpointId =>
          val defined = knownPeers.isDefined(endpointId)
          val authenticated = knownPeers.getSequencerId(endpointId).isDefined
          PeerEndpointStatus(
            endpointId,
            health = (defined, authenticated) match {
              case (false, _) => PeerEndpointHealth(PeerEndpointHealthStatus.Unknown, None)
              case (_, false) => PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None)
              case _ => PeerEndpointHealth(PeerEndpointHealthStatus.Authenticated, None)
            },
          )
        }
        .toSeq
    )

  private lazy val peerThresholdForAvailabilityStart =
    AvailabilityModule.quorum(initialEndpointsCount)

  private lazy val peerThresholdForConsensusStart = strongQuorumSize(initialEndpointsCount)

  private def startModulesIfNeeded()(implicit
      traceContext: TraceContext
  ): Unit = {
    if (!mempoolStarted) {
      logger.debug(s"Starting mempool")
      dependencies.mempool.asyncSend(Mempool.Start)
      mempoolStarted = true
    }
    // Waiting for just a quorum (minus self) of peers to be authenticated assumes that they are not faulty
    if (!availabilityStarted) {
      if (maxPeersContemporarilyAuthenticated >= peerThresholdForAvailabilityStart - 1) {
        logger.debug(
          s"Peer threshold $peerThresholdForAvailabilityStart reached: starting availability"
        )
        dependencies.availability.asyncSend(Availability.Start)
        availabilityStarted = true
      }
    }
    if (!consensusStarted) {
      if (maxPeersContemporarilyAuthenticated >= peerThresholdForConsensusStart - 1) {
        logger.debug(s"Peer threshold $peerThresholdForConsensusStart reached: starting consensus")
        dependencies.consensus.asyncSend(Consensus.Start)
        consensusStarted = true
      }
    }
    if (!outputStarted) {
      logger.debug(s"Starting output")
      dependencies.output.asyncSend(Output.Start)
      outputStarted = true
    }
  }

  private def networkSend(
      ref: P2PNetworkRef[BftOrderingServiceReceiveRequest],
      message: BftOrderingMessageBody,
  )(implicit traceContext: TraceContext, mc: MetricsContext): Unit = {
    val start = Instant.now()
    ref.asyncP2PSend(messageToSend(message)) {
      val end = Instant.now()
      metrics.p2p.send.networkWriteLatency.update(Duration.between(start, end))
    }
  }

  private def messageToSend(
      message: BftOrderingMessageBody
  )(implicit traceContext: TraceContext): BftOrderingServiceReceiveRequest =
    BftOrderingServiceReceiveRequest.of(
      traceContext.traceId.getOrElse(""),
      Some(message),
      thisSequencerId.uid.toProtoPrimitive,
    )

  private def connectInitialPeers(otherInitialEndpoints: Seq[P2PEndpoint])(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): Unit =
    if (!initialPeersConnecting) {
      logger.debug(s"Connecting to initial peers: $otherInitialEndpoints")
      otherInitialEndpoints.foreach { endpoint =>
        val _ = connect(endpoint)
      }
      initialPeersConnecting = true
    }

  private def connect(
      endpoint: P2PEndpoint
  )(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): P2PNetworkRef[BftOrderingServiceReceiveRequest] = {
    logger.debug(
      s"Connecting new peer at ${endpoint.id}"
    )
    val networkRef = dependencies.p2pNetworkManager.createNetworkRef(context, endpoint) {
      (endpointId, sequencerId) =>
        context.self.asyncSend(P2PNetworkOut.Network.Authenticated(endpointId, sequencerId))
    }
    knownPeers.add(endpoint, networkRef)
    emitConnectedCount(metrics, knownPeers)
    logEndpointsStatus()
    networkRef
  }

  private def registerAuthenticated(
      endpointId: P2PEndpoint.Id,
      sequencerId: SequencerId,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Registering peer $sequencerId at $endpointId")
    knownPeers.setSequencerId(endpointId, sequencerId)
    emitAuthenticatedCount(metrics, knownPeers)
    logEndpointsStatus()
    maxPeersContemporarilyAuthenticated =
      Math.max(maxPeersContemporarilyAuthenticated, knownPeers.authenticatedCount)
    startModulesIfNeeded()
  }

  private def disconnect(
      endpointId: P2PEndpoint.Id
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(
      s"Disconnecting peer ${knownPeers.getSequencerId(endpointId).map(_.toString).getOrElse("<unknown>")} at $endpointId"
    )
    knownPeers.delete(endpointId)
    emitConnectedCount(metrics, knownPeers)
    logEndpointsStatus()
  }

  private def logEndpointsStatus()(implicit traceContext: TraceContext): Unit =
    logger.info(getStatus().toString)
}

private[bftordering] object BftP2PNetworkOut {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  final class State {

    val knownPeers = new KnownPeers
    var initialPeersConnecting = false
    var availabilityStarted = false
    var mempoolStarted = false
    var consensusStarted = false
    var outputStarted = false

    // We want to track the maximum number of contemporarily authenticated peers,
    //  because the threshold actions will be used by protocol modules to know when
    //  there are enough connections to start, so we don't want to consider
    //  peers that disconnected afterward. For example, when peer P1 connects
    //  to other peers:
    //
    //  - P2 authenticates.
    //  - P3 authenticates.
    //  - P2 gets disconnected (e.g. by an admin) slightly before processing the request of
    //    consensus to be started when 2 peers are authenticated.
    //
    //  In this case, we want to start consensus anyway.
    var maxPeersContemporarilyAuthenticated = 0
  }
}
