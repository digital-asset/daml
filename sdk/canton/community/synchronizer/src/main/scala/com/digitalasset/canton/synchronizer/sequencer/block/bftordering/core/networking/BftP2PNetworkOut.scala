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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
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
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import java.time.{Duration, Instant}
import scala.collection.mutable
import scala.util.{Failure, Success}

private[bftordering] class KnownEndpointsAndNodes {

  private val networkRefs =
    mutable.Map.empty[
      P2PEndpoint.Id,
      (P2PEndpoint, P2PNetworkRef[BftOrderingServiceReceiveRequest]),
    ]
  private val nodes = mutable.Map.empty[P2PEndpoint.Id, BftNodeId]
  private val endpoints = mutable.Map.empty[BftNodeId, P2PEndpoint.Id]

  def isDefined(endpoint: P2PEndpoint.Id): Boolean = networkRefs.contains(endpoint)

  def actOn(node: BftNodeId, ifEmpty: => Unit)(
      action: P2PNetworkRef[BftOrderingServiceReceiveRequest] => Unit
  ): Unit =
    endpoints.get(node).fold(ifEmpty)(networkRefs.get(_).map(_._2).fold(ifEmpty)(action))

  def add(
      endpoint: P2PEndpoint,
      ref: P2PNetworkRef[BftOrderingServiceReceiveRequest],
  ): Unit =
    networkRefs.addOne(endpoint.id -> (endpoint, ref))

  def getNode(endpoint: P2PEndpoint.Id): Option[BftNodeId] =
    nodes.get(endpoint)

  def setNode(endpointId: P2PEndpoint.Id, node: BftNodeId): Unit = {
    nodes.addOne(endpointId -> node)
    endpoints.addOne(node -> endpointId)
  }

  def getEndpoints: Seq[P2PEndpoint] = networkRefs.values.map(_._1).toSeq

  def authenticatedCount: Int = nodes.size

  def delete(endpoint: P2PEndpoint.Id): Unit = {
    networkRefs.remove(endpoint).foreach(_._2.close())
    nodes.remove(endpoint).foreach(endpoints.remove)
  }
}

final class BftP2PNetworkOut[E <: Env[E]](
    thisNode: BftNodeId,
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
        connectInitialNodes(endpoints)
        startModulesIfNeeded()

      case P2PNetworkOut.Internal.Connect(endpoint) =>
        val _ = connect(endpoint)

      case P2PNetworkOut.Internal.Disconnect(endpoint) =>
        disconnect(endpoint)

      case P2PNetworkOut.Network.Authenticated(endpointId, node) =>
        if (node == thisNode) {
          emitIdentityEquivocation(metrics, endpointId, node)
          logger.warn(
            s"A node authenticated from $endpointId with the sequencer ID of this very node " +
              s"($thisNode); this could indicate malicious behavior: disconnecting it"
          )
          disconnect(endpointId)
        } else {
          known.getNode(endpointId) match {
            case Some(existingNode) if existingNode != node =>
              emitIdentityEquivocation(metrics, endpointId, existingNode)
              logger.warn(
                s"On reconnection, a node authenticated from endpoint $endpointId " +
                  s"with a different sequencer id $node, but it was already authenticated " +
                  s"as $existingNode; this could indicate malicious behavior: disconnecting it"
              )
              disconnect(endpointId)
            case _ =>
              logger.debug(s"Authenticated node $node at $endpointId")
              registerAuthenticated(endpointId, node)
          }
        }

      case P2PNetworkOut.Multicast(message, nodes) =>
        nodes.toSeq.sorted // For determinism
          .foreach(sendIfKnown(_, message))

      case admin: P2PNetworkOut.Admin =>
        processModuleAdmin(admin)
    }

  private def sendIfKnown(
      node: BftNodeId,
      message: BftOrderingNetworkMessage,
  )(implicit
      traceContext: TraceContext
  ): Unit =
    if (node != thisNode)
      networkSendIfKnown(node, message)
    else
      dependencies.p2pNetworkIn.asyncSend(messageToSend(message.toProto))

  private def networkSendIfKnown(
      to: BftNodeId,
      message: BftOrderingNetworkMessage,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    val serializedMessage = message.toProto
    known.actOn(
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
        if (known.isDefined(endpoint.id)) {
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
        if (known.isDefined(endpointId)) {
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
          known.getEndpoints
            .map(_.id)
            .sorted // Sorted for output determinism and easier testing
        )
        .map { endpointId =>
          val defined = known.isDefined(endpointId)
          val authenticated = known.getNode(endpointId).isDefined
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

  private lazy val endpointThresholdForAvailabilityStart =
    AvailabilityModule.quorum(initialEndpointsCount)

  private lazy val endpointThresholdForConsensusStart = strongQuorumSize(initialEndpointsCount)

  private def startModulesIfNeeded()(implicit
      traceContext: TraceContext
  ): Unit = {
    if (!mempoolStarted) {
      logger.debug(s"Starting mempool")
      dependencies.mempool.asyncSend(Mempool.Start)
      mempoolStarted = true
    }
    // Waiting for just a quorum (minus self) of nodes to be authenticated assumes that they are not faulty
    if (!availabilityStarted) {
      if (maxNodesContemporarilyAuthenticated >= endpointThresholdForAvailabilityStart - 1) {
        logger.debug(
          s"Threshold $endpointThresholdForAvailabilityStart reached: starting availability"
        )
        dependencies.availability.asyncSend(Availability.Start)
        availabilityStarted = true
      }
    }
    if (!consensusStarted) {
      if (maxNodesContemporarilyAuthenticated >= endpointThresholdForConsensusStart - 1) {
        logger.debug(
          s"Threshold $endpointThresholdForConsensusStart reached: starting consensus"
        )
        dependencies.consensus.asyncSend(Consensus.Start)
        consensusStarted = true
      }
    }
    if (!outputStarted) {
      logger.debug(s"Starting output")
      dependencies.output.asyncSend(Output.Start)
      outputStarted = true
    }
    if (!pruningStarted) {
      logger.debug(s"Starting pruning")
      dependencies.pruning.asyncSend(Pruning.Start)
      pruningStarted = true
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
    BftOrderingServiceReceiveRequest(
      traceContext.traceId.getOrElse(""),
      Some(message),
      thisNode,
    )

  private def connectInitialNodes(otherInitialEndpoints: Seq[P2PEndpoint])(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): Unit =
    if (!initialNodesConnecting) {
      logger.debug(s"Connecting to initial nodes: $otherInitialEndpoints")
      otherInitialEndpoints.foreach { endpoint =>
        val _ = connect(endpoint)
      }
      initialNodesConnecting = true
    }

  private def connect(
      endpoint: P2PEndpoint
  )(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): P2PNetworkRef[BftOrderingServiceReceiveRequest] = {
    logger.debug(
      s"Connecting new node at ${endpoint.id}"
    )
    val networkRef = dependencies.p2pNetworkManager.createNetworkRef(context, endpoint) {
      (endpointId, node) =>
        context.self.asyncSend(P2PNetworkOut.Network.Authenticated(endpointId, node))
    }
    known.add(endpoint, networkRef)
    emitConnectedCount(metrics, known)
    logEndpointsStatus()
    networkRef
  }

  private def registerAuthenticated(
      endpointId: P2PEndpoint.Id,
      node: BftNodeId,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(s"Registering '$node' at $endpointId")
    known.setNode(endpointId, node)
    emitAuthenticatedCount(metrics, known)
    logEndpointsStatus()
    maxNodesContemporarilyAuthenticated =
      Math.max(maxNodesContemporarilyAuthenticated, known.authenticatedCount)
    startModulesIfNeeded()
  }

  private def disconnect(
      endpointId: P2PEndpoint.Id
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    logger.debug(
      s"Disconnecting '${known.getNode(endpointId).getOrElse("<unknown>")}' at $endpointId"
    )
    known.delete(endpointId)
    emitConnectedCount(metrics, known)
    logEndpointsStatus()
  }

  private def logEndpointsStatus()(implicit traceContext: TraceContext): Unit =
    logger.info(getStatus().toString)
}

private[bftordering] object BftP2PNetworkOut {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  final class State {

    val known = new KnownEndpointsAndNodes
    var initialNodesConnecting = false
    var availabilityStarted = false
    var mempoolStarted = false
    var consensusStarted = false
    var outputStarted = false
    var pruningStarted = false

    // We want to track the maximum number of contemporarily authenticated nodes,
    //  because the threshold actions will be used by protocol modules to know when
    //  there are enough connections to start, so we don't want to consider
    //  nodes that disconnected afterward. For example, when node N1 connects
    //  to other nodes:
    //
    //  - N2 authenticates.
    //  - N3 authenticates.
    //  - N2 gets disconnected (e.g. by an admin) slightly before processing the request of
    //    consensus to be started when 2 nodes are authenticated.
    //
    //  In this case, we want to start consensus anyway.
    var maxNodesContemporarilyAuthenticated = 0
  }
}
