// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.admin.SequencerBftAdminData.{
  PeerEndpointHealth,
  PeerEndpointHealthStatus,
  PeerEndpointStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.availability.AvailabilityModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule.DefaultDatabaseReadTimeout
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PMetrics.{
  emitAuthenticatedCount,
  emitConnectedCount,
  emitIdentityEquivocation,
  emitSendStats,
  sendMetricsContext,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.data.P2PEndpointsStore
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
  P2PConnectionEventListener,
  P2PNetworkRef,
  P2PNetworkRefFactory,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessageBody,
  BftOrderingServiceReceiveRequest,
}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.timestamp.Timestamp

import java.time.Instant
import scala.collection.mutable
import scala.util.{Failure, Success}

private[p2p] class KnownEndpointsAndNodes {

  private val endpointIdToNetworkRef =
    mutable.Map.empty[
      P2PEndpoint.Id,
      (P2PEndpoint, P2PNetworkRef[BftOrderingServiceReceiveRequest]),
    ]
  private val connectedEndpointIds = mutable.Set.empty[P2PEndpoint.Id]
  private val endpointIdToNodeId = mutable.Map.empty[P2PEndpoint.Id, BftNodeId]
  private val nodeIdToEndpointId = mutable.Map.empty[BftNodeId, P2PEndpoint.Id]

  def isDefined(endpointId: P2PEndpoint.Id): Boolean =
    endpointIdToNetworkRef.contains(endpointId)

  def isConnected(endpointId: P2PEndpoint.Id): Boolean =
    connectedEndpointIds.contains(endpointId)

  def actOn(
      node: BftNodeId,
      ifEmpty: => Unit,
  )(
      action: P2PNetworkRef[BftOrderingServiceReceiveRequest] => Unit
  ): Unit =
    nodeIdToEndpointId
      .get(node)
      .fold(ifEmpty)(endpointIdToNetworkRef.get(_).map(_._2).fold(ifEmpty)(action))

  def add(
      endpoint: P2PEndpoint,
      ref: P2PNetworkRef[BftOrderingServiceReceiveRequest],
  ): Unit =
    endpointIdToNetworkRef.addOne(endpoint.id -> (endpoint, ref))

  def setConnected(endpointId: P2PEndpoint.Id, connected: Boolean): Boolean =
    if (connected)
      connectedEndpointIds.add(endpointId)
    else
      connectedEndpointIds.remove(endpointId)

  def getNode(endpointId: P2PEndpoint.Id): Option[BftNodeId] =
    endpointIdToNodeId.get(endpointId)

  def setNode(endpointId: P2PEndpoint.Id, node: BftNodeId): Unit = {
    endpointIdToNodeId.addOne(endpointId -> node)
    nodeIdToEndpointId.addOne(node -> endpointId)
  }

  def getEndpoints: Seq[P2PEndpoint] =
    endpointIdToNetworkRef.values.map(_._1).toSeq

  def authenticatedCount: Int =
    endpointIdToNodeId.size

  def connectedCount: Int =
    connectedEndpointIds.size

  def delete(endpointId: P2PEndpoint.Id): Unit = {
    setConnected(endpointId, connected = false).discard
    endpointIdToNodeId.remove(endpointId).foreach(nodeIdToEndpointId.remove)
    endpointIdToNetworkRef.remove(endpointId).foreach(_._2.close())
  }
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class P2PNetworkOutModule[
    E <: Env[E],
    P2PNetworkRefFactoryT <: P2PNetworkRefFactory[E, BftOrderingServiceReceiveRequest],
](
    thisNode: BftNodeId,
    @VisibleForTesting private[bftordering] val p2pEndpointsStore: P2PEndpointsStore[E],
    metrics: BftOrderingMetrics,
    override val dependencies: P2PNetworkOutModuleDependencies[E, P2PNetworkRefFactoryT],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
    state: P2PNetworkOutModule.State = new P2PNetworkOutModule.State,
)(implicit mc: MetricsContext)
    extends P2PNetworkOut[E, P2PNetworkRefFactoryT]
    with P2PConnectionEventListener {

  val p2pNetworkRefFactory: P2PNetworkRefFactoryT =
    dependencies.createP2PNetworkRefFactory(this)

  private var initialEndpointsCount = 1

  private var maybeSelf: Option[ModuleRef[P2PNetworkOut.Message]] = None

  override def ready(self: ModuleRef[P2PNetworkOut.Message]): Unit = {
    maybeSelf = Some(self)
    self.asyncSendNoTrace(P2PNetworkOut.Start)
  }

  override def onSequencerId(endpointId: P2PEndpoint.Id, nodeId: BftNodeId): Unit =
    maybeSelf.foreach(_.asyncSendNoTrace(P2PNetworkOut.Network.Authenticated(endpointId, nodeId)))

  override def onConnect(endpointId: P2PEndpoint.Id): Unit =
    maybeSelf.foreach(_.asyncSendNoTrace(P2PNetworkOut.Network.Connected(endpointId)))

  override def onDisconnect(endpointId: P2PEndpoint.Id): Unit =
    maybeSelf.foreach(_.asyncSendNoTrace(P2PNetworkOut.Network.Disconnected(endpointId)))

  import state.*

  override def receiveInternal(
      message: P2PNetworkOut.Message
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    message match {
      case P2PNetworkOut.Start =>
        val endpoints =
          context.blockingAwait(p2pEndpointsStore.listEndpoints, DefaultDatabaseReadTimeout)
        initialEndpointsCount = endpoints.size + 1
        connectInitialNodes(endpoints)
        startModulesIfNeeded()

      case P2PNetworkOut.Internal.Connect(endpoint) =>
        connect(endpoint).discard

      case P2PNetworkOut.Internal.Disconnect(endpointId) =>
        disconnect(endpointId)

      case P2PNetworkOut.Network.Connected(endpointId) =>
        if (known.setConnected(endpointId, connected = true)) {
          emitConnectedCount(metrics, known)
          logEndpointsStatus()
        }

      case P2PNetworkOut.Network.Disconnected(endpointId) =>
        if (known.setConnected(endpointId, connected = false)) {
          emitConnectedCount(metrics, known)
          logEndpointsStatus()
        }

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
  )(implicit traceContext: TraceContext): Unit =
    if (node != thisNode)
      networkSendIfKnown(node, message)
    else
      dependencies.p2pNetworkIn.asyncSend(
        messageToSend(message.toProto, maybeNetworkSendInstant = None)
      )

  private def networkSendIfKnown(
      to: BftNodeId,
      message: BftOrderingNetworkMessage,
  )(implicit traceContext: TraceContext): Unit = {
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
        logger.debug(
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

  private def getStatus(
      endpointIds: Option[Iterable[P2PEndpoint.Id]] = None
  )(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): SequencerBftAdminData.PeerNetworkStatus =
    SequencerBftAdminData.PeerNetworkStatus(
      endpointIds
        .getOrElse(
          known.getEndpoints
            .map(_.id)
            .sorted // Sorted for output determinism and easier testing
        )
        .map { endpointId =>
          val defined = known.isDefined(endpointId)
          val connected = known.isConnected(endpointId)
          val maybeNodeId = known.getNode(endpointId)
          PeerEndpointStatus(
            endpointId,
            health = (defined, connected, maybeNodeId) match {
              case (false, _, _) =>
                PeerEndpointHealth(PeerEndpointHealthStatus.UnknownEndpoint, None)
              case (_, false, _) => PeerEndpointHealth(PeerEndpointHealthStatus.Disconnected, None)
              case (_, _, None) =>
                PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None)
              case (_, _, Some(nodeId)) =>
                PeerEndpointHealth(
                  PeerEndpointHealthStatus.Authenticated(
                    SequencerNodeId
                      .fromBftNodeId(nodeId)
                      .getOrElse(abort(s"Node ID '$nodeId' is not a valid sequencer ID"))
                  ),
                  None,
                )
            },
          )
        }
        .toSeq
    )

  private lazy val endpointThresholdForAvailabilityStart =
    AvailabilityModule.quorum(initialEndpointsCount)

  private lazy val endpointThresholdForConsensusStart = strongQuorumSize(initialEndpointsCount)

  private def startModulesIfNeeded()(implicit traceContext: TraceContext): Unit = {
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
  )(implicit traceContext: TraceContext, mc: MetricsContext): Unit =
    ref.asyncP2PSend(maybeNetworkSendInstant => messageToSend(message, maybeNetworkSendInstant))

  private def messageToSend(
      message: BftOrderingMessageBody,
      maybeNetworkSendInstant: Option[Instant],
  )(implicit traceContext: TraceContext): BftOrderingServiceReceiveRequest =
    BftOrderingServiceReceiveRequest(
      traceContext.asW3CTraceContext.map(_.parent).getOrElse(""),
      Some(message),
      thisNode,
      maybeNetworkSendInstant.map(networkSendInstant =>
        Timestamp(networkSendInstant.getEpochSecond, networkSendInstant.getNano)
      ),
    )

  private def connectInitialNodes(
      otherInitialEndpoints: Seq[P2PEndpoint]
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    if (!initialNodesConnecting) {
      logger.debug(s"Connecting to initial nodes: $otherInitialEndpoints")
      otherInitialEndpoints.foreach(connect(_).discard)
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
    val networkRef =
      p2pNetworkRefFactory.createNetworkRef(context, endpoint)
    known.add(endpoint, networkRef)
    emitConnectedCount(metrics, known)
    logEndpointsStatus()
    networkRef
  }

  private def registerAuthenticated(
      endpointId: P2PEndpoint.Id,
      node: BftNodeId,
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit = {
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
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit = {
    logger.debug(
      s"Disconnecting '${known.getNode(endpointId).getOrElse("<unknown>")}' at $endpointId"
    )
    known.delete(endpointId)
    emitConnectedCount(metrics, known)
    logEndpointsStatus()
  }

  private def logEndpointsStatus()(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): Unit =
    logger.info(getStatus().toString)
}

private[bftordering] object P2PNetworkOutModule {

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
