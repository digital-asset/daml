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
  PeerConnectionStatus,
  PeerEndpointHealth,
  PeerEndpointHealthStatus,
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
  P2PAddress,
  P2PConnectionEventListener,
  P2PNetworkManager,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessage,
  BftOrderingMessageBody,
}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.timestamp.Timestamp

import java.time.Instant
import scala.collection.mutable
import scala.util.{Failure, Success}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final class P2PNetworkOutModule[
    E <: Env[E],
    P2PNetworkManagerT <: P2PNetworkManager[E, BftOrderingMessage],
](
    thisBftNodeId: BftNodeId,
    isGenesis: Boolean,
    bootstrapTopologySize: Int,
    state: P2PNetworkOutModule.State,
    @VisibleForTesting private[bftordering] val p2pEndpointsStore: P2PEndpointsStore[E],
    metrics: BftOrderingMetrics,
    override val dependencies: P2PNetworkOutModuleDependencies[E, P2PNetworkManagerT],
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit mc: MetricsContext)
    extends P2PNetworkOut[E, P2PNetworkManagerT]
    with P2PConnectionEventListener {

  private val connectedP2PEndpointIds = mutable.Set.empty[P2PEndpoint.Id]

  val p2pNetworkManager: P2PNetworkManagerT =
    dependencies.createP2PNetworkManager(this, dependencies.p2pNetworkIn)

  private var maybeSelf: Option[ModuleRef[P2PNetworkOut.Message]] = None

  override def ready(self: ModuleRef[P2PNetworkOut.Message]): Unit = {
    maybeSelf = Some(self)
    self.asyncSendNoTrace(P2PNetworkOut.Start)
  }

  override def onSequencerId(p2pEndpointId: P2PEndpoint.Id, bftNodeId: BftNodeId)(implicit
      traceContext: TraceContext
  ): Unit =
    maybeSelf.foreach(_.asyncSend(P2PNetworkOut.Network.Authenticated(bftNodeId, p2pEndpointId)))

  override def onConnect(p2pEndpointId: P2PEndpoint.Id)(implicit traceContext: TraceContext): Unit =
    maybeSelf.foreach(_.asyncSend(P2PNetworkOut.Network.Connected(p2pEndpointId)))

  override def onDisconnect(p2pEndpointId: P2PEndpoint.Id)(implicit
      traceContext: TraceContext
  ): Unit =
    maybeSelf.foreach(_.asyncSend(P2PNetworkOut.Network.Disconnected(p2pEndpointId)))

  import state.*

  override def receiveInternal(
      message: P2PNetworkOut.Message
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    message match {
      case P2PNetworkOut.Start =>
        val p2pEndpoints =
          context.blockingAwait(p2pEndpointsStore.listEndpoints, DefaultDatabaseReadTimeout)
        connectInitialNodes(p2pEndpoints)
        startModulesIfNeeded()

      case P2PNetworkOut.Internal.Connect(p2pEndpointId) =>
        connect(p2pEndpointId).discard

      case P2PNetworkOut.Internal.Disconnect(p2pEndpointId) =>
        disconnect(p2pEndpointId)

      case P2PNetworkOut.Network.Connected(p2pEndpointId) =>
        if (setConnected(p2pEndpointId, connected = true)) {
          emitConnectedCount(metrics, connectedCount)
          logEndpointsStatus()
        }

      case P2PNetworkOut.Network.Disconnected(p2pEndpointId) =>
        if (setConnected(p2pEndpointId, connected = false)) {
          emitConnectedCount(metrics, p2pConnectionState.authenticatedCount)
          logEndpointsStatus()
        }

      case P2PNetworkOut.Network.Authenticated(bftNodeId, p2pEndpointId) =>
        if (bftNodeId == thisBftNodeId) {
          emitIdentityEquivocation(metrics, p2pEndpointId, bftNodeId)
          logger.warn(
            s"A node authenticated from $p2pEndpointId with the sequencer ID of this very node " +
              s"($thisBftNodeId); this could indicate malicious behavior: disconnecting it"
          )
          disconnect(p2pEndpointId)
        } else {
          p2pConnectionState.getBftNodeId(p2pEndpointId) match {
            case Some(existingBftNodeId) if existingBftNodeId != bftNodeId =>
              emitIdentityEquivocation(metrics, p2pEndpointId, existingBftNodeId)
              logger.warn(
                s"On reconnection, a node authenticated from endpoint $p2pEndpointId " +
                  s"with a different sequencer id $bftNodeId, but it was already authenticated " +
                  s"as $existingBftNodeId; this could indicate malicious behavior: disconnecting it"
              )
              disconnect(p2pEndpointId)
            case _ =>
              logger.debug(s"Authenticated node $bftNodeId at $p2pEndpointId")
              registerAuthenticated(p2pEndpointId, bftNodeId)
          }
        }

      case P2PNetworkOut.Multicast(message, bftNodeIds) =>
        bftNodeIds.toSeq.sorted // For determinism
          .foreach(sendIfKnown(_, message))

      case admin: P2PNetworkOut.Admin =>
        processModuleAdmin(admin)
    }

  private def connectedCount: Int =
    connectedP2PEndpointIds.size

  private def isConnected(p2pEndpointId: P2PEndpoint.Id): Boolean =
    connectedP2PEndpointIds.contains(p2pEndpointId)

  private def setConnected(p2pEndpointId: P2PEndpoint.Id, connected: Boolean): Boolean =
    if (connected)
      connectedP2PEndpointIds.add(p2pEndpointId)
    else
      connectedP2PEndpointIds.remove(p2pEndpointId)

  private def sendIfKnown(
      bftNodeId: BftNodeId,
      message: BftOrderingNetworkMessage,
  )(implicit traceContext: TraceContext): Unit =
    if (bftNodeId != thisBftNodeId)
      networkSendIfKnown(bftNodeId, message)
    else
      dependencies.p2pNetworkIn.asyncSend(
        messageToSend(message.toProto, maybeNetworkSendInstant = None)
      )

  private def networkSendIfKnown(
      destinationBftNodeId: BftNodeId,
      message: BftOrderingNetworkMessage,
  )(implicit traceContext: TraceContext): Unit = {
    val serializedMessage = message.toProto
    p2pConnectionState.actOnBftNodeId(
      destinationBftNodeId,
      ifEmpty = {
        val mc1 =
          sendMetricsContext(
            metrics,
            serializedMessage,
            destinationBftNodeId,
            droppedAsUnauthenticated = true,
          )
        locally {
          implicit val mc: MetricsContext = mc1
          emitSendStats(metrics, serializedMessage)
        }
        logger.debug(
          s"Dropping network message to unknown $destinationBftNodeId (possibly unauthenticated as of yet)"
        )
        logger.trace(s"Dropped message to $destinationBftNodeId is: $message")
      },
    ) { ref =>
      val mc1: MetricsContext =
        sendMetricsContext(
          metrics,
          serializedMessage,
          destinationBftNodeId,
          droppedAsUnauthenticated = false,
        )
      locally {
        logger.debug(s"Sending network message to $destinationBftNodeId")
        logger.trace(s"Message to $destinationBftNodeId is: $message")
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
      case Admin.AddEndpoint(p2pEndpoint, callback) =>
        if (p2pConnectionState.isDefined(p2pEndpoint.id)) {
          callback(false)
        } else {
          context.pipeToSelf(p2pEndpointsStore.addEndpoint(p2pEndpoint)) {
            case Success(hasBeenAdded) =>
              callback(hasBeenAdded)
              if (hasBeenAdded)
                Some(P2PNetworkOut.Internal.Connect(p2pEndpoint))
              else
                None
            case Failure(exception) =>
              abort(s"Failed to add endpoint $p2pEndpoint", exception)
          }
        }
      case Admin.RemoveEndpoint(p2pEndpointId, callback) =>
        if (p2pConnectionState.isDefined(p2pEndpointId)) {
          context.pipeToSelf(p2pEndpointsStore.removeEndpoint(p2pEndpointId)) {
            case Success(hasBeenRemoved) =>
              callback(hasBeenRemoved)
              if (hasBeenRemoved)
                Some(P2PNetworkOut.Internal.Disconnect(p2pEndpointId))
              else
                None
            case Failure(exception) =>
              abort(s"Failed to remove endpoint $p2pEndpointId", exception)
          }
        } else {
          callback(false)
        }
      case Admin.GetStatus(callback, p2pEndpointIds) =>
        callback(getStatus(p2pEndpointIds))
    }

  private def getStatus(
      p2pEndpointIds: Option[Iterable[P2PEndpoint.Id]] = None
  )(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): SequencerBftAdminData.PeerNetworkStatus =
    SequencerBftAdminData.PeerNetworkStatus(
      p2pEndpointIds
        .getOrElse(
          p2pConnectionState.connections
            .map(_.id)
            .sorted // Sorted for output determinism and easier testing
        )
        .map { p2pEndpointId =>
          val defined = p2pConnectionState.isDefined(p2pEndpointId)
          val outgoing = p2pConnectionState.isOutgoing(p2pEndpointId)
          val connected = isConnected(p2pEndpointId)
          val maybeNodeId = p2pConnectionState.getBftNodeId(p2pEndpointId)
          PeerConnectionStatus.PeerEndpointStatus(
            p2pEndpointId,
            outgoing,
            health = (defined, connected, maybeNodeId) match {
              case (false, _, _) =>
                PeerEndpointHealth(PeerEndpointHealthStatus.UnknownEndpoint, None)
              case (_, false, _) => PeerEndpointHealth(PeerEndpointHealthStatus.Disconnected, None)
              case (_, _, None) =>
                PeerEndpointHealth(PeerEndpointHealthStatus.Unauthenticated, None)
              case (_, _, Some(bftNodeId)) =>
                PeerEndpointHealth(
                  PeerEndpointHealthStatus.Authenticated(
                    SequencerNodeId
                      .fromBftNodeId(bftNodeId)
                      .getOrElse(abort(s"Node ID '$bftNodeId' is not a valid sequencer ID"))
                  ),
                  None,
                )
            },
          )
        }
        .toSeq
    )

  private lazy val p2pEndpointThresholdForAvailabilityStart =
    AvailabilityModule.quorum(bootstrapTopologySize)

  private lazy val p2pEndpointThresholdForConsensusStart =
    strongQuorumSize(bootstrapTopologySize)

  private def startModulesIfNeeded()(implicit traceContext: TraceContext): Unit = {
    if (!mempoolStarted) {
      logger.debug(s"Starting mempool")
      dependencies.mempool.asyncSend(Mempool.Start)
      mempoolStarted = true
    }
    // Waiting for just a quorum (minus self) of nodes to be authenticated assumes that they are not faulty
    if (!availabilityStarted) {
      if (
        !isGenesis || maxNodesContemporarilyAuthenticated >= p2pEndpointThresholdForAvailabilityStart
      ) {
        logger.debug(
          s"Threshold $p2pEndpointThresholdForAvailabilityStart reached: starting availability"
        )
        dependencies.availability.asyncSend(Availability.Start)
        availabilityStarted = true
      }
    }
    if (!consensusStarted) {
      if (
        !isGenesis || maxNodesContemporarilyAuthenticated >= p2pEndpointThresholdForConsensusStart
      ) {
        logger.debug(
          s"Threshold $p2pEndpointThresholdForConsensusStart reached: starting consensus"
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
      ref: P2PNetworkRef[BftOrderingMessage],
      message: BftOrderingMessageBody,
  )(implicit traceContext: TraceContext, mc: MetricsContext): Unit =
    ref.asyncP2PSend(maybeNetworkSendInstant => messageToSend(message, maybeNetworkSendInstant))

  private def messageToSend(
      message: BftOrderingMessageBody,
      maybeNetworkSendInstant: Option[Instant],
  )(implicit traceContext: TraceContext): BftOrderingMessage =
    BftOrderingMessage(
      traceContext.asW3CTraceContext.map(_.parent).getOrElse(""),
      Some(message),
      thisBftNodeId,
      maybeNetworkSendInstant.map(networkSendInstant =>
        Timestamp(networkSendInstant.getEpochSecond, networkSendInstant.getNano)
      ),
    )

  private def connectInitialNodes(
      otherInitialP2PEndpoints: Seq[P2PEndpoint]
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit =
    if (!initialNodesConnecting) {
      logger.debug(s"Connecting to initial nodes: $otherInitialP2PEndpoints")
      otherInitialP2PEndpoints.foreach(connect(_).discard)
      initialNodesConnecting = true
    }

  private def connect(
      p2pEndpoint: P2PEndpoint
  )(implicit
      context: E#ActorContextT[P2PNetworkOut.Message],
      traceContext: TraceContext,
  ): P2PNetworkRef[BftOrderingMessage] = {
    logger.debug(
      s"Connecting new node at ${p2pEndpoint.id}"
    )
    val networkRef =
      p2pNetworkManager.createNetworkRef(context, P2PAddress.Endpoint(p2pEndpoint))
    p2pConnectionState.addNetworkRef(p2pEndpoint, networkRef)
    emitConnectedCount(metrics, connectedCount)
    logEndpointsStatus()
    networkRef
  }

  private def registerAuthenticated(
      p2pEndpointId: P2PEndpoint.Id,
      bftNodeId: BftNodeId,
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit = {
    logger.debug(s"Registering '$bftNodeId' at $p2pEndpointId")
    p2pConnectionState.setBftNodeId(p2pEndpointId, bftNodeId)
    emitAuthenticatedCount(metrics, p2pConnectionState.authenticatedCount)
    logEndpointsStatus()
    maxNodesContemporarilyAuthenticated = Math.max(
      maxNodesContemporarilyAuthenticated,
      p2pConnectionState.authenticatedCount + 1,
    ) // +1 for self
    startModulesIfNeeded()
  }

  private def disconnect(
      p2pEndpointId: P2PEndpoint.Id
  )(implicit context: E#ActorContextT[P2PNetworkOut.Message], traceContext: TraceContext): Unit = {
    logger.debug(
      s"Disconnecting '${p2pConnectionState.getBftNodeId(p2pEndpointId).getOrElse("<unknown>")}' at $p2pEndpointId"
    )
    p2pConnectionState.delete(p2pEndpointId)
    setConnected(p2pEndpointId = p2pEndpointId, connected = false).discard
    emitConnectedCount(metrics, connectedCount)
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
  final class State(val p2pConnectionState: P2PConnectionState) {

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
    var maxNodesContemporarilyAuthenticated = 1 // i.e., self
  }
}
