// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  ServerHandleInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ClientP2PNetworkManager,
  ModuleRef,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveResponse
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, PostStop}

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

import PekkoModuleSystem.PekkoActorContext

private sealed trait PekkoGrpcConnectionManagerActorMessage[P2PMessageT]
private final case class Initialize[P2PMessageT]()
    extends PekkoGrpcConnectionManagerActorMessage[P2PMessageT]
private final case class SendMessage[P2PMessageT](
    grpcMessage: P2PMessageT,
    onCompletion: () => Unit,
) extends PekkoGrpcConnectionManagerActorMessage[P2PMessageT]
private final case class Close[P2PMessageT]()
    extends PekkoGrpcConnectionManagerActorMessage[P2PMessageT]

final class PekkoP2PNetworkRef[P2PMessageT](
    connectionHandler: ActorRef[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends P2PNetworkRef[P2PMessageT]
    with NamedLogging {

  connectionHandler ! Initialize[P2PMessageT]()

  override def asyncP2PSend(msg: P2PMessageT)(
      onCompletion: => Unit
  )(implicit traceContext: TraceContext): Unit = {
    val _ = performUnlessClosing("send-message") {
      connectionHandler ! SendMessage(msg, () => onCompletion)
    }
  }

  override def onClosed(): Unit =
    connectionHandler ! Close()
}

object PekkoGrpcP2PNetworking {

  private val SendRetryDelay = 2.seconds

  final class PekkoClientP2PNetworkManager[P2PMessageT](
      getServerHandleOrStartConnection: P2PEndpoint => Option[ServerHandleInfo[P2PMessageT]],
      closeConnection: P2PEndpoint => Unit,
      timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  ) extends ClientP2PNetworkManager[PekkoModuleSystem.PekkoEnv, P2PMessageT]
      with NamedLogging {

    override def createNetworkRef[ActorContextT](
        context: PekkoActorContext[ActorContextT],
        endpoint: P2PEndpoint,
    )(
        onNode: (P2PEndpoint.Id, BftNodeId) => Unit
    ): P2PNetworkRef[P2PMessageT] = {
      val security = if (endpoint.transportSecurity) "tls" else "plaintext"
      val actorName =
        s"node-${endpoint.address}-${endpoint.port}-$security-client-connection" // The actor name must be unique.
      logger.debug(
        s"created client connection-managing actor '$actorName'"
      )(TraceContext.empty)
      new PekkoP2PNetworkRef(
        context.underlying.spawn(
          createGrpcConnectionManagerPekkoBehavior(
            endpoint,
            getServerHandleOrStartConnection,
            closeConnection,
            onNode,
            loggerFactory,
          ),
          actorName,
        ),
        timeouts,
        loggerFactory,
      )
    }
  }

  def tryCreateServerHandle[P2PMessageT](
      node: BftNodeId,
      inputModule: ModuleRef[P2PMessageT],
      clientHandle: StreamObserver[BftOrderingServiceReceiveResponse],
      cleanupClientHandle: StreamObserver[BftOrderingServiceReceiveResponse] => Unit,
      loggerFactory: NamedLoggerFactory,
  )(implicit metricsContext: MetricsContext): StreamObserver[P2PMessageT] =
    Try(
      clientHandle.onNext(
        BftOrderingServiceReceiveResponse(node)
      )
    ) match {
      case Failure(exception) =>
        clientHandle.onError(exception) // Required by the gRPC streaming API
        throw exception // gRPC requires onError to be the last event, so it doesn't make sense to return a handler
      case Success(_) =>
        new GrpcServerHandle[P2PMessageT](
          inputModule,
          clientHandle,
          cleanupClientHandle,
          loggerFactory,
        )
    }

  private def createGrpcConnectionManagerPekkoBehavior[P2PMessageT](
      endpoint: P2PEndpoint,
      getServerHandle: P2PEndpoint => Option[ServerHandleInfo[P2PMessageT]],
      closeConnection: P2PEndpoint => Unit,
      onNode: (P2PEndpoint.Id, BftNodeId) => Unit,
      loggerFactory: NamedLoggerFactory,
  ): Behavior[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] = {
    val logger = loggerFactory.getLogger(this.getClass)
    val endpointId = endpoint.id

    def scheduleMessageIfNotConnectedBehavior(
        message: => PekkoGrpcConnectionManagerActorMessage[P2PMessageT]
    )(whenConnected: StreamObserver[P2PMessageT] => Unit)(implicit
        context: ActorContext[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]]
    ): Behavior[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] = {
      getServerHandle(endpoint) match {
        case Some(ServerHandleInfo(sequencerId, serverHandle, isNewlyConnected)) =>
          // Connection available
          if (isNewlyConnected)
            onNode(endpointId, SequencerNodeId.toBftNodeId(sequencerId))
          whenConnected(serverHandle)
        case _ =>
          logger.info(
            s"Connection-managing actor for endpoint in server role $endpointId " +
              s"couldn't obtain connection yet, retrying in $SendRetryDelay"
          )
          val _ = context.scheduleOnce(SendRetryDelay, target = context.self, message)
      }
      Behaviors.same
    }

    def closeBehavior(): Behavior[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] = {
      logger.info(s"Closing connection-managing actor for endpoint in server role $endpointId")
      closeConnection(endpoint)
      Behaviors.stopped
    }

    Behaviors.setup { implicit context =>
      Behaviors
        .receiveMessage[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] {
          case initMsg: Initialize[P2PMessageT] =>
            logger.info(s"Starting connection to endpoint $endpointId")
            scheduleMessageIfNotConnectedBehavior(initMsg)(_ => ())
          case sendMsg: SendMessage[P2PMessageT] =>
            scheduleMessageIfNotConnectedBehavior(sendMsg) { serverEndpoint =>
              try {
                serverEndpoint.onNext(sendMsg.grpcMessage)
                sendMsg.onCompletion()
              } catch {
                case exception: Exception =>
                  logger.info(
                    s"Connection-managing actor for endpoint in server role $endpointId couldn't send ${sendMsg.grpcMessage}, " +
                      s"invalidating the connection and retrying in $SendRetryDelay",
                    exception,
                  )
                  serverEndpoint.onError(exception) // Required by the gRPC streaming API
                  // gRPC requires onError to be the last event, so the connection must be invalidated even though
                  //  the send operation will be retried.
                  closeConnection(endpoint)
                  val _ = context.scheduleOnce(SendRetryDelay, target = context.self, sendMsg)
              }
            }
          case Close() =>
            logger.info(
              s"Connection-managing actor for endpoint in server role $endpointId is stopping, closing"
            )
            closeBehavior()
        }
        .receiveSignal { case (_, PostStop) =>
          logger.info(
            s"Connection-managing actor for endpoint in server role $endpointId stopped, closing"
          )
          closeBehavior()
        }
    }
  }
}
