// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  P2PEndpoint,
  ServerHandleInfo,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ClientP2PNetworkManager,
  ModuleName,
  ModuleRef,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveResponse
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, PostStop}

import java.time.{Duration, Instant}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try}

import PekkoModuleSystem.PekkoActorContext

private sealed trait PekkoGrpcConnectionManagerActorMessage[P2PMessageT]
private final case class Initialize[P2PMessageT]()
    extends PekkoGrpcConnectionManagerActorMessage[P2PMessageT]
private final case class SendMessage[P2PMessageT](
    createMessage: Option[Instant] => P2PMessageT,
    metricsContext: MetricsContext,
    sendInstant: Instant = Instant.now,
    maybeDelay: Option[FiniteDuration] = None,
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

  override def asyncP2PSend(
      createMessage: Option[Instant] => P2PMessageT
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
    performUnlessClosing("send-message") {
      connectionHandler ! SendMessage(
        createMessage,
        metricsContext,
      )
    }.discard

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
      metrics: BftOrderingMetrics,
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
            metrics,
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
      getMessageSendInstant: P2PMessageT => Option[Instant],
      loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
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
          getMessageSendInstant,
          loggerFactory,
          metrics,
        )
    }

  private def createGrpcConnectionManagerPekkoBehavior[P2PMessageT](
      endpoint: P2PEndpoint,
      getServerHandle: P2PEndpoint => Option[ServerHandleInfo[P2PMessageT]],
      closeConnection: P2PEndpoint => Unit,
      onNode: (P2PEndpoint.Id, BftNodeId) => Unit,
      loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  ): Behavior[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] = {
    val logger = loggerFactory.getLogger(this.getClass)
    val endpointId = endpoint.id

    // Provides retry logic if not connected
    def scheduleMessageIfNotConnectedBehavior(
        message: PekkoGrpcConnectionManagerActorMessage[P2PMessageT]
    )(whenConnected: StreamObserver[P2PMessageT] => Unit)(implicit
        context: ActorContext[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]]
    ): Behavior[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] = {
      // Emit actor queue latency for the message
      message match {
        case SendMessage(_, metricsContext, sendInstant, maybeDelay) =>
          metrics.performance.orderingStageLatency.emitModuleQueueLatency(
            ModuleName(this.getClass.getSimpleName),
            sendInstant,
            maybeDelay,
          )(metricsContext)
        case _ =>
      }
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
          // Retrying after a delay due to not being connected:
          //  record the send instant and delay to emit the actor queue latency when processing the message
          val (delayedMessage, metricsContext) =
            message match {
              case SendMessage(grpcMessage, metricsContext, _, _) =>
                SendMessage(
                  grpcMessage,
                  metricsContext,
                  sendInstant = Instant.now,
                  maybeDelay = Some(SendRetryDelay),
                ) -> metricsContext
              case _ =>
                message -> MetricsContext.Empty
            }
          context.scheduleOnce(SendRetryDelay, target = context.self, delayedMessage).discard
          metrics.p2p.send.sendsRetried.inc()(metricsContext)
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
              val now = Instant.now
              val msg = sendMsg.createMessage(Some(now))
              try {
                serverEndpoint.onNext(msg)
                // Network send succeeded (but it may still be lost)
                metrics.p2p.send.networkWriteLatency.update(
                  Duration.between(sendMsg.sendInstant, now)
                )(sendMsg.metricsContext)
              } catch {
                case exception: Exception =>
                  logger.info(
                    s"Connection-managing actor for endpoint in server role $endpointId couldn't send $msg, " +
                      s"invalidating the connection and retrying in $SendRetryDelay",
                    exception,
                  )
                  serverEndpoint.onError(exception) // Required by the gRPC streaming API
                  // gRPC requires onError to be the last event, so the connection must be invalidated even though
                  //  the send operation will be retried.
                  closeConnection(endpoint)
                  // Retrying after a delay due to an exception:
                  //  record the send instant and delay to emit the actor queue latency when processing the message
                  context
                    .scheduleOnce(
                      SendRetryDelay,
                      target = context.self,
                      sendMsg.copy(sendInstant = Instant.now, maybeDelay = Some(SendRetryDelay)),
                    )
                    .discard
                  metrics.p2p.send.sendsRetried.inc()(sendMsg.metricsContext)
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
