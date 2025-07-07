// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.GrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoActorContext
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
  P2PNetworkRef,
  P2PNetworkRefFactory,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingServiceReceiveRequest,
  BftOrderingServiceReceiveResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, PostStop}

import java.time.{Duration, Instant}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success, Try}

private sealed trait PekkoGrpcP2PConnectionManagerActorMessage
private final case class Initialize() extends PekkoGrpcP2PConnectionManagerActorMessage
private final case class SendMessage(
    createMessage: Option[Instant] => BftOrderingServiceReceiveRequest,
    metricsContext: MetricsContext,
    attemptNumber: Int,
    sendInstant: Instant = Instant.now,
    maybeDelay: Option[FiniteDuration] = None,
) extends PekkoGrpcP2PConnectionManagerActorMessage
private final case class Close() extends PekkoGrpcP2PConnectionManagerActorMessage

final class PekkoP2PNetworkRef(
    connectionHandler: ActorRef[PekkoGrpcP2PConnectionManagerActorMessage],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends P2PNetworkRef[BftOrderingServiceReceiveRequest]
    with NamedLogging {

  connectionHandler ! Initialize()

  override def asyncP2PSend(
      createMessage: Option[Instant] => BftOrderingServiceReceiveRequest
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
    synchronizeWithClosingSync("send-message") {
      connectionHandler ! SendMessage(
        createMessage,
        metricsContext,
        attemptNumber = 1,
      )
    }.discard

  override def onClosed(): Unit =
    connectionHandler ! Close()
}

object PekkoGrpcP2PNetworking {

  private val SendRetryDelay = 2.seconds
  private val MaxAttempts = 5

  final class PekkoP2PNetworkRefFactory(
      clientConnectionManager: P2PGrpcClientConnectionManager,
      override val timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  ) extends P2PNetworkRefFactory[PekkoModuleSystem.PekkoEnv, BftOrderingServiceReceiveRequest]
      with NamedLogging {

    override def createNetworkRef[ActorContextT](
        context: PekkoActorContext[ActorContextT],
        endpoint: P2PEndpoint,
    ): P2PNetworkRef[BftOrderingServiceReceiveRequest] = {
      val security = if (endpoint.transportSecurity) "tls" else "plaintext"
      val actorName =
        s"node-${endpoint.address}-${endpoint.port}-$security-client-connection" // The actor name must be unique.
      logger.debug(
        s"created client connection-managing actor '$actorName'"
      )(TraceContext.empty)
      new PekkoP2PNetworkRef(
        context.underlying.spawn(
          createGrpcP2PConnectionManagerPekkoBehavior(
            endpoint,
            clientConnectionManager,
            loggerFactory,
            metrics,
          ),
          actorName,
        ),
        timeouts,
        loggerFactory,
      )
    }

    override def onClosed(): Unit = {
      logger.info("Closing Pekko client connection manager")(TraceContext.empty)
      clientConnectionManager.close()
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

  private def createGrpcP2PConnectionManagerPekkoBehavior(
      endpoint: P2PEndpoint,
      clientConnectionManager: P2PGrpcClientConnectionManager,
      loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  ): Behavior[PekkoGrpcP2PConnectionManagerActorMessage] = {
    val logger = loggerFactory.getLogger(this.getClass)
    val endpointId = endpoint.id

    // Provides retry logic if not connected
    def scheduleMessageIfNotConnectedBehavior(
        message: PekkoGrpcP2PConnectionManagerActorMessage
    )(whenConnected: StreamObserver[BftOrderingServiceReceiveRequest] => Unit)(implicit
        context: ActorContext[
          PekkoGrpcP2PConnectionManagerActorMessage
        ]
    ): Behavior[PekkoGrpcP2PConnectionManagerActorMessage] = {
      // Emit actor queue latency for the message
      message match {
        case SendMessage(_, metricsContext, _, sendInstant, maybeDelay) =>
          metrics.performance.orderingStageLatency.emitModuleQueueLatency(
            this.getClass.getSimpleName,
            sendInstant,
            maybeDelay,
          )(metricsContext)
        case _ =>
      }
      clientConnectionManager.getServerHandleOrStartConnection(endpoint) match {
        case Some(serverHandle) =>
          // Connection available
          whenConnected(serverHandle)
        case _ =>
          message match {
            case SendMessage(grpcMessage, metricsContext, attemptNumber, _, _) =>
              val newAttemptNumber = attemptNumber + 1
              if (newAttemptNumber <= MaxAttempts) {
                logger.info(
                  s"Connection-managing actor for endpoint in server role $endpointId " +
                    s"couldn't obtain connection yet for send operation, retrying it in $SendRetryDelay, " +
                    s"attempt $newAttemptNumber out of $MaxAttempts"
                )
                // Retrying after a delay due to not being connected:
                //  record the send instant and delay to emit the actor queue latency when processing the message
                val delayedMessage = SendMessage(
                  grpcMessage,
                  metricsContext,
                  newAttemptNumber,
                  sendInstant = Instant.now,
                  maybeDelay = Some(SendRetryDelay),
                )
                context.scheduleOnce(SendRetryDelay, target = context.self, delayedMessage).discard
                metrics.p2p.send.sendsRetried.inc()(metricsContext)
              } else
                logger.info(
                  s"Connection-managing actor for endpoint in server role $endpointId " +
                    s"couldn't obtain connection yet for send operation, no more retries left"
                )
            case Initialize() =>
              // Initialize must always be retried, since there are modules that wait for a quorum of
              // connections to be established before being initialized. So if we stopped retrying too soon,
              // some nodes could get stuck, which could easily happen in a network where nodes are starting
              // up simultaneously and are not immediately reachable to one another.
              logger.info(
                s"Connection-managing actor for endpoint in server role $endpointId " +
                  s"couldn't obtain connection yet for initialize operation, retrying it in $SendRetryDelay"
              )
              context.scheduleOnce(SendRetryDelay, target = context.self, message).discard
              metrics.p2p.send.sendsRetried.inc()(MetricsContext.Empty)
            case Close() => () // should never happen
          }
      }
      Behaviors.same
    }

    def closeBehavior(): Behavior[PekkoGrpcP2PConnectionManagerActorMessage] = {
      logger.info(s"Closing connection-managing actor for endpoint in server role $endpointId")
      clientConnectionManager.closeConnection(endpoint)
      Behaviors.stopped
    }

    Behaviors.setup { implicit context =>
      Behaviors
        .receiveMessage[PekkoGrpcP2PConnectionManagerActorMessage] {
          case initMsg: Initialize =>
            logger.info(s"Starting connection to endpoint $endpointId")
            scheduleMessageIfNotConnectedBehavior(initMsg)(_ => ())
          case sendMsg: SendMessage =>
            scheduleMessageIfNotConnectedBehavior(sendMsg) { serverEndpoint =>
              val now = Instant.now
              val msg = sendMsg.createMessage(Some(now))
              try {
                serverEndpoint.onNext(msg)
                // Network send succeeded (but it may still be lost)
                updateTimer(
                  metrics.p2p.send.networkWriteLatency,
                  Duration.between(sendMsg.sendInstant, now),
                )(sendMsg.metricsContext)
              } catch {
                case exception: Exception =>
                  serverEndpoint.onError(exception) // Required by the gRPC streaming API
                  // gRPC requires onError to be the last event, so the connection must be invalidated even though
                  //  the send operation will be retried.
                  clientConnectionManager.closeConnection(endpoint)
                  // Retrying after a delay due to an exception:
                  //  record the send instant and delay to emit the actor queue latency when processing the message
                  val newAttemptNumber = sendMsg.attemptNumber + 1
                  if (newAttemptNumber <= MaxAttempts) {
                    logger.info(
                      s"Connection-managing actor for endpoint in server role $endpointId couldn't send $msg, " +
                        s"invalidating the connection and retrying in $SendRetryDelay, " +
                        s"attempt $newAttemptNumber out of $MaxAttempts",
                      exception,
                    )
                    context
                      .scheduleOnce(
                        SendRetryDelay,
                        target = context.self,
                        sendMsg.copy(
                          attemptNumber = newAttemptNumber,
                          sendInstant = Instant.now,
                          maybeDelay = Some(SendRetryDelay),
                        ),
                      )
                      .discard
                    metrics.p2p.send.sendsRetried.inc()(sendMsg.metricsContext)
                  } else
                    logger.info(
                      s"Connection-managing actor for endpoint in server role $endpointId couldn't send $msg, " +
                        s"invalidating the connection. No more retries left.",
                      exception,
                    )
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
