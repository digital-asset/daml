// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.P2PEndpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoActorContext
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
  P2PNetworkRef,
  P2PNetworkRefFactory,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.abort
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

private sealed trait PekkoP2PGrpcConnectionManagerActorMessage

/** Asks the connection-managing actor to initialize the connection without performing a send; used
  * to create a connection eagerly rather than the first time a message is sent.
  */
private final case class Initialize() extends PekkoP2PGrpcConnectionManagerActorMessage

private final case class SendMessage(
    createMessage: Option[Instant] => BftOrderingServiceReceiveRequest,
    metricsContext: MetricsContext,
    attemptNumber: Int,
    sendInstant: Instant = Instant.now,
    maybeDelay: Option[FiniteDuration] = None,
) extends PekkoP2PGrpcConnectionManagerActorMessage

/** Closes the connection-managing actor. Sent as part of disconnecting an endpoint.
  */
private final case class Close() extends PekkoP2PGrpcConnectionManagerActorMessage

final class PekkoP2PNetworkRef(
    connectionManagingActorRef: ActorRef[PekkoP2PGrpcConnectionManagerActorMessage],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends P2PNetworkRef[BftOrderingServiceReceiveRequest]
    with NamedLogging {

  connectionManagingActorRef ! Initialize()

  override def asyncP2PSend(
      createMessage: Option[Instant] => BftOrderingServiceReceiveRequest
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit =
    synchronizeWithClosingSync("send-message") {
      connectionManagingActorRef ! SendMessage(
        createMessage,
        metricsContext,
        attemptNumber = 1,
      )
    }.discard

  override def onClosed(): Unit = {
    implicit val traceContext: TraceContext = TraceContext.empty
    logger.debug(s"Sending Close message to connection managing actor for ref $this")
    connectionManagingActorRef ! Close()
  }
}

object PekkoP2PGrpcNetworking {

  private val SendRetryDelay = 2.seconds
  private val MaxAttempts = 5

  final class PekkoP2PGrpcNetworkRefFactory(
      clientConnectionManager: P2PGrpcClientConnectionManager,
      override val timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  ) extends P2PNetworkRefFactory[PekkoModuleSystem.PekkoEnv, BftOrderingServiceReceiveRequest]
      with NamedLogging {

    private implicit val traceContext: TraceContext = TraceContext.empty

    override def createNetworkRef[ActorContextT](
        context: PekkoActorContext[ActorContextT],
        endpoint: P2PEndpoint,
    ): P2PNetworkRef[BftOrderingServiceReceiveRequest] = {
      val security = if (endpoint.transportSecurity) "tls" else "plaintext"
      val actorName =
        s"node-${endpoint.address}-${endpoint.port}-$security-client-connection" // The actor name must be unique.
      logger.debug(s"created client connection-managing actor '$actorName'")
      new PekkoP2PNetworkRef(
        context.underlying.spawn(
          createGrpcP2PConnectionManagerPekkoBehavior(
            endpoint,
            actorName,
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
      logger.info("Closing P2P gRPC client connection manager")
      clientConnectionManager.close()
    }
  }

  def tryCreateServerSidePeerReceiver(
      node: BftNodeId,
      inputModule: ModuleRef[BftOrderingServiceReceiveRequest],
      peerSender: StreamObserver[BftOrderingServiceReceiveResponse],
      cleanupPeerSender: StreamObserver[BftOrderingServiceReceiveResponse] => Unit,
      getMessageSendInstant: BftOrderingServiceReceiveRequest => Option[Instant],
      loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  )(implicit metricsContext: MetricsContext): P2PGrpcStreamingServerSideReceiver =
    Try(
      peerSender.onNext(BftOrderingServiceReceiveResponse(node))
    ) match {
      case Failure(exception) =>
        peerSender.onError(exception) // Required by the gRPC streaming API
        throw exception // gRPC requires onError to be the last event, so it doesn't make sense to return a handler
      case Success(_) =>
        new P2PGrpcStreamingServerSideReceiver(
          inputModule,
          peerSender,
          cleanupPeerSender,
          getMessageSendInstant,
          loggerFactory,
          metrics,
        )
    }

  private def createGrpcP2PConnectionManagerPekkoBehavior(
      endpoint: P2PEndpoint,
      actorName: String,
      clientConnectionManager: P2PGrpcClientConnectionManager,
      loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  ): Behavior[PekkoP2PGrpcConnectionManagerActorMessage] = {
    implicit val traceContext: TraceContext = TraceContext.empty
    val logger = loggerFactory.getTracedLogger(this.getClass)
    val endpointId = endpoint.id

    // Reschedules an Initialize or Send if the connection is not available yet
    def scheduleMessageIfNotConnectedBehavior(
        message: PekkoP2PGrpcConnectionManagerActorMessage
    )(whenConnected: StreamObserver[BftOrderingServiceReceiveRequest] => Unit)(implicit
        context: ActorContext[
          PekkoP2PGrpcConnectionManagerActorMessage
        ]
    ): Unit = {
      // Emit actor queue latency for the message
      message match {
        case SendMessage(_, metricsContext, _, sendInstant, maybeDelay) =>
          // Emit actor queue latency for the message
          metrics.performance.orderingStageLatency.emitModuleQueueLatency(
            this.getClass.getSimpleName,
            sendInstant,
            maybeDelay,
          )(metricsContext)
        case _: Initialize =>
          logger.debug(s"Connection-managing actor $actorName for $endpointId received Initialize")
        case _ =>
      }
      clientConnectionManager.getPeerSenderOrStartConnection(endpoint) match {
        case Some(peerSender) =>
          logger.debug(
            s"Connection-managing actor $actorName for $endpointId found connection available"
          )
          whenConnected(peerSender)
        case _ =>
          message match {
            case SendMessage(grpcMessage, metricsContext, attemptNumber, _, _) =>
              val newAttemptNumber = attemptNumber + 1
              if (newAttemptNumber <= MaxAttempts) {
                logger.info(
                  s"Connection-managing actor $actorName for $endpointId " +
                    s"couldn't yet obtain connection for Send, retrying it in $SendRetryDelay, " +
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
                  s"Connection-managing actor $actorName for $endpointId " +
                    s"couldn't yet obtain connection yet for Send, no more retries left"
                )
            case Initialize() =>
              // Initialize must always be retried, since there are modules that wait for a quorum of
              // connections to be established before being initialized. So if we stopped retrying too soon,
              // some nodes could get stuck, which could easily happen in a network where nodes are starting
              // up simultaneously and are not immediately reachable to one another.
              logger.info(
                s"Connection-managing actor $actorName for $endpointId " +
                  s"couldn't yet obtain connection for Initialize, retrying it in $SendRetryDelay"
              )
              context.scheduleOnce(SendRetryDelay, target = context.self, message).discard
              metrics.p2p.send.sendsRetried.inc()(MetricsContext.Empty)
            case Close() =>
              abort(
                logger,
                s"Connection-managing actor $actorName for $endpointId is unexpectedly processing " +
                  "Close messages with retries",
              )
          }
      }
    }

    def closeBehavior(): Behavior[PekkoP2PGrpcConnectionManagerActorMessage] = {
      logger.info(s"Closing connection-managing actor for $endpointId")
      clientConnectionManager.closeConnection(endpoint)
      Behaviors.stopped
    }

    Behaviors.setup { implicit pekkoActorContext =>
      Behaviors
        .receiveMessage[PekkoP2PGrpcConnectionManagerActorMessage] {
          case initMsg: Initialize =>
            logger.debug(s"Connection-managing actor $actorName initializing to $endpointId")
            scheduleMessageIfNotConnectedBehavior(initMsg)(_ => ())
            Behaviors.same
          case sendMsg: SendMessage =>
            scheduleMessageIfNotConnectedBehavior(sendMsg) { peerSender =>
              val now = Instant.now
              val msg = sendMsg.createMessage(Some(now))
              try {
                logger.trace(
                  s"Connection-managing actor $actorName sending message to sender $peerSender for $endpointId"
                )
                peerSender.onNext(msg)
                // Network send succeeded (but it may still be lost)
                updateTimer(
                  metrics.p2p.send.networkWriteLatency,
                  Duration.between(sendMsg.sendInstant, now),
                )(sendMsg.metricsContext)
              } catch {
                case exception: Exception =>
                  logger.debug(
                    s"Connection-managing actor $actorName failed sending message to sender $peerSender for $endpointId",
                    exception,
                  )
                  peerSender.onError(exception) // Required by the gRPC streaming API
                  // gRPC requires onError to be the last event, so the connection must be invalidated even though
                  //  the send operation will be retried.
                  clientConnectionManager.closeConnection(endpoint)
                  // Retrying after a delay due to an exception:
                  //  record the send instant and delay to emit the actor queue latency when processing the message
                  val newAttemptNumber = sendMsg.attemptNumber + 1
                  if (newAttemptNumber <= MaxAttempts) {
                    logger.info(
                      s"Connection-managing actor $actorName for $endpointId couldn't send $msg, " +
                        s"invalidating the connection and retrying in $SendRetryDelay, " +
                        s"attempt $newAttemptNumber out of $MaxAttempts",
                      exception,
                    )
                    pekkoActorContext
                      .scheduleOnce(
                        SendRetryDelay,
                        target = pekkoActorContext.self,
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
                      s"Connection-managing actor $actorName for $endpointId couldn't send $msg, " +
                        s"invalidating the connection. No more retries left.",
                      exception,
                    )
              }
            }
            Behaviors.same
          case Close() =>
            logger.info(
              s"Connection-managing actor $actorName for $endpointId is stopping, closing"
            )
            closeBehavior()
        }
        .receiveSignal { case (_, PostStop) =>
          logger.info(s"Connection-managing actor $actorName for $endpointId stopped, closing")
          closeBehavior()
        }
    }
  }
}
