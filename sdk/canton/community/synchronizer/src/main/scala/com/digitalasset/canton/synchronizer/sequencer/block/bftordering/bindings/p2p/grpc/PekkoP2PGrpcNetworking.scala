// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics.updateTimer
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  P2PEndpoint,
  failGrpcStreamObserver,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.PekkoActorContext
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  P2PAddress,
  P2PNetworkManager,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.abort
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingMessage
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, PostStop}

import java.time.{Duration, Instant}
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.{DurationInt, FiniteDuration}

private sealed trait PekkoP2PGrpcConnectionManagerActorMessage

/** Asks the connection-managing actor to initialize the connection without performing a Send; used
  * to create a connection eagerly rather than the first time a message is sent.
  */
private final case class Initialize() extends PekkoP2PGrpcConnectionManagerActorMessage

private final case class SendMessage(
    createMessage: Option[Instant] => BftOrderingMessage,
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
    val actorName: String,
    outstandingMessages: AtomicInteger,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends P2PNetworkRef[BftOrderingMessage]
    with NamedLogging {

  connectionManagingActorRef ! Initialize()

  override def toString: String = this.getClass.getSimpleName + s"($actorName)"

  override def asyncP2PSend(
      createMessage: Option[Instant] => BftOrderingMessage
  )(implicit traceContext: TraceContext, metricsContext: MetricsContext): Unit = {
    outstandingMessages.incrementAndGet().discard
    synchronizeWithClosingSync("send-message") {
      connectionManagingActorRef ! SendMessage(
        createMessage,
        metricsContext,
        attemptNumber = 1,
      )
    }.discard
  }

  override def onClosed(): Unit = {
    implicit val traceContext: TraceContext = TraceContext.empty
    logger.debug(s"Sending Close message to connection managing actor for ref $this")
    connectionManagingActorRef ! Close()
  }
}

object PekkoP2PGrpcNetworking {

  private val SendRetryDelay = 2.seconds
  private val MaxAttempts = 5

  final class PekkoP2PGrpcNetworkManager(
      val connectionManager: P2PGrpcConnectionManager,
      override val timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  ) extends P2PNetworkManager[PekkoModuleSystem.PekkoEnv, BftOrderingMessage]
      with NamedLogging {

    override def createNetworkRef[ActorContextT](
        context: PekkoActorContext[ActorContextT],
        p2pAddress: P2PAddress,
    )(implicit traceContext: TraceContext): P2PNetworkRef[BftOrderingMessage] = {

      val bftNodeIdActorNameComponent =
        p2pAddress.maybeBftNodeId.getOrElse("unknown-bft-node-id")
      val p2pEndpointActorNameComponent = p2pAddress.maybeP2PEndpoint
        .map { p2pEndpoint =>
          val security = if (p2pEndpoint.transportSecurity) "tls" else "plaintext"
          s"${p2pEndpoint.address}-${p2pEndpoint.port}-$security"
        }
        .getOrElse("unknown-p2p-endpoint")

      // The Pekko actor name must be unique within the actor system; for each endpoint and node ID we always have
      //  at most one active P2P gRPC connection-managing actor but network ref consolidation could stop and
      //  re-create one for the same endpoint and node ID, so we ensure unicity by appending a UUID.
      //
      //  An example of that situation follows:
      //
      //  - A is configured with an endpoint to B but B is not configured with an endpoint to A
      //  - A connects and authenticates successfully to B
      //  - B wants to send to A and thus it creates a network ref to A
      //  - The connection crashes and B cleans up the network ref
      //  - A reconnects and authenticates successfully to B
      val actorName =
        s"pekko-p2p-grpc-connection-managing-actor-$p2pEndpointActorNameComponent-$bftNodeIdActorNameComponent-${UUID.randomUUID()}"

      val outstandingMessages = new AtomicInteger()

      logger.debug(s"Spawning P2P gRPC connection-managing actor '$actorName'")
      val result =
        new PekkoP2PNetworkRef(
          context.underlying.spawn(
            createGrpcP2PConnectionManagerPekkoBehavior(
              p2pAddress,
              actorName,
              connectionManager,
              outstandingMessages,
              loggerFactory,
              metrics,
            ),
            actorName,
          ),
          actorName,
          outstandingMessages,
          timeouts,
          loggerFactory,
        )
      logger.debug(s"Spawned P2P gRPC connection-managing actor '$actorName'")
      result
    }

    override def onClosed(): Unit = {
      logger.info("Closing P2P gRPC network manager")(TraceContext.empty)
      connectionManager.close()
    }

    override def shutdownOutgoingConnection(
        p2pEndpointId: P2PEndpoint.Id
    )(implicit traceContext: TraceContext): Unit =
      connectionManager.shutdownConnection(
        Left(p2pEndpointId),
        clearNetworkRefAssociations = true,
        closeNetworkRefs = true,
      )
  }

  private def createGrpcP2PConnectionManagerPekkoBehavior(
      p2pAddress: P2PAddress,
      actorName: String,
      connectionManager: P2PGrpcConnectionManager,
      outstandingMessages: AtomicInteger,
      loggerFactory: NamedLoggerFactory,
      metrics: BftOrderingMetrics,
  ): Behavior[PekkoP2PGrpcConnectionManagerActorMessage] = {
    implicit val traceContext: TraceContext = TraceContext.empty
    val logger = loggerFactory.getTracedLogger(this.getClass)

    // Reschedules an Initialize or Send if the connection is not available yet
    def scheduleMessageIfNotConnectedBehavior(
        message: PekkoP2PGrpcConnectionManagerActorMessage
    )(whenConnected: StreamObserver[BftOrderingMessage] => Unit)(implicit
        context: ActorContext[
          PekkoP2PGrpcConnectionManagerActorMessage
        ]
    ): Unit = {

      def emitModuleQueueStats(): Unit =
        // Emit actor queue latency for the message
        message match {
          case SendMessage(_, metricsContext, _, sendInstant, maybeDelay) =>
            // Emit actor queue metrics
            metrics.performance.orderingStageLatency.emitModuleQueueLatency(
              "PekkoP2PGrpcConnectionManagingActor",
              sendInstant,
              maybeDelay,
            )(metricsContext)
            metrics.performance.orderingStageLatency.emitModuleQueueSize(
              "PekkoP2PGrpcConnectionManagingActor",
              outstandingMessages.decrementAndGet(),
            )(metricsContext)
          case _: Initialize =>
            logger.debug(s"Connection-managing actor $actorName received Initialize")
          case _ =>
        }

      emitModuleQueueStats()

      connectionManager.getPeerSenderOrStartConnection(p2pAddress) match {
        case Some(peerSender) =>
          logger.debug(s"Connection-managing actor $actorName found connection available for Send")
          whenConnected(peerSender)
        case _ =>
          message match {
            case SendMessage(grpcMessage, metricsContext, attemptNumber, _, _) =>
              val newAttemptNumber = attemptNumber + 1
              if (newAttemptNumber <= MaxAttempts) {
                logger.info(
                  s"Connection-managing actor $actorName " +
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
                  s"Connection-managing actor $actorName " +
                    s"couldn't yet obtain connection yet for Send, no more retries left"
                )
            case Initialize() =>
              // Initialize must always be retried, since there are modules that wait for a quorum of
              // connections to be established before being initialized. So if we stopped retrying too soon,
              // some nodes could get stuck, which could easily happen in a network where nodes are starting
              // up simultaneously and are not immediately reachable to one another.
              logger.info(
                s"Connection-managing actor $actorName " +
                  s"couldn't yet obtain connection for Initialize, retrying it in $SendRetryDelay"
              )
              context.scheduleOnce(SendRetryDelay, target = context.self, message).discard
              metrics.p2p.send.sendsRetried.inc()(MetricsContext.Empty)
            case Close() =>
              abort(
                logger,
                s"Connection-managing actor $actorName is unexpectedly processing " +
                  "Close messages with retries",
              )
          }
      }
    }

    def closeBehavior(): Behavior[PekkoP2PGrpcConnectionManagerActorMessage] = {
      logger.info(s"Closing connection-managing actor $actorName")
      connectionManager.shutdownConnection(
        p2pAddress.id,
        clearNetworkRefAssociations = true,
        closeNetworkRefs = false, // Because we're already closing the actor
      )
      Behaviors.stopped
    }

    Behaviors.setup { implicit pekkoActorContext =>
      Behaviors
        .receiveMessage[PekkoP2PGrpcConnectionManagerActorMessage] {
          case initMsg: Initialize =>
            logger.info(s"Connection-managing actor $actorName initializing")
            scheduleMessageIfNotConnectedBehavior(initMsg)(_ => ())
            Behaviors.same
          case sendMsg: SendMessage =>
            scheduleMessageIfNotConnectedBehavior(sendMsg) { peerSender =>
              val now = Instant.now
              val msg = sendMsg.createMessage(Some(now))
              try {
                logger.debug(
                  s"Connection-managing actor $actorName sending message to sender $peerSender"
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
                    s"Connection-managing actor $actorName failed sending message $msg to sender $peerSender",
                    exception,
                  )
                  // Failing the stream in case of an exception when sending is required by the gRPC streaming API
                  failGrpcStreamObserver(peerSender, exception, logger)
                  // gRPC requires onError to be the last event, so the connection must be invalidated even though
                  //  the send operation will be retried.
                  connectionManager.shutdownConnection(
                    p2pAddress.id,
                    clearNetworkRefAssociations = false,
                    closeNetworkRefs = false,
                  )
                  // Retrying after a delay due to an exception:
                  //  record the send instant and delay to emit the actor queue latency when processing the message
                  val newAttemptNumber = sendMsg.attemptNumber + 1
                  if (newAttemptNumber <= MaxAttempts) {
                    logger.info(
                      s"Connection-managing actor $actorName couldn't send a message to sender $peerSender, " +
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
                      s"Connection-managing actor $actorName couldn't send $msg, " +
                        s"invalidating the connection. No more retries left.",
                      exception,
                    )
              }
            }
            Behaviors.same
          case Close() =>
            logger.info(
              s"Connection-managing actor $actorName is stopping, closing"
            )
            closeBehavior()
        }
        .receiveSignal { case (_, PostStop) =>
          logger.info(s"Connection-managing actor $actorName stopped, closing")
          closeBehavior()
        }
    }
  }
}
