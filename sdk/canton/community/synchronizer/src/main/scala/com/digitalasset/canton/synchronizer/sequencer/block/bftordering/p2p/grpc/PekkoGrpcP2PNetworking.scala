// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.p2p.grpc

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.pekko.PekkoModuleSystem
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ClientP2PNetworkManager,
  ModuleRef,
  P2PNetworkRef,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v1.BftOrderingServiceReceiveResponse
import com.digitalasset.canton.topology.SequencerId
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
      getServerEndpointOrStartConnection: Endpoint => Option[
        (SequencerId, StreamObserver[P2PMessageT])
      ],
      closeConnection: Endpoint => Unit,
      timeouts: ProcessingTimeout,
      override val loggerFactory: NamedLoggerFactory,
  ) extends ClientP2PNetworkManager[PekkoModuleSystem.PekkoEnv, P2PMessageT]
      with NamedLogging {

    override def createNetworkRef[ActorContextT](
        context: PekkoActorContext[ActorContextT],
        peer: Endpoint,
    )(
        onSequencerId: (Endpoint, SequencerId) => Unit
    ): P2PNetworkRef[P2PMessageT] = {
      val actorName =
        s"peer-${peer.host}-${peer.port}-client-connection" // The actor name must be unique.
      logger.debug(
        s"created client connection-managing actor '$actorName'"
      )(TraceContext.empty)
      new PekkoP2PNetworkRef(
        context.underlying.spawn(
          createGrpcConnectionManagerPekkoBehavior(
            peer,
            getServerEndpointOrStartConnection,
            closeConnection,
            onSequencerId,
            loggerFactory,
          ),
          actorName,
        ),
        timeouts,
        loggerFactory,
      )
    }
  }

  def tryCreateServerEndpoint[P2PMessageT](
      sequencerId: SequencerId,
      inputModule: ModuleRef[P2PMessageT],
      clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse],
      cleanupClientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse] => Unit,
      loggerFactory: NamedLoggerFactory,
  ): StreamObserver[P2PMessageT] =
    Try(
      clientEndpoint.onNext(
        BftOrderingServiceReceiveResponse.of(sequencerId.uid.toProtoPrimitive)
      )
    ) match {
      case Failure(exception) =>
        clientEndpoint.onError(exception) // Required by the gRPC streaming API
        throw exception // gRPC requires onError to be the last event, so it doesn't make sense to return a handler
      case Success(_) =>
        new GrpcServerEndpoint[P2PMessageT](
          inputModule,
          clientEndpoint,
          cleanupClientEndpoint,
          loggerFactory,
        )
    }

  private def createGrpcConnectionManagerPekkoBehavior[P2PMessageT](
      peer: Endpoint,
      getServerPeerEndpoint: Endpoint => Option[(SequencerId, StreamObserver[P2PMessageT])],
      closeConnection: Endpoint => Unit,
      onSequencerId: (Endpoint, SequencerId) => Unit,
      loggerFactory: NamedLoggerFactory,
  ): Behavior[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] = {
    val logger = loggerFactory.getLogger(this.getClass)

    def scheduleMessageIfNotConnectedBehavior(
        message: => PekkoGrpcConnectionManagerActorMessage[P2PMessageT]
    )(whenConnected: StreamObserver[P2PMessageT] => Unit)(implicit
        context: ActorContext[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]]
    ): Behavior[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] = {
      getServerPeerEndpoint(peer) match {
        case Some((sequencerId, serverPeerEndpoint)) =>
          // Connection available
          onSequencerId(peer, sequencerId)
          whenConnected(serverPeerEndpoint)
        case _ =>
          logger.info(
            s"Connection-managing actor for peer in server role $peer " +
              s"couldn't obtain connection yet, retrying in $SendRetryDelay"
          )
          val _ = context.scheduleOnce(SendRetryDelay, target = context.self, message)
      }
      Behaviors.same
    }

    def closeBehavior(): Behavior[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] = {
      logger.info(s"Closing connection-managing actor for peer in server role $peer")
      closeConnection(peer)
      Behaviors.stopped
    }

    Behaviors.setup { implicit context =>
      Behaviors
        .receiveMessage[PekkoGrpcConnectionManagerActorMessage[P2PMessageT]] {
          case initMsg: Initialize[P2PMessageT] =>
            logger.info(s"Starting connection to peer $peer")
            scheduleMessageIfNotConnectedBehavior(initMsg)(_ => ())
          case sendMsg: SendMessage[P2PMessageT] =>
            scheduleMessageIfNotConnectedBehavior(sendMsg) { serverPeerEndpoint =>
              try {
                serverPeerEndpoint.onNext(sendMsg.grpcMessage)
                sendMsg.onCompletion()
              } catch {
                case exception: Exception =>
                  logger.info(
                    s"Connection-managing actor for peer in server role $peer couldn't send ${sendMsg.grpcMessage}, " +
                      s"invalidating the connection and retrying in $SendRetryDelay",
                    exception,
                  )
                  serverPeerEndpoint.onError(exception) // Required by the gRPC streaming API
                  // gRPC requires onError to be the last event, so the connection must be invalidated even though
                  //  the send will be retried.
                  closeConnection(peer)
                  val _ = context.scheduleOnce(SendRetryDelay, target = context.self, sendMsg)
              }
            }
          case Close() =>
            logger.info(
              s"Connection-managing actor for peer in server role $peer is stopping, closing"
            )
            closeBehavior()
        }
        .receiveSignal { case (_, PostStop) =>
          logger.info(s"Connection-managing actor for peer in server role $peer stopped, closing")
          closeBehavior()
        }
    }
  }
}
