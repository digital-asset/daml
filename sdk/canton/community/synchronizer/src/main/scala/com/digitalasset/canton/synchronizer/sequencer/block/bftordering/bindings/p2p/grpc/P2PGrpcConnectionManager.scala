// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import cats.data.OptionT
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder.createChannelBuilder
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth
import com.digitalasset.canton.synchronizer.metrics.BftOrderingMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  AuthenticationInitialState,
  P2PEndpoint,
  completeGrpcStreamObserver,
  failGrpcStreamObserver,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication.{
  AddEndpointHeaderClientInterceptor,
  AuthenticateServerClientInterceptor,
  ServerAuthenticatingServerInterceptor,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.P2PConnectionManagementConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PConnectionState.Error
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.p2p.P2PMetrics.emitIdentityEquivocation
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
  P2PAddress,
  P2PConnectionEventListener,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.{
  abort,
  objId,
}
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessage,
  BftOrderingMessageBody,
  BftOrderingServiceGrpc,
  ConnectionOpened,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{AtomicUtil, DelayUtil}
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.{AbstractStub, StreamObserver}
import io.grpc.{Channel, ClientInterceptors, ManagedChannel}

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ExecutorService, ThreadLocalRandom}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, blocking}
import scala.jdk.CollectionConverters.*
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.{Failure, Success, Try}

private[bftordering] final class P2PGrpcConnectionManager(
    thisNode: BftNodeId,
    p2pConnectionManagementConfig: P2PConnectionManagementConfig,
    p2pGrpcConnectionState: P2PGrpcConnectionState, // Owns it and closes it
    // None if authentication is disabled
    authenticationInitialState: Option[AuthenticationInitialState],
    serverToClientAuthenticationEndpoint: Option[P2PEndpoint],
    p2pConnectionEventListener: P2PConnectionEventListener,
    p2pNetworkIn: ModuleRef[BftOrderingMessage],
    metrics: BftOrderingMetrics,
    longRunningExecutor: ExecutorService,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContextExecutor, metricsContext: MetricsContext)
    extends NamedLogging
    with FlagCloseableAsync { self =>

  import P2PGrpcConnectionManager.*

  private val random = ThreadLocalRandom.current()

  private val isAuthenticationEnabled: Boolean = authenticationInitialState.isDefined

  private val longRunningExecutionContext = ExecutionContext.fromExecutor(longRunningExecutor)

  private val p2pOutgoingConnectionsStatusRef =
    new AtomicReference[UnlessShutdown[Map[P2PEndpoint.Id, P2POutgoingConnectionStatus]]](
      UnlessShutdown.Outcome(Map.empty)
    )

  // Called by the connection-managing actor when establishing a connection to an endpoint
  def getPeerSenderOrStartConnection(
      p2pAddress: P2PAddress
  )(implicit traceContext: TraceContext): Option[StreamObserver[BftOrderingMessage]] =
    if (!isClosing) {
      p2pGrpcConnectionState.associateP2PEndpointIdToBftNodeId(p2pAddress).toOption.flatMap { _ =>
        val maybeP2PEndpoint = p2pAddress.maybeP2PEndpoint
        p2pGrpcConnectionState.getSender(p2pAddress.id) match {
          case found @ Some(_) =>
            // A sender may be present due to an incoming connection, so we need to stop trying to connect
            maybeP2PEndpoint
              .map(_.id)
              .foreach { p2pEndpointId =>
                logger.debug(
                  s"Found existing sender for $p2pEndpointId " +
                    "(from either fully connected incoming or outgoing connection), " +
                    "checking and shutting down any in-progress outgoing connection potentially started earlier"
                )
                shutdownOutgoingConnectionIfNeeded(
                  p2pEndpointId,
                  onlyIfNotFullyConnected = true,
                ).discard
              }
            found
          case _ =>
            logger.debug(
              s"Requested a send but no sender found for $p2pAddress, " +
                "ensuring an outgoing connection is established or being established asynchronously"
            )
            maybeP2PEndpoint.foreach(connectIfNeeded(_).discard)
            None
        }
      }
    } else {
      logger.debug(
        s"P2P gRPC connection manager not providing a sender for $p2pAddress due to shutdown"
      )
      None
    }

  // Called by the network ref factory on behalf of the P2P network out module when it
  //  receives a disconnect admin command, or it detects identity equivocation.
  def shutdownConnection(
      p2pEndpointId: P2PEndpoint.Id
  )(implicit traceContext: TraceContext): Unit =
    shutdownConnection(
      Left(p2pEndpointId),
      clearNetworkRefAssociations = true,
      closeNetworkRefs = true,
    )

  // Called by either:
  // - `close()`; in this case, the association between the endpoint(s) and the network ref must be removed and the
  //   connection-managing actor closed.
  // - The network ref factory on behalf of the P2P network out module when it
  //   receives a disconnect admin command, or it detects identity equivocation;
  //   in this case, the association between the endpoint(s) and the network ref must be removed and the
  //   connection-managing actor closed.
  // - The connection-managing actor, whenever it fails to send a message to a node;
  //   in this case nothing is modified nor closed, as the connection will be re-established.
  def shutdownConnection(
      p2pAddressId: P2PAddress.Id,
      clearNetworkRefAssociations: Boolean,
      closeNetworkRefs: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Shutting down asynchronously any incoming or outgoing connection to $p2pAddressId"
    )
    val maybeP2PEndpointId = p2pAddressId.left.toOption
    maybeP2PEndpointId.foreach(
      shutdownOutgoingConnectionIfNeeded(_, onlyIfNotFullyConnected = false).discard
    )
    p2pGrpcConnectionState
      .shutdownConnectionAndReturnPeerSender(
        p2pAddressId,
        clearNetworkRefAssociations,
        closeNetworkRefs,
      )
      .foreach { peerSender =>
        completeGrpcStreamObserver(peerSender, logger)
        maybeP2PEndpointId.foreach(notifyEndpointDisconnection)
      }
  }

  // Called by the peer receiver of an outgoing connection on error and on completion,
  //  which also occurs in case of duplicate connection.
  //  No network ref associations must be changed and no network ref must be closed,
  //  as the connection will be re-established.
  private def shutdownOutgoingConnectionDueToRemoteCompletion(
      peerEndpointId: P2PEndpoint.Id,
      peerSender: StreamObserver[BftOrderingMessage],
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Shutting down asynchronously any (active or duplicate) outgoing connection to $peerEndpointId " +
        "due to remote completion"
    )
    shutdownOutgoingConnectionIfNeeded(peerEndpointId, onlyIfNotFullyConnected = false).discard
    cleanupPeerSender(peerSender)
  }

  // Called by the peer receiver of an incoming connection on error and on completion,
  //  which also occurs in case of duplicate connection.
  //  No network ref associations must be changed and no network ref must be closed,
  //  as the connection will be re-established.
  private def shutdownIncomingConnectionDueToRemoteCompletion(
      peerSender: StreamObserver[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Shutting down (active or duplicate) incoming connection with peer sender ${objId(peerSender)}"
    )
    cleanupPeerSender(peerSender)
  }

  private def cleanupPeerSender(
      peerSender: StreamObserver[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Unit = {
    completeGrpcStreamObserver(peerSender, logger)
    p2pGrpcConnectionState
      .unassociateSenderAndReturnEndpointIds(peerSender)
      .foreach(notifyEndpointDisconnection)
  }

  private def notifyEndpointDisconnection(peerEndpointId: P2PEndpoint.Id)(implicit
      traceContext: TraceContext
  ): Unit =
    p2pConnectionEventListener.onDisconnect(peerEndpointId)

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    AsyncCloseable(
      "bft-ordering-grpc-networking-connection-manager-connection-state-close",
      closeConnectionState(),
      timeouts.closing,
    ) +:
      (p2pOutgoingConnectionsStatusRef
        .getAndUpdate(_ => UnlessShutdown.AbortedDueToShutdown) match {
        case UnlessShutdown.Outcome(state) =>
          state
            .map { case (p2pEndpointId, outgoingConnectionStatus) =>
              p2pEndpointId -> outgoingConnectionStatus.channelO
                .map { case (channel, authenticationContextO) =>
                  shutdownGrpcChannelIfNeeded(p2pEndpointId, channel, authenticationContextO)
                }
                .getOrElse(FutureUnlessShutdown.unit)
            }
            .map { case (p2pEndpointId, closer) =>
              AsyncCloseable(
                s"bft-ordering-grpc-networking-connection-manager-connection-$p2pEndpointId",
                closer.unwrap,
                timeouts.closing,
              )
            }
            .toSeq
        case UnlessShutdown.AbortedDueToShutdown => Seq.empty
      })
  }

  private def closeConnectionState()(implicit traceContext: TraceContext): Future[Unit] =
    Future
      .sequence(p2pGrpcConnectionState.connections.map {
        case (maybeP2PEndpointId, maybeBftNodeId) =>
          Future(
            maybeP2PAddressId(maybeP2PEndpointId, maybeBftNodeId)
              .foreach(
                shutdownConnection(
                  _,
                  clearNetworkRefAssociations = true,
                  closeNetworkRefs = true,
                )
              )
          )
      })
      .map(_ => ())

  private def maybeP2PAddressId(
      maybeP2PEndpointId: Option[P2PEndpoint.Id],
      maybeBftNodeId: Option[BftNodeId],
  ): Option[P2PAddress.Id] =
    maybeBftNodeId
      .map(Right(_))
      .orElse(maybeP2PEndpointId.map(Left(_)))

  private def connectIfNeeded(
      p2pEndpoint: P2PEndpoint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val p2pEndpointId = p2pEndpoint.id
    logger.debug(s"Ensuring connection to $p2pEndpointId")
    val startConnection =
      attemptTransitionToConnecting(p2pEndpointId)
        .logAndExtract(prefix =
          s"State transition for $p2pEndpointId when attempting to start outgoing connection: "
        )
    if (startConnection) {
      logger.debug(s"Starting connection for $p2pEndpointId")
      connect(p2pEndpoint)
    } else {
      logger.debug(s"Connection for $p2pEndpointId is already in progress or established")
      FutureUnlessShutdown.unit
    }
  }

  private def connect(
      p2pEndpoint: P2PEndpoint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val p2pEndpointId = p2pEndpoint.id
    if (!isClosing) {
      openGrpcChannel(p2pEndpoint)
        .flatMap {
          case Some(
                OpenChannel(
                  ch,
                  acO,
                  maybeSequencerIdFromAuthenticationPromiseUS,
                  asyncStub,
                )
              ) =>
            logger.info(
              s"Created a gRPC channel ${objId(ch)} to $p2pEndpointId, starting a connect worker"
            )

            p2pConnectionEventListener.onConnect(p2pEndpointId)
            val sequencerIdUS =
              maybeSequencerIdFromAuthenticationPromiseUS.getOrElse(
                // Authentication is disabled, the peer receiver will use the first message's sentBy to backfill
                PromiseUnlessShutdown.unsupervised[SequencerId]()
              )
            // Add the connection to the state asynchronously as soon as a sequencer ID is available
            val connectWorker =
              addPeerEndpointForOutgoingConnectionOnAuthenticationCompletion(
                p2pEndpoint,
                ch,
                acO,
                asyncStub,
                sequencerIdUS,
              )
            attemptTransitionToConnectingWithChannelAndWorker(
              p2pEndpointId,
              ch,
              connectWorker,
            ).logAndExtract(prefix =
              s"State transition for $p2pEndpointId when attempting to connect over gRPC channel with worker: "
            )
            connectWorker

          case None =>
            FutureUnlessShutdown.unit
        }
    } else {
      logger.info(
        s"Not attempting to create a gRPC channel to connect to $p2pEndpointId due to shutdown"
      )
      FutureUnlessShutdown.unit
    }
  }

  private def addPeerEndpointForOutgoingConnectionOnAuthenticationCompletion(
      p2pEndpoint: P2PEndpoint,
      channel: ManagedChannel,
      authenticationContextO: Option[GrpcSequencerClientAuth],
      asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
      sequencerIdUS: PromiseUnlessShutdown[SequencerId],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val p2pEndpointId = p2pEndpoint.id
    val peerSenderOT =
      createPeerSender(p2pEndpoint, channel, authenticationContextO, asyncStub, sequencerIdUS)
    sequencerIdUS.futureUS
      .flatMap { sequencerId =>
        toUnitFutureUS(
          peerSenderOT
            .map { peerSender =>
              logger.info(
                s"P2P endpoint $p2pEndpointId successfully connected and authenticated " +
                  s"as ${sequencerId.toProtoPrimitive}"
              )
              tryAddPeerEndpoint(
                sequencerId,
                peerSender,
                Some(p2pEndpoint),
              )
            }
        )
      }
      .transformWith {
        case f @ Failure(exception) =>
          logger.info(
            s"Failed adding the P2P endpoint $p2pEndpointId, shutting down the gRPC channel",
            exception,
          )
          val doShutdownChannel =
            attemptTransitionToDisconnectedAfterConnectWorkerFailed(p2pEndpointId, channel)
              .logAndExtract(s"State transition for $p2pEndpointId after connect worker failure: ")
          if (doShutdownChannel) {
            shutdownGrpcChannelIfNeeded(p2pEndpointId, channel, authenticationContextO)
          } else {
            logger.debug(s"No gRPC channel to shutdown for $p2pEndpointId")
            FutureUnlessShutdown.unit
          }
        case s: Success[?] => FutureUnlessShutdown.unit
      }
  }

  private def tryAddPeerEndpoint(
      sequencerId: SequencerId,
      peerSender: StreamObserver[BftOrderingMessage],
      // It may be None if the peer is connecting to us and did not communicate its endpoint
      maybeP2PEndpoint: Option[P2PEndpoint],
  )(implicit traceContext: TraceContext): Unit = {
    val bftNodeId = SequencerNodeId.toBftNodeId(sequencerId)
    val maybeP2PEndpointId = maybeP2PEndpoint.map(_.id)
    logger.info(
      s"Adding peer endpoint $maybeP2PEndpointId for $bftNodeId with peer sender ${objId(peerSender)}"
    )
    maybeP2PEndpointId.map(
      p2pGrpcConnectionState.associateP2PEndpointIdToBftNodeId(_, bftNodeId)
    ) match {
      case None | Some(Right(())) =>
        if (p2pGrpcConnectionState.addSenderIfMissing(bftNodeId, peerSender)) {
          p2pConnectionEventListener.onSequencerId(bftNodeId, maybeP2PEndpoint)
        } else {
          logger.info(
            s"Completing peer sender ${objId(peerSender)} for $bftNodeId <-> $maybeP2PEndpointId " +
              "because one already exists"
          )
          completeGrpcStreamObserver(peerSender, logger)
        }
      case Some(Left(error)) =>
        error match {
          case Error.CannotAssociateP2PEndpointIdsToSelf(p2pEndpointId, thisBftNodeId) =>
            emitIdentityEquivocation(metrics, p2pEndpointId, thisBftNodeId)
          case Error.P2PEndpointIdAlreadyAssociated(
                p2pEndpointId,
                _,
                newBftNodeId,
              ) =>
            emitIdentityEquivocation(metrics, p2pEndpointId, newBftNodeId)
        }
        logger.warn(
          s"Detected identity equivocation when adding peer endpoint $maybeP2PEndpointId for $bftNodeId, " +
            s"failing the peer sender ${objId(peerSender)}"
        )
        throw new RuntimeException(error.toString)
    }
  }

  private def openGrpcChannel(
      p2pEndpoint: P2PEndpoint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Option[OpenChannel]] = {
    val p2pEndpointId = p2pEndpoint.id

    logger.info(s"Creating a gRPC channel to $p2pEndpointId")

    val channel = createChannelBuilder(p2pEndpoint.endpointConfig).build()
    val channelId = objId(channel)

    val authenticationContextO =
      authenticationInitialState.map(auth =>
        new GrpcSequencerClientAuth(
          auth.psId,
          member = auth.sequencerId,
          crypto = auth.authenticationServices.syncCryptoForAuthentication.crypto,
          channelPerEndpoint =
            NonEmpty(Map, Endpoint(p2pEndpoint.address, p2pEndpoint.port) -> channel),
          supportedProtocolVersions = Seq(auth.psId.protocolVersion),
          tokenManagerConfig = auth.authTokenConfig,
          metricsO = None,
          metricsContext = MetricsContext.Empty,
          clock = auth.clock,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        )
      )
    val authenticationContextId = authenticationContextO.map(objId)

    val success =
      attemptTransitionToConnectingWithChannel(p2pEndpointId, channel, authenticationContextO)
        .logAndExtract(prefix =
          s"State transition for $p2pEndpointId when attempting to start outgoing connection after creating a channel: "
        )

    if (success) {
      logger.info(
        s"Created gRPC channel $channelId with authentication context $authenticationContextId to $p2pEndpointId"
      )

      // When authentication is enabled, the external address normally also
      //  appears as peer endpoint for this peer in other peers' configurations, so
      //  if the connecting peer always sends it (i.e., even when authentication is disabled),
      //  the server peer can use it to deduplicate connections:
      //
      // - If the server already connected to the peer with that endpoint or there is
      //   already an incoming connection from that peer, it will close the duplicate connection.
      // - Else it will associate the incoming connection with the endpoint and reuse it
      //   instead of creating a new outgoing connection to the same peer.
      def stubWithEndpointHeaderClientInterceptor[S <: AbstractStub[S]](
          stub: S,
          endpoint: P2PEndpoint,
      ) =
        stub.withInterceptors(
          new AddEndpointHeaderClientInterceptor(
            endpoint,
            loggerFactory,
          )
        )

      def maybeAuthenticateStub[S <: AbstractStub[S]](stub: S) =
        serverToClientAuthenticationEndpoint.fold(stub) { endpoint =>
          val augmentedStub = stubWithEndpointHeaderClientInterceptor(stub, endpoint)
          authenticationContextO.fold(augmentedStub)(_.apply(augmentedStub))
        }

      val (potentiallyCheckedChannel, maybeSequencerIdFromAuthenticationPromiseUS) =
        maybeApplyServerAuthenticatingClientInterceptor(channel)

      FutureUnlessShutdown.pure(
        Some(
          OpenChannel(
            channel,
            authenticationContextO,
            maybeSequencerIdFromAuthenticationPromiseUS,
            maybeAuthenticateStub(BftOrderingServiceGrpc.stub(potentiallyCheckedChannel)),
          )
        )
      )
    } else {
      logger.info(
        s"Shutting gRPC channel $channelId with authentication context $authenticationContextId " +
          s"to connect to $p2pEndpointId due to connection status having moved away from 'Connecting'"
      )
      shutdownGrpcChannelIfNeeded(p2pEndpointId, channel, authenticationContextO).map(_ =>
        Option.empty[OpenChannel]
      )
    }
  }

  // Returns a tuple of the channel, with the server-authenticating gRPC client interceptor applied
  //  if authentication is enabled, and in that case also a promise that is completed with the sequencer ID
  //  by the gRPC client interceptor before the call is activated.
  //  NOTE: we could research again a less convoluted mechanism to retrieve the sequencer ID from the gRPC client
  //  authentication interceptor; unfortunately, the gRPC context seems to be a server-only mechanism.
  private def maybeApplyServerAuthenticatingClientInterceptor(
      channel: Channel
  ): (Channel, Option[PromiseUnlessShutdown[SequencerId]]) =
    authenticationInitialState
      .fold[(Channel, Option[PromiseUnlessShutdown[SequencerId]])](channel -> None) { auth =>
        val memberAuthenticationService = auth.authenticationServices.memberAuthenticationService
        val sequencerIdFromAuthenticationPromiseUS =
          PromiseUnlessShutdown.unsupervised[SequencerId]()
        val interceptor =
          new AuthenticateServerClientInterceptor(
            memberAuthenticationService,
            onAuthenticationSuccess = sequencerId =>
              if (!sequencerIdFromAuthenticationPromiseUS.isCompleted)
                sequencerIdFromAuthenticationPromiseUS.outcome_(sequencerId),
            onAuthenticationFailure = throwable =>
              if (!sequencerIdFromAuthenticationPromiseUS.isCompleted)
                sequencerIdFromAuthenticationPromiseUS.failure(throwable),
            loggerFactory,
          )
        ClientInterceptors.intercept(channel, List(interceptor).asJava) -> Some(
          sequencerIdFromAuthenticationPromiseUS
        )
      }

  @SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
  private def createPeerSender(
      p2pEndpoint: P2PEndpoint,
      channel: ManagedChannel,
      authenticationContextO: Option[GrpcSequencerClientAuth],
      asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
      sequencerIdPromiseUS: PromiseUnlessShutdown[SequencerId],
      connectRetryDelay: NonNegativeFiniteDuration =
        p2pConnectionManagementConfig.initialConnectionRetryDelay.toInternal,
      attemptNumber: Int = 1,
  )(implicit traceContext: TraceContext): OptionT[
    FutureUnlessShutdown,
    StreamObserver[BftOrderingMessage],
  ] =
    synchronizeWithClosing("p2p-create-peer-sender") {
      val channelId = objId(channel)
      val authenticationContextId = authenticationContextO.map(objId)
      val p2pEndpointId = p2pEndpoint.id
      val logPrefix = s"[Connect worker for channel $channelId w/authctx $authenticationContextId]:"

      def retry(
          failedOperationName: String,
          exception: Throwable,
          previousRetryDelay: NonNegativeFiniteDuration,
          attemptNumber: Int,
      ): OptionT[
        FutureUnlessShutdown,
        StreamObserver[BftOrderingMessage],
      ] = {

        def logFailure(msg: => String, exc: Throwable): Unit =
          if (
            attemptNumber <= p2pConnectionManagementConfig.maxConnectionAttemptsBeforeWarning.value
          )
            logger.info(s"$logPrefix $msg", exc)
          else
            logger.warn(s"$logPrefix $msg", exc)

        val retryDelayBase =
          p2pConnectionManagementConfig.maxConnectionRetryDelay.toInternal
            .min(
              previousRetryDelay * p2pConnectionManagementConfig.connectionRetryDelayMultiplier
            )
            .toConfig
            .underlying
        val retryDelayBaseLength = retryDelayBase.length
        val jitteredRetryDelay =
          NonNegativeFiniteDuration
            .tryCreate(
              Duration(
                retryDelayBaseLength / 2 + random.nextLong(0, retryDelayBaseLength / 2),
                retryDelayBase.unit,
              ).toJava
            )
        if (!isClosing) {
          logFailure(
            s"failed to $failedOperationName during connection attempt $attemptNumber, " +
              s"retrying in $jitteredRetryDelay",
            exception,
          )
          for {
            _ <-
              // Wait for the retry delay
              OptionT[FutureUnlessShutdown, Unit](
                DelayUtil
                  .delayIfNotClosing("grpc-networking", jitteredRetryDelay.toScala, self)
                  .map(Some(_))
              )
            success =
              attemptTransitionToRetryConnecting(p2pEndpointId, channel)
                .logAndExtract(
                  s"$logPrefix State transition for $p2pEndpointId when attempting to retry outgoing connection:"
                )
            result <-
              if (success) {
                logger.info(
                  s"$logPrefix failed to $failedOperationName during connection attempt $attemptNumber, retrying"
                )
                createPeerSender(
                  p2pEndpoint,
                  channel,
                  authenticationContextO,
                  asyncStub,
                  sequencerIdPromiseUS,
                  jitteredRetryDelay,
                  attemptNumber,
                ) // Async-trampolined recursive retry
              } else {
                logger.info(
                  s"$logPrefix failed to $failedOperationName during attempt $attemptNumber, " +
                    "but not retrying and shutting down the gRPC channel",
                  exception,
                )
                OptionT(
                  shutdownGrpcChannelIfNeeded(p2pEndpointId, channel, authenticationContextO).map(
                    _ => Option.empty[StreamObserver[BftOrderingMessage]]
                  )
                )
              }
          } yield result
        } else {
          logger.info(
            s"$logPrefix failed to $failedOperationName during connection attempt $attemptNumber " +
              s"but not retrying due to shutdown",
            exception,
          )
          OptionT(
            shutdownGrpcChannelIfNeeded(p2pEndpointId, channel, authenticationContextO).map(_ =>
              Option.empty[StreamObserver[BftOrderingMessage]]
            )
          )
        }
      }

      // The gRPC streaming API needs to be passed a receiver and returns the sender,
      //  but we need the receiver to have a reference to the sender so that, when the receiver
      //  is shut down, it can correctly look up and clean up the associated connection state
      //  (which is looked up by sender, as it is uniquely associated to the node ID and thus the peer).
      //  So we create the receiver first, then the sender, and finally we provide the sender to
      //  the receiver via a promise that is guaranteed to complete or fail fast, and the
      //  shutdown logic in the receiver awaits on that promise before cleaning up.
      val peerSenderPromiseUS =
        PromiseUnlessShutdown.unsupervised[StreamObserver[BftOrderingMessage]]()
      logger.info(
        s"$logPrefix Creating a P2P gRPC stream receiver for new outgoing connection to $p2pEndpointId"
      )
      val peerReceiver =
        new P2PGrpcStreamingReceiver(
          Some(p2pEndpointId),
          p2pNetworkIn,
          sequencerIdPromiseUS,
          isAuthenticationEnabled,
          metrics,
          loggerFactory,
        ) {
          override def shutdown(): Unit =
            // Cleanup the outgoing connection by looking up the state through the unique peer sender
            //  as soon as it is available
            peerSenderPromiseUS.futureUS
              .transform(
                _.map(shutdownOutgoingConnectionDueToRemoteCompletion(p2pEndpointId, _)),
                identity,
              )
              .discard
        }

      val initialConnectionMaxDelay =
        p2pConnectionManagementConfig.initialConnectionMaxDelay.underlying
      val jitteredConnectDelay =
        Duration(
          random.nextLong(0, initialConnectionMaxDelay.length),
          initialConnectionMaxDelay.unit,
        )
      logger.info(
        s"$logPrefix Trying to create a stream $p2pEndpointId in $jitteredConnectDelay"
      )
      for {
        _ <- OptionT[FutureUnlessShutdown, Unit](
          DelayUtil
            .delayIfNotClosing(
              "grpc-networking",
              jitteredConnectDelay,
              self,
            )
            .map(Some(_))
        ) // Wait for the retry delay
        result <-
          // Try to connect
          Try(asyncStub.receive(peerReceiver)) match {

            case Failure(exception) =>
              // No need to complete the peer sender promise nor fail the receiver, as the receiver wasn't installed
              retry(
                failedOperationName =
                  s"create a stream to $p2pEndpointId over gRPC channel $channel",
                exception,
                connectRetryDelay,
                attemptNumber + 1,
              )

            case Success(peerSender) =>
              // Complete the peer sender promise
              peerSenderPromiseUS.outcome_(peerSender)
              logger.info(
                s"$logPrefix Stream to $p2pEndpointId created successfully, " +
                  "sending connection opener to preemptively check the connection " +
                  "and provide the sequencer ID (if needed)"
              )

              Try(peerSender.onNext(createConnectionOpener(thisNode))) match {

                case Failure(exception) =>
                  // Close the connection by failing the sender; no need to close the receiver as it will be
                  //  uninstalled by closing the connection and no state has been updated yet.
                  failGrpcStreamObserver(peerSender, exception, logger)
                  retry(
                    failedOperationName =
                      s"send connection opener for $p2pEndpointId over gRPC channel $channel",
                    exception,
                    connectRetryDelay,
                    attemptNumber + 1,
                  )

                case Success(_) =>
                  logger.info(
                    s"$logPrefix Sending connection opener to $p2pEndpointId succeeded, " +
                      "waiting for authentication"
                  )

                  // Retry also if the sequencer ID couldn't be retrieved, and we're not shutting down
                  val sequencerIdFUS = sequencerIdPromiseUS.futureUS
                  OptionT(
                    sequencerIdFUS.transformWith {

                      case Success(sequencerIdUS) =>
                        sequencerIdUS match {

                          case UnlessShutdown.Outcome(sequencerId) =>
                            logger.info(
                              s"$logPrefix P2P endpoint $p2pEndpointId " +
                                s"successfully authenticated as ${sequencerId.toProtoPrimitive}"
                            )
                            val channelToShutdownO =
                              attemptConnectionOrDisconnectionCompletion(
                                p2pEndpointId,
                                channel,
                                authenticationContextO,
                              )
                                .logAndExtract(prefix =
                                  s"$logPrefix State transition for $p2pEndpointId when attempting to complete " +
                                    "outgoing connection (or its disconnection, if requested): "
                                )
                            channelToShutdownO.fold {
                              logger.info(
                                s"$logPrefix Connection to $p2pEndpointId successful, connect worker is ending"
                              )
                              FutureUnlessShutdown.pure(Option(peerSender))
                            } { case (channel, authenticationContextO) =>
                              logger.info(
                                s"$logPrefix Connection to $p2pEndpointId just established needs to be closed, " +
                                  "closing the sender and shutting down the gRPC channel"
                              )
                              completeGrpcStreamObserver(peerSender, logger)
                              shutdownGrpcChannelIfNeeded(
                                p2pEndpointId,
                                channel,
                                authenticationContextO,
                              ).map(_ => None)
                            }

                          case UnlessShutdown.AbortedDueToShutdown =>
                            logger.info(
                              s"$logPrefix Connection to $p2pEndpointId aborted due to shutdown"
                            )
                            transitionToDisconnected(p2pEndpointId, onlyIfNotConnected = false)
                              .logAndExtract(prefix =
                                s"$logPrefix State transition when shutting down outgoing connection " +
                                  s"for $p2pEndpointId due to shutdown"
                              ) match {
                              case Left(_) =>
                                // The future is either unit or this very worker, so no need to wait for it, just terminate
                                FutureUnlessShutdown.pure(None)
                              case Right(channel -> authenticationContextO) =>
                                logger.debug(
                                  s"$logPrefix Closing the sender and " +
                                    s"shutting down the gRPC channel $channel to $p2pEndpointId"
                                )
                                completeGrpcStreamObserver(peerSender, logger)
                                shutdownGrpcChannelIfNeeded(
                                  p2pEndpointId,
                                  channel,
                                  authenticationContextO,
                                ).map(_ => None)
                            }
                        }

                      case Failure(exception) =>
                        logger.info(
                          s"$logPrefix P2P endpoint $p2pEndpointId authentication failed, " +
                            s"notifying an error to the sender"
                        )
                        // Close the connection by failing the sender; no need to close the receiver as it will be
                        //  uninstalled by closing the connection and no state has been updated yet.
                        failGrpcStreamObserver(peerSender, exception, logger)
                        retry(
                          s"create a stream to $p2pEndpointId over gRPC channel $channel",
                          exception,
                          connectRetryDelay,
                          attemptNumber + 1,
                        ).value // We are rebuilding the OptionT, so we need to extract the FUS by calling `value`
                    }
                  )
              }
          }
      } yield result
    }

  private def createConnectionOpener(
      thisNode: BftNodeId
  ): BftOrderingMessage = {
    val networkSendInstant = Instant.now()
    BftOrderingMessage(
      "",
      Some(
        BftOrderingMessageBody(
          BftOrderingMessageBody.Message.ConnectionOpened(ConnectionOpened())
        )
      ),
      thisNode,
      Some(Timestamp(networkSendInstant.getEpochSecond, networkSendInstant.getNano)),
    )
  }

  private def shutdownGrpcChannelIfNeeded(
      p2pEndpointId: P2PEndpoint.Id,
      channel: ManagedChannel,
      authenticationContextO: Option[GrpcSequencerClientAuth],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val channelId = objId(channel)
    if (!channel.isShutdown) {
      val authenticationContextId = authenticationContextO.map(objId)
      FutureUnlessShutdown.outcomeF(Future {
        authenticationContextO.foreach { ac =>
          logger.info(
            s"Closing the authentication context $authenticationContextId for gRPC channel $channelId to $p2pEndpointId"
          )
          ac.close()
        }
        logger.info(s"Shutting down gRPC channel $channelId to $p2pEndpointId")
        // We ensure channels are terminated before returning in order to avoid orphans, but this requires blocking,
        //  so we use the execution context for long-running operations that will create threads as needed.
        val shutDownChannel = channel.shutdownNow()
        val terminated =
          blocking {
            shutDownChannel
              .awaitTermination(
                timeouts.closing.duration.toMillis,
                java.util.concurrent.TimeUnit.MILLISECONDS,
              )
          }
        if (!terminated) {
          logger.warn(
            s"Failed to terminate in ${timeouts.closing.duration} the gRPC channel $channelId to $p2pEndpointId"
          )
        } else {
          logger.info(
            s"Successfully terminated gRPC channel $channelId to $p2pEndpointId"
          )
        }
      }(longRunningExecutionContext))
    } else {
      logger.info(
        s"No need to shut down gRPC channel $channelId with authentication context $authenticationContextO " +
          s"to $p2pEndpointId (already shut down)"
      )
      FutureUnlessShutdown.unit
    }
  }

  // Called by the BFT ordering service when receiving a new gRPC streaming connection
  def createServerSidePeerReceiver(
      inputModule: ModuleRef[BftOrderingMessage],
      peerSender: StreamObserver[BftOrderingMessage],
  )(implicit
      executionContext: ExecutionContext,
      metricsContext: MetricsContext,
      traceContext: TraceContext,
  ): UnlessShutdown[StreamObserver[BftOrderingMessage]] = {
    val peerSenderId = objId(peerSender)
    if (!isClosing) {
      logger.info("Creating a peer receiver for an incoming connection")
      Try(peerSender.onNext(createConnectionOpener(thisNode))) match {

        case Failure(exception) =>
          logger.info(
            s"Failed to send the connection opener message to peer sender $peerSenderId",
            exception,
          )
          // Close the sender and fail accepting the connection
          failGrpcStreamObserver(peerSender, exception, logger)
          throw exception

        case Success(()) =>
          val sequencerIdPromiseUS = PromiseUnlessShutdown.unsupervised[SequencerId]()
          if (isAuthenticationEnabled)
            extractSequencerIdFromGrpcContextInto(sequencerIdPromiseUS)
          val peerReceiver =
            new P2PGrpcStreamingReceiver(
              maybeP2PEndpointId = None,
              inputModule,
              sequencerIdPromiseUS,
              isAuthenticationEnabled,
              metrics,
              loggerFactory,
            ) {
              override def shutdown(): Unit =
                shutdownIncomingConnectionDueToRemoteCompletion(peerSender)
            }
          val peerReceiverId = objId(peerReceiver)
          logger.info(
            s"Successfully created a peer receiver $peerReceiverId for an incoming connection"
          )
          // A connecting node could omit the peer endpoint when P2P endpoint authentication is disabled,
          //  or send a wrong or different one; in that case, a subsequent send attempt by this node to an endpoint
          //  of that peer won't find the gRPC channel and will create a new one in the opposite direction that will
          //  effectively be a duplicate; however, when the sequencer ID of this duplicate connection is received,
          //  it will be detected as duplicate by the connection state and shut down.
          //  This also protects against potentially malicious peers that try to establish more than one connection.
          val maybeEndpoint = ServerAuthenticatingServerInterceptor.peerEndpointContextKey.get()
          logger.info(
            s"Peer endpoint communicated via the server context: $maybeEndpoint; " +
              "adding the connection to the state asynchronously as soon as a sequencer ID is available"
          )
          sequencerIdPromiseUS.futureUS
            .map(tryAddPeerEndpoint(_, peerSender, maybeEndpoint))
            .transform(
              identity,
              { exception =>
                logger.info(
                  s"Failed authenticating incoming connection with sender $peerSender, closing the sender",
                  exception,
                )
                // Close the connection by failing the sender; no need to close the receiver as it will be
                //  uninstalled by closing the connection and no state has been updated yet.
                failGrpcStreamObserver(peerSender, exception, logger)
                exception
              },
            )
            .discard
          UnlessShutdown.Outcome(peerReceiver)
      }
    } else {
      val msg =
        s"Not creating a P2P gRPC stream receiver for incoming connection with sender $peerSender " +
          "due to shutdown"
      logger.info(msg)
      UnlessShutdown.AbortedDueToShutdown
    }
  }

  private def shutdownOutgoingConnectionIfNeeded(
      p2pEndpointId: P2PEndpoint.Id,
      onlyIfNotFullyConnected: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    transitionToDisconnected(p2pEndpointId, onlyIfNotFullyConnected).logAndExtract(prefix =
      s"State transition when potentially shutting down outgoing connection for $p2pEndpointId " +
        s"(onlyIfNotFullyConnected: $onlyIfNotFullyConnected): "
    ) match {
      case Left(fus) => fus
      case Right(channel -> authenticationContextO) =>
        val channelId = objId(channel)
        val authenticationContextId = authenticationContextO.map(objId)
        logger.debug(
          s"Shutting down the gRPC channel $channelId " +
            s"with authentication context $authenticationContextId to $p2pEndpointId"
        )
        shutdownGrpcChannelIfNeeded(p2pEndpointId, channel, authenticationContextO)
    }

  private def extractSequencerIdFromGrpcContextInto(
      sequencerIdPromiseUS: PromiseUnlessShutdown[SequencerId]
  )(implicit traceContext: TraceContext): Unit =
    IdentityContextHelper.storedMemberContextKey
      .get()
      .fold(
        abort(logger, "Authentication is enabled but the context does not contain a member ID!")
      ) {
        case sequencerId: SequencerId =>
          logger.info(
            s"Found sequencer ID ${sequencerId.toProtoPrimitive} in the context of the incoming connection"
          )
          sequencerIdPromiseUS.outcome_(sequencerId)
        case _ =>
          // If the context is not set, it means that authentication is not enabled or there is a bug,
          //  as the connection should have already been killed in that case.
          abort(logger, "Authentication is enabled but the peer is not a sequencer!")
      }

  private def attemptTransitionToConnecting(
      p2pEndpointId: P2PEndpoint.Id
  )(implicit traceContext: TraceContext): AnnotatedResult[Boolean] =
    AtomicUtil.updateAndGetComputed(p2pOutgoingConnectionsStatusRef) {
      case s @ UnlessShutdown.Outcome(p2pConnectionsStatus) =>
        p2pConnectionsStatus.get(p2pEndpointId) match {
          case None =>
            // No connection [attempt], create gRPC channel and connect
            UnlessShutdown.Outcome(
              p2pConnectionsStatus.updated(
                p2pEndpointId,
                P2POutgoingConnectionStatus.Connecting,
              )
            ) -> AnnotatedResult(
              true, // Start connection
              () => "Disconnected (not in state) -> Connecting",
              debug,
            )

          case Some(status) =>
            status match {

              case P2POutgoingConnectionStatus.DisconnectingFromChannel(ch, acO, cw) =>
                // Connect worker still active on a gRPC channel and asked to disconnect, cancel request
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                lazy val cwId = objId(cw)
                UnlessShutdown.Outcome(
                  p2pConnectionsStatus
                    .updated(
                      p2pEndpointId,
                      P2POutgoingConnectionStatus.ConnectingOnChannel(ch, acO, Some(cw)),
                    )
                ) ->
                  AnnotatedResult(
                    false,
                    () =>
                      s"DisconnectingFromChannel(ch: $chId, ac: $acId, cw: $cwId -> " +
                        s"ConnectingOnChannel(ch: $chId, ac: $acId, cw: $cwId)",
                    debug,
                  )

              case P2POutgoingConnectionStatus.Connecting =>
                // Already connecting
                s -> AnnotatedResult(
                  false,
                  () => "Connecting (unchanged)",
                  debug,
                )

              case P2POutgoingConnectionStatus.ConnectingOnChannel(ch, acO, cw) =>
                // Already connecting
                s -> AnnotatedResult(
                  false,
                  () => s"ConnectingOnChannel(ch: ${objId(ch)}, cw: ${cw.map(objId)}) (unchanged)",
                  debug,
                )

              case P2POutgoingConnectionStatus.ConnectedOnChannel(ch, acO) =>
                // Already connected
                s -> AnnotatedResult(
                  false,
                  () => s"Connected(ch: ${objId(ch)}, ac: ${acO.map(objId)}) (unchanged)",
                  debug,
                )
            }
        }

      case s @ UnlessShutdown.AbortedDueToShutdown =>
        s -> AnnotatedResult(
          false,
          () => "Disconnected (state shut down, unchanged)",
          debug,
        )
    }

  private def attemptTransitionToConnectingWithChannel(
      p2pEndpointId: P2PEndpoint.Id,
      channel: ManagedChannel,
      authenticationContextO: Option[GrpcSequencerClientAuth],
  )(implicit traceContext: TraceContext): AnnotatedResult[Boolean] =
    AtomicUtil.updateAndGetComputed(p2pOutgoingConnectionsStatusRef) {
      case s @ UnlessShutdown.Outcome(p2pConnectionsStatus) =>
        p2pConnectionsStatus.get(p2pEndpointId) match {

          case Some(status) =>
            status match {

              case P2POutgoingConnectionStatus.Connecting =>
                UnlessShutdown.Outcome(
                  p2pConnectionsStatus
                    .updated(
                      p2pEndpointId,
                      P2POutgoingConnectionStatus
                        .ConnectingOnChannel(channel, authenticationContextO, connectWorkerO = None),
                    )
                ) -> AnnotatedResult(
                  true,
                  () =>
                    s"Connecting -> ConnectingOnChannel(ch: ${objId(channel)}, ac: ${authenticationContextO
                        .map(objId)}, cw: None)",
                  debug,
                )

              case P2POutgoingConnectionStatus.ConnectedOnChannel(ch, acO) =>
                s -> AnnotatedResult(
                  false,
                  () => s"Connected(ch: ${objId(ch)}, ac: ${acO.map(objId)}) (unchanged)",
                  logUnexpected,
                )

              case P2POutgoingConnectionStatus.ConnectingOnChannel(ch, acO, cwO) =>
                s -> AnnotatedResult(
                  false,
                  () =>
                    s"ConnectingOnChannel(ch: ${objId(ch)}, ac: ${acO.map(objId)}, cw: ${cwO.map(objId)}) (unchanged)",
                  logUnexpected,
                )

              case P2POutgoingConnectionStatus.DisconnectingFromChannel(ch, acO, cw) =>
                s -> AnnotatedResult(
                  false,
                  () =>
                    s"DisconnectingFromChannel(ch: ${objId(ch)}, ac: ${acO.map(objId)}, cw: ${objId(cw)}) (unchanged)",
                  logUnexpected,
                )
            }

          case None =>
            // gRPC channel shut down before recording the new channel
            s -> AnnotatedResult(
              false,
              () => "Disconnected (not in state) (unchanged)",
              debug,
            )
        }

      case s @ UnlessShutdown.AbortedDueToShutdown =>
        s -> AnnotatedResult(
          false,
          () => "Disconnected (state shut down, unchanged)",
          debug,
        )
    }

  private def attemptTransitionToConnectingWithChannelAndWorker(
      p2pEndpointId: P2PEndpoint.Id,
      channel: ManagedChannel,
      connectWorker: FutureUnlessShutdown[Unit],
  )(implicit traceContext: TraceContext): AnnotatedResult[Unit] =
    AtomicUtil.updateAndGetComputed(p2pOutgoingConnectionsStatusRef) {
      case s @ UnlessShutdown.Outcome(p2pConnectionsStatus) =>
        val connectWorkerId = objId(connectWorker)
        p2pConnectionsStatus.get(p2pEndpointId) match {

          case Some(status) =>
            status match {

              case P2POutgoingConnectionStatus.ConnectingOnChannel(ch, acO, cwO) =>
                lazy val chId = objId(ch)
                if (ch == channel && cwO.isEmpty)
                  // Record the connect worker
                  UnlessShutdown.Outcome(
                    p2pConnectionsStatus
                      .updated(
                        p2pEndpointId,
                        P2POutgoingConnectionStatus.ConnectingOnChannel(
                          channel,
                          acO,
                          Some(connectWorker),
                        ),
                      )
                  ) -> AnnotatedResult(
                    (),
                    () =>
                      s"ConnectingOnChannel(ch: $chId, cw: None) -> " +
                        s"ConnectingOnChannel(ch: $chId, cw: Some($connectWorkerId)",
                    debug,
                  )
                else
                  s -> AnnotatedResult(
                    (),
                    () => s"ConnectingOnChannel(ch: $chId, cw: ${cwO.map(objId)}) (unchanged)",
                    logUnexpected,
                  )

              case P2POutgoingConnectionStatus.DisconnectingFromChannel(ch, acO, cw) =>
                s -> AnnotatedResult(
                  (),
                  () =>
                    s"DisconnectingFromChannel(ch: ${objId(ch)}, ac: ${acO.map(objId)}, cw: ${objId(cw)}) (unchanged)",
                  logUnexpected,
                )

              case P2POutgoingConnectionStatus.Connecting =>
                s -> AnnotatedResult(
                  (),
                  () => "Connecting (unchanged)",
                  logUnexpected,
                )

              case P2POutgoingConnectionStatus.ConnectedOnChannel(ch, acO) =>
                s -> AnnotatedResult(
                  (),
                  () => s"ConnectedOnChannel(ch: ${objId(ch)}, ac: ${acO.map(objId)}) (unchanged)",
                  logUnexpected,
                )
            }

          case None =>
            // Disconnection requested, the connect worker will see that and clean up
            s -> AnnotatedResult(
              (),
              () => "Disconnected (not in state) (unchanged)",
              debug,
            )
        }

      case s @ UnlessShutdown.AbortedDueToShutdown =>
        s -> AnnotatedResult(
          (),
          () => "Disconnected (state shut down, unchanged)",
          debug,
        )
    }

  private def attemptTransitionToRetryConnecting(
      p2pEndpointId: P2PEndpoint.Id,
      channel: ManagedChannel,
  )(implicit traceContext: TraceContext): AnnotatedResult[Boolean] =
    AtomicUtil.updateAndGetComputed(p2pOutgoingConnectionsStatusRef) {
      case s @ UnlessShutdown.Outcome(p2pConnectionsStatus) =>
        p2pConnectionsStatus.get(p2pEndpointId) match {

          case Some(status) =>
            status match {

              case P2POutgoingConnectionStatus.ConnectingOnChannel(ch, acO, cwO) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                lazy val cwId = cwO.map(objId)
                if (ch == channel)
                  // This connect worker is still valid
                  s -> AnnotatedResult(
                    true,
                    () =>
                      s"ConnectingOnChannel(ch: $chId = this worker's channel, ac: $acId, cw: $cwId) (unchanged)",
                    debug,
                  )
                else
                  s -> AnnotatedResult(
                    false,
                    () =>
                      s"ConnectingOnChannel(ch: $chId = another worker's channel, ac: $acId, cw: $cwId) (unchanged)",
                    logUnexpected,
                  )

              case P2POutgoingConnectionStatus.DisconnectingFromChannel(ch, acO, cw) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                lazy val cwId = objId(cw)
                if (ch == channel)
                  // Requested to disconnect
                  UnlessShutdown.Outcome(
                    p2pConnectionsStatus.removed(
                      p2pEndpointId
                    )
                  ) -> AnnotatedResult(
                    false,
                    () =>
                      s"DisconnectingFromChannel(ch: $chId = this worker's channel, ac: $acId, cw: $cwId) -> " +
                        "Disconnected (not in state)",
                    debug,
                  )
                else
                  s -> AnnotatedResult(
                    false,
                    () =>
                      s"ConnectingOnChannel(ch: $chId = another worker's channel, ac: $acId, cw: $cwId) (unchanged)",
                    logUnexpected,
                  )

              case P2POutgoingConnectionStatus.ConnectedOnChannel(ch, acO) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                s -> AnnotatedResult(
                  false,
                  () =>
                    if (ch == channel)
                      s"ConnectedOnChannel(ch: $chId = this worker's channel, ac: $acId) (unchanged)"
                    else
                      s"ConnectedOnChannel(ch: $chId = another worker's channel, ac: $acId) (unchanged)",
                  logUnexpected,
                )

              case P2POutgoingConnectionStatus.Connecting =>
                s -> AnnotatedResult(
                  false,
                  () => "Connecting (unchanged)",
                  logUnexpected,
                )
            }

          case None =>
            s -> AnnotatedResult(
              false,
              () => "Disconnected (not in state) (unchanged)",
              logUnexpected,
            )
        }

      case s @ UnlessShutdown.AbortedDueToShutdown =>
        s -> AnnotatedResult(
          false,
          () => "Disconnected (state shut down, unchanged)",
          debug,
        )
    }

  private def attemptConnectionOrDisconnectionCompletion(
      p2pEndpointId: P2PEndpoint.Id,
      channel: ManagedChannel,
      authenticationContextO: Option[GrpcSequencerClientAuth],
  )(implicit
      traceContext: TraceContext
  ): AnnotatedResult[Option[(ManagedChannel, Option[GrpcSequencerClientAuth])]] =
    AtomicUtil.updateAndGetComputed(p2pOutgoingConnectionsStatusRef) {
      case s @ UnlessShutdown.Outcome(p2pConnectionsStatus) =>
        p2pConnectionsStatus.get(p2pEndpointId) match {

          case Some(status) =>
            status match {

              case P2POutgoingConnectionStatus.ConnectingOnChannel(ch, acO, cwO) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                lazy val cwId = cwO.map(objId)
                if (ch == channel)
                  // Mark connection as complete
                  UnlessShutdown.Outcome(
                    p2pConnectionsStatus
                      .updated(
                        p2pEndpointId,
                        P2POutgoingConnectionStatus.ConnectedOnChannel(channel, acO),
                      )
                  ) -> AnnotatedResult(
                    None,
                    () =>
                      s"ConnectingOnChannel(ch: $chId = this worker's channel, ac: $acId, cw: $cwId) -> " +
                        s"ConnectedOnChannel(ch: $chId)",
                    debug,
                  )
                else
                  s -> AnnotatedResult(
                    Some(channel -> acO),
                    () =>
                      s"ConnectingOnChannel(ch: $chId = another worker's channel, ac: $acId, cw: $cwId) (unchanged)",
                    logUnexpected,
                  )

              case P2POutgoingConnectionStatus
                    .DisconnectingFromChannel(ch, acO, cw) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                lazy val cwId = objId(cw)
                if (ch == channel)
                  // Complete disconnection request
                  UnlessShutdown.Outcome(
                    p2pConnectionsStatus.removed(p2pEndpointId)
                  ) -> AnnotatedResult(
                    Some(channel -> acO),
                    () =>
                      s"DisconnectingFromChannel(ch: $chId = this worker's channel, ac: $acId, cw: $cwId) -> " +
                        "Disconnected (not in state)",
                    debug,
                  )
                else
                  s -> AnnotatedResult(
                    Some(channel -> acO),
                    () =>
                      s"DisconnectingFromChannel(ch: $chId = another worker's channel, ac: $acId, cw: $cwId) " +
                        "(unchanged)",
                    logUnexpected,
                  )

              case P2POutgoingConnectionStatus.ConnectedOnChannel(ch, acO) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                s -> AnnotatedResult(
                  None,
                  () =>
                    if (ch == channel)
                      s"ConnectedOnChannel(ch: $chId = this worker's channel, ac: $acId) (unchanged)"
                    else
                      s"ConnectedOnChannel(ch: $chId = another worker's channel, ac: $acId) (unchanged)",
                  logUnexpected,
                )

              case P2POutgoingConnectionStatus.Connecting =>
                s -> AnnotatedResult(
                  Some(channel -> authenticationContextO),
                  () => "Connecting (unchanged)",
                  logUnexpected,
                )
            }

          case None =>
            // gRPC channel shut down before the running worker was recorded as assigned to it
            s -> AnnotatedResult(
              Some(channel -> authenticationContextO),
              () => "Disconnected (not in state) (unchanged)",
              debug,
            )
        }

      case s @ UnlessShutdown.AbortedDueToShutdown =>
        s -> AnnotatedResult(
          Some(channel -> authenticationContextO),
          () => "Disconnected (state shut down, unchanged)",
          debug,
        )
    }

  private def attemptTransitionToDisconnectedAfterConnectWorkerFailed(
      p2pEndpointId: P2PEndpoint.Id,
      channel: ManagedChannel,
  )(implicit traceContext: TraceContext): AnnotatedResult[Boolean] =
    AtomicUtil.updateAndGetComputed(p2pOutgoingConnectionsStatusRef) {
      case s @ UnlessShutdown.Outcome(p2pConnectionsStatus) =>
        p2pConnectionsStatus.get(p2pEndpointId) match {

          case Some(status) =>
            // Shut down the channel whenever it's associated to this connect worker that failed
            //  and transition to Disconnected

            status match {

              case P2POutgoingConnectionStatus.ConnectedOnChannel(ch, acO) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                if (ch == channel)
                  UnlessShutdown.Outcome(
                    p2pConnectionsStatus.removed(
                      p2pEndpointId
                    )
                  ) -> AnnotatedResult(
                    true,
                    () =>
                      s"ConnectedOnChannel(ch: $chId = this worker's channel, ac: $acId) -> " +
                        "Disconnected (not in state)",
                    debug,
                  )
                else
                  s -> AnnotatedResult(
                    false,
                    () =>
                      s"ConnectedOnChannel(ch: $chId = another worker's channel, ac: $acId) (unchanged)",
                    debug,
                  )

              case P2POutgoingConnectionStatus.ConnectingOnChannel(ch, acO, cwO) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                lazy val cwId = cwO.map(objId)
                if (ch == channel)
                  UnlessShutdown.Outcome(
                    p2pConnectionsStatus.removed(
                      p2pEndpointId
                    )
                  ) -> AnnotatedResult(
                    true,
                    () =>
                      s"ConnectingOnChannel(ch: $chId = this worker's channel, ac: $acId, cw: $cwId) -> " +
                        "Disconnected (not in state)",
                    debug,
                  )
                else
                  s -> AnnotatedResult(
                    false,
                    () =>
                      s"ConnectingOnChannel(ch: $ch = another worker's channel, ac: $acId, cw: $cwId) (unchanged)",
                    debug,
                  )

              case P2POutgoingConnectionStatus.DisconnectingFromChannel(ch, acO, cw) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                lazy val cwId = objId(cw)
                if (ch == channel)
                  UnlessShutdown.Outcome(
                    p2pConnectionsStatus.removed(
                      p2pEndpointId
                    )
                  ) -> AnnotatedResult(
                    true,
                    () =>
                      s"DisconnectingFromChannel(ch: $chId = this worker's channel, ac: $acId, cw: $cwId) -> " +
                        "Disconnected (not in state)",
                    debug,
                  )
                else
                  s -> AnnotatedResult(
                    false,
                    () =>
                      s"DisconnectingFromChannel(ch: $chId = another worker's channel, ac: $acId, cw: $cwId) (unchanged)",
                    debug,
                  )

              case P2POutgoingConnectionStatus.Connecting =>
                s -> AnnotatedResult(
                  false,
                  () => "Connecting (unchanged)",
                  debug,
                )
            }

          case None =>
            s -> AnnotatedResult(
              true,
              () => "Disconnected (not in state) (unchanged)",
              debug,
            )
        }

      case s @ UnlessShutdown.AbortedDueToShutdown =>
        s -> AnnotatedResult(
          true,
          () => "Disconnected (state shut down, unchanged)",
          debug,
        )
    }

  private def transitionToDisconnected(
      p2pEndpointId: P2PEndpoint.Id,
      onlyIfNotConnected: Boolean,
  )(implicit
      traceContext: TraceContext
  ): AnnotatedResult[
    Either[FutureUnlessShutdown[Unit], (ManagedChannel, Option[GrpcSequencerClientAuth])]
  ] =
    AtomicUtil.updateAndGetComputed(p2pOutgoingConnectionsStatusRef) {
      case s @ UnlessShutdown.Outcome(p2pConnectionsStatus) =>
        p2pConnectionsStatus.get(p2pEndpointId) match {

          case Some(p2pConnectionStatus) =>
            p2pConnectionStatus match {

              case P2POutgoingConnectionStatus.ConnectedOnChannel(ch, acO) =>
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                if (onlyIfNotConnected) {
                  s -> AnnotatedResult(
                    Left(FutureUnlessShutdown.unit),
                    () => s"ConnectedOnChannel(ch: $chId, ac: $acId) (unchanged)",
                    debug,
                  )
                } else {
                  // No connect worker, just close the channel
                  UnlessShutdown.Outcome(
                    p2pConnectionsStatus.removed(p2pEndpointId)
                  ) -> AnnotatedResult(
                    Right(ch -> acO),
                    () =>
                      s"ConnectedOnChannel(ch: $chId, ac: $acId) -> Disconnected (not in state)",
                    debug,
                  )
                }

              case P2POutgoingConnectionStatus.Connecting =>
                // Let the gRPC channel setup logic orderly abort the connection attempt
                UnlessShutdown.Outcome(
                  p2pConnectionsStatus.removed(p2pEndpointId)
                ) -> AnnotatedResult(
                  Left(FutureUnlessShutdown.unit),
                  () => "Connecting -> Disconnected (not in state)",
                  debug,
                )

              case P2POutgoingConnectionStatus.ConnectingOnChannel(ch, acO, Some(cw)) =>
                // Let the connect worker orderly abort the connection attempt
                lazy val chId = objId(ch)
                lazy val acId = acO.map(objId)
                lazy val cwId = objId(cw)
                UnlessShutdown.Outcome(
                  p2pConnectionsStatus.updated(
                    p2pEndpointId,
                    P2POutgoingConnectionStatus.DisconnectingFromChannel(ch, acO, cw),
                  )
                ) -> AnnotatedResult(
                  Left(cw),
                  () =>
                    s"ConnectingOnChannel(ch: $chId, ac: $acId, cw: $cwId) -> " +
                      s"DisconnectingFromChannel(ch: $chId, ac: $acId, cw: $cwId)",
                  debug,
                )

              case P2POutgoingConnectionStatus.ConnectingOnChannel(ch, acO, None) =>
                // Let the connect worker orderly abort the connection attempt
                UnlessShutdown.Outcome(
                  p2pConnectionsStatus.removed(p2pEndpointId)
                ) -> AnnotatedResult(
                  Right(ch -> acO),
                  () =>
                    s"ConnectingOnChannel(ch: $ch, ac: ${acO.map(objId)}, cw: None) -> " +
                      "Disconnected (not in state)",
                  debug,
                )

              case P2POutgoingConnectionStatus.DisconnectingFromChannel(ch, acO, cw) =>
                // Let the connect worker finish aborting the connection attempt
                s -> AnnotatedResult(
                  Left(cw),
                  () =>
                    s"DisconnectingFromChannel(ch: $ch, ac: ${acO.map(objId)}, cw: ${objId(cw)}) (unchanged)",
                  debug,
                )
            }

          case None =>
            // No connection established nor in progress
            s -> AnnotatedResult(
              Left(FutureUnlessShutdown.unit),
              () => "Disconnected (not in state) (unchanged)",
              debug,
            )
        }

      case s @ UnlessShutdown.AbortedDueToShutdown =>
        s -> AnnotatedResult(
          Left(FutureUnlessShutdown.unit),
          () => "Disconnected (state shut down, unchanged)",
          debug,
        )
    }

  private def debug(s: => String)(implicit traceContext: TraceContext): Unit =
    logger.debug(s)

  private def logUnexpected(s: => String)(implicit traceContext: TraceContext): Unit =
    logger.warn(s"[UNEXPECTED] $s")
}

private[bftordering] object P2PGrpcConnectionManager {

  sealed trait P2POutgoingConnectionStatus extends Product with Serializable {
    val channelO: Option[(ManagedChannel, Option[GrpcSequencerClientAuth])]
  }
  object P2POutgoingConnectionStatus {
    final case object Connecting extends P2POutgoingConnectionStatus {
      override val channelO: Option[(ManagedChannel, Option[GrpcSequencerClientAuth])] = None
    }
    final case class ConnectingOnChannel(
        channel: ManagedChannel,
        authenticationContextO: Option[GrpcSequencerClientAuth],
        connectWorkerO: Option[FutureUnlessShutdown[Unit]],
    ) extends P2POutgoingConnectionStatus {
      override val channelO: Option[(ManagedChannel, Option[GrpcSequencerClientAuth])] =
        Some(channel -> authenticationContextO)
    }
    final case class ConnectedOnChannel(
        channel: ManagedChannel,
        authenticationContextO: Option[GrpcSequencerClientAuth],
    ) extends P2POutgoingConnectionStatus {
      override val channelO: Option[(ManagedChannel, Option[GrpcSequencerClientAuth])] =
        Some(channel -> authenticationContextO)
    }
    final case class DisconnectingFromChannel(
        channel: ManagedChannel,
        authenticationContextO: Option[GrpcSequencerClientAuth],
        connectWorker: FutureUnlessShutdown[Unit],
    ) extends P2POutgoingConnectionStatus {
      override val channelO: Option[(ManagedChannel, Option[GrpcSequencerClientAuth])] =
        Some(channel -> authenticationContextO)
    }
  }

  private final case class OpenChannel(
      channel: ManagedChannel,
      maybeAuthenticationContext: Option[GrpcSequencerClientAuth],
      // A promise that will be present if authentication is enabled, and in that case it will be completed
      //  by the gRPC client interceptor with the sequencer ID
      maybeSequencerIdFromAuthenticationPromiseUS: Option[
        PromiseUnlessShutdown[SequencerId]
      ],
      asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
  )

  final case class AnnotatedResult[T](
      private val result: T,
      private val annotation: () => String,
      private val logger: (=> String) => Unit,
  ) {
    def logAndExtract(prefix: String): T = {
      logger(s"$prefix ${annotation()}")
      result
    }
  }

  private def toUnitFutureUS[X](optionT: OptionT[FutureUnlessShutdown, X])(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[Unit] =
    optionT.value.map(_ => ())
}
