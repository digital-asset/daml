// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import cats.data.OptionT
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
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
  mutex,
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
import com.digitalasset.canton.util.DelayUtil
import com.google.protobuf.timestamp.Timestamp
import io.grpc.stub.{AbstractStub, StreamObserver}
import io.grpc.{Channel, ClientInterceptors, ManagedChannel}
import org.slf4j.event.Level

import java.time.Instant
import java.util.concurrent.{Executor, Executors, ThreadLocalRandom}
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future, blocking}
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
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext, metricsContext: MetricsContext)
    extends NamedLogging
    with FlagCloseable { self =>

  import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionManager.*

  private val random = ThreadLocalRandom.current()

  private val isAuthenticationEnabled: Boolean = authenticationInitialState.isDefined

  private val connectExecutor = Executors.newCachedThreadPool()
  private val connectExecutionContext = ExecutionContext.fromExecutor(connectExecutor)
  private val connectWorkers =
    mutable.Map[P2PEndpoint.Id, (ManagedChannel, FutureUnlessShutdown[Unit])]()
  private val channels = mutable.Map[P2PEndpoint.Id, ManagedChannel]()
  private val grpcSequencerClientAuths = mutable.Map[P2PEndpoint.Id, GrpcSequencerClientAuth]()

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
            logger.debug(s"Found existing sender for $p2pAddress")
            maybeP2PEndpoint.map(_.id).foreach(signalConnectWorkerToStop)
            found
          case _ =>
            logger.debug(
              s"Requested a send but no sender found for $p2pAddress, ensuring connection worker is running"
            )
            maybeP2PEndpoint.foreach(ensureConnectWorker)
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
      s"Cleaning up active outgoing connection to $p2pAddressId"
    )
    val maybeP2PEndpointId = p2pAddressId.left.toOption
    maybeP2PEndpointId.foreach(ensureChannelClosed(_)(connectExecutionContext, traceContext))
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

  // Called by the peer receiver of an outgoing connection on error and on completion
  //  which also occurs in case of duplicate connection.
  //  No network ref associations must be changed and no network ref must be closed,
  //  as the connection will be re-established.
  private def cleanupOutgoingConnectionDueToRemoteCompletion(
      peerEndpointId: P2PEndpoint.Id,
      peerSender: StreamObserver[BftOrderingMessage],
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(s"Cleaning up (active or duplicate) outgoing connection to $peerEndpointId")
    ensureChannelClosed(peerEndpointId)(connectExecutionContext, traceContext)
    cleanupPeerSender(peerSender)
  }

  // Called by the peer receiver of an incoming connection on error and on completion,
  //  which also occurs in case of duplicate connection.
  //  No network ref associations must be changed and no network ref must be closed,
  //  as the connection will be re-established.
  private def cleanupIncomingConnectionDueToRemoteCompletion(
      peerSender: StreamObserver[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(s"Cleaning up (active or duplicate) incoming connection with sender $peerSender")
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

  override def onClosed(): Unit = {
    import TraceContext.Implicits.Empty.*
    logger.info("Closing P2P gRPC client connection manager")
    logger.info("Shutting down authenticators")
    // We cannot lock threads across a potentially long await, so we use finer-grained locks
    mutex(this)(grpcSequencerClientAuths.values.toSeq).foreach(_.close())
    timeouts.closing
      .await(
        "bft-ordering-grpc-networking-state-client-close",
        logFailing = Some(Level.WARN),
      )(asyncCloseConnectionState())
      .discard
    logger.info("Shutting down gRPC channels")
    mutex(this)(channels.values.toSeq).foreach(_.shutdown().discard)
    logger.info("Shutting down connection executor")
    connectExecutor.shutdown()
    logger.info("Closed P2P gRPC connection manager")
  }

  private def asyncCloseConnectionState()(implicit traceContext: TraceContext): Future[Unit] =
    Future
      .sequence(mutex(this)(p2pGrpcConnectionState.connections.map {
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
      }))
      .map(_ => ())

  private def maybeP2PAddressId(
      maybeP2PEndpointId: Option[P2PEndpoint.Id],
      maybeBftNodeId: Option[BftNodeId],
  ): Option[P2PAddress.Id] =
    maybeBftNodeId
      .map(Right(_))
      .orElse(maybeP2PEndpointId.map(Left(_)))

  private def ensureConnectWorker(
      p2pEndpoint: P2PEndpoint
  )(implicit traceContext: TraceContext): Unit = {
    val p2pEndpointId = p2pEndpoint.id
    mutex(this)(connectWorkers.get(p2pEndpointId)) match {
      case Some(_ -> task) if !task.isCompleted =>
        logger.info(s"Connection worker for $p2pEndpointId is already running")
      case _ =>
        logger.info(s"Starting connection worker for $p2pEndpointId:")
        connect(p2pEndpoint)(connectExecutionContext, traceContext)
          .foreach(channelAndWorker =>
            mutex(this)(connectWorkers.put(p2pEndpointId, channelAndWorker)).discard
          )
    }
  }

  private def connect(
      p2pEndpoint: P2PEndpoint
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Option[(ManagedChannel, FutureUnlessShutdown[Unit])] = {
    val p2pEndpointId = p2pEndpoint.id
    if (!isClosing) {
      logger.info(s"Creating a gRPC channel to $p2pEndpointId")
      openGrpcChannel(p2pEndpoint) match {
        case OpenChannel(
              channel,
              maybeSequencerIdFromAuthenticationPromiseUS,
              asyncStub,
            ) =>
          p2pConnectionEventListener.onConnect(p2pEndpointId)
          val sequencerIdUS =
            maybeSequencerIdFromAuthenticationPromiseUS.getOrElse(
              // Authentication is disabled, the peer receiver will use the first message's sentBy to backfill
              PromiseUnlessShutdown.unsupervised[SequencerId]()
            )
          // Add the connection to the state asynchronously as soon as a sequencer ID is available
          Some(
            channel ->
              addPeerEndpointForOutgoingConnectionOnAuthenticationCompletion(
                p2pEndpoint,
                channel,
                asyncStub,
                sequencerIdUS,
              )
          )
      }
    } else {
      logger.info(
        s"Not attempting to create a gRPC channel and connect to $p2pEndpointId due to shutdown"
      )
      None
    }
  }

  private def addPeerEndpointForOutgoingConnectionOnAuthenticationCompletion(
      p2pEndpoint: P2PEndpoint,
      channel: ManagedChannel,
      asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
      sequencerIdUS: PromiseUnlessShutdown[SequencerId],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] = {
    val p2pEndpointId = p2pEndpoint.id
    val peerSenderOT =
      createPeerSender(p2pEndpoint, channel, asyncStub, sequencerIdUS)
    sequencerIdUS.futureUS
      .flatMap { sequencerId =>
        toUnitFutureUS(
          peerSenderOT
            .map { peerSender =>
              logger.info(
                s"P2P endpoint $p2pEndpointId successfully connected and authenticated as ${sequencerId.toProtoPrimitive}"
              )
              tryAddPeerEndpoint(
                sequencerId,
                peerSender,
                Some(p2pEndpoint),
              )
            }
        )
      }
      .transform {
        case f @ Failure(exception) =>
          logger.info(
            s"Failed connecting to and authenticating P2P endpoint $p2pEndpointId, shutting down the gRPC channel",
            exception,
          )
          ensureChannelClosed(p2pEndpointId)(connectExecutionContext, traceContext)
          f
        case s: Success[_] => s
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
      s"Adding peer endpoint $maybeP2PEndpointId for $bftNodeId with sender $peerSender"
    )
    maybeP2PEndpointId.map(
      p2pGrpcConnectionState.associateP2PEndpointIdToBftNodeId(_, bftNodeId)
    ) match {
      case None | Some(Right(())) =>
        if (p2pGrpcConnectionState.addSenderIfMissing(bftNodeId, peerSender)) {
          p2pConnectionEventListener.onSequencerId(bftNodeId, maybeP2PEndpoint)
        } else {
          logger.info(
            s"Completing peer sender $peerSender for $bftNodeId <-> $maybeP2PEndpointId because one already exists"
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

        throw new RuntimeException(error.toString)
    }
  }

  private def openGrpcChannel(
      p2pEndpoint: P2PEndpoint
  )(implicit traceContext: TraceContext): OpenChannel = {
    implicit val executor: Executor = (command: Runnable) => executionContext.execute(command)
    val channel = createChannelBuilder(p2pEndpoint.endpointConfig).build()
    val maybeAuthenticationContext =
      authenticationInitialState.map(auth =>
        new GrpcSequencerClientAuth(
          auth.psId,
          member = auth.sequencerId,
          crypto = auth.authenticationServices.syncCryptoForAuthentication.crypto,
          channelPerEndpoint =
            NonEmpty(Map, Endpoint(p2pEndpoint.address, p2pEndpoint.port) -> channel),
          supportedProtocolVersions = Seq(auth.psId.protocolVersion),
          tokenManagerConfig = auth.authTokenConfig,
          clock = auth.clock,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        )
      )
    mutex(this) {
      val p2pEndpointId = p2pEndpoint.id
      channels.put(p2pEndpointId, channel).discard
      maybeAuthenticationContext.foreach(grpcSequencerClientAuths.put(p2pEndpointId, _).discard)
      logger.info(s"Created gRPC channel to $p2pEndpointId")
    }

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
        maybeAuthenticationContext.fold(augmentedStub)(_.apply(augmentedStub))
      }

    val (potentiallyCheckedChannel, maybeSequencerIdFromAuthenticationPromiseUS) =
      maybeApplyServerAuthenticatingClientInterceptor(channel)

    OpenChannel(
      channel,
      maybeSequencerIdFromAuthenticationPromiseUS,
      maybeAuthenticateStub(BftOrderingServiceGrpc.stub(potentiallyCheckedChannel)),
    )
  }

  // Returns a tuple of the channel, with the server-authenticating gRPC client interceptor applied if authentication is enabled,
  //  and in that case also a promise that is completed with the sequencer ID by the gRPC client interceptor
  //  before the call is activated.
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
      val p2pEndpointId = p2pEndpoint.id
      def retry(
          failedOperationName: String,
          exception: Throwable,
          previousRetryDelay: NonNegativeFiniteDuration,
          attemptNumber: Int,
      ): OptionT[
        FutureUnlessShutdown,
        StreamObserver[BftOrderingMessage],
      ] = {
        def log(msg: => String, exc: Throwable): Unit =
          if (
            attemptNumber <= p2pConnectionManagementConfig.maxConnectionAttemptsBeforeWarning.value
          )
            logger.info(msg, exc)
          else
            logger.warn(msg, exc)

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
        log(
          s"failed to $failedOperationName during attempt $attemptNumber, retrying in $jitteredRetryDelay",
          exception,
        )
        for {
          _ <- OptionT[FutureUnlessShutdown, Unit](
            DelayUtil
              .delayIfNotClosing("grpc-networking", jitteredRetryDelay.toScala, self)
              .map(Some(_))
          ) // Wait for the retry delay
          result <-
            if (mutex(this)(connectWorkers.get(p2pEndpointId).exists(_._1 == channel))) {
              createPeerSender(
                p2pEndpoint,
                channel,
                asyncStub,
                sequencerIdPromiseUS,
                jitteredRetryDelay,
                attemptNumber,
              ) // Async-trampolined
            } else {
              logger.info(
                s"failed to $failedOperationName during attempt $attemptNumber, " +
                  "but not retrying because the connection is being closed",
                exception,
              )
              OptionT
                .none[
                  FutureUnlessShutdown,
                  StreamObserver[BftOrderingMessage],
                ]
            }
        } yield result
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
        s"Creating a P2P gRPC stream receiver for new outgoing connection to $p2pEndpointId"
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
                _.map(cleanupOutgoingConnectionDueToRemoteCompletion(p2pEndpointId, _)),
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
      logger.info(s"Trying to create a stream $p2pEndpointId in $jitteredConnectDelay")
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
                failedOperationName = s"create a stream to $p2pEndpointId",
                exception,
                connectRetryDelay,
                attemptNumber + 1,
              )

            case Success(peerSender) =>
              // Complete the peer sender promise
              peerSenderPromiseUS.outcome_(peerSender)
              logger.info(
                s"Stream to $p2pEndpointId created successfully, " +
                  "sending connection opener  to preemptively check the connection " +
                  "and provide the sequencer ID (if needed)"
              )

              Try(peerSender.onNext(createConnectionOpener(thisNode))) match {

                case Failure(exception) =>
                  // Close the connection by failing the sender; no need to close the receiver as it will be
                  //  uninstalled by closing the connection and no state has been updated yet.
                  failGrpcStreamObserver(peerSender, exception, logger)
                  retry(
                    failedOperationName = s"open endpoint $p2pEndpointId",
                    exception,
                    connectRetryDelay,
                    attemptNumber + 1,
                  )

                case Success(_) =>
                  logger.info(
                    s"Sending connection opener to $p2pEndpointId succeeded, waiting for authentication"
                  )
                  // Retry also if the sequencer ID couldn't be retrieved
                  val sequencerIdFUS = sequencerIdPromiseUS.futureUS
                  OptionT(
                    sequencerIdPromiseUS.futureUS.transformWith {

                      case Success(_) =>
                        sequencerIdFUS.map { sequencerId =>
                          logger.info(
                            s"P2P endpoint $p2pEndpointId successfully authenticated as ${sequencerId.toProtoPrimitive}"
                          )
                          Option(peerSender)
                        }

                      case Failure(exception) =>
                        logger.info(
                          s"P2P endpoint $p2pEndpointId authentication failed, notifying an error to the sender"
                        )
                        // Close the connection by failing the sender; no need to close the receiver as it will be
                        //  uninstalled by closing the connection and no state has been updated yet.
                        failGrpcStreamObserver(peerSender, exception, logger)
                        retry(
                          s"create a stream to $p2pEndpointId",
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

  private def shutdownGrpcChannel(
      p2pEndpointId: P2PEndpoint.Id,
      channel: ManagedChannel,
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Unit =
    Future {
      logger.info(s"Terminating gRPC channel to $p2pEndpointId")
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
          s"Failed to terminate in ${timeouts.closing.duration} the gRPC channel to $p2pEndpointId"
        )
      } else {
        logger.info(
          s"Successfully terminated gRPC channel to $p2pEndpointId"
        )
      }
    }.discard

  private def signalConnectWorkerToStop(
      p2pEndpointId: P2PEndpoint.Id
  )(implicit traceContext: TraceContext): Unit =
    mutex(this) {
      connectWorkers
        .remove(p2pEndpointId)
        .foreach(_ => logger.info(s"Signalled connect worker to $p2pEndpointId to stop"))
    }

  // Called by the BFT ordering service when receiving a new gRPC streaming connection
  def tryCreateServerSidePeerReceiver(
      inputModule: ModuleRef[BftOrderingMessage],
      peerSender: StreamObserver[BftOrderingMessage],
  )(implicit
      executionContext: ExecutionContext,
      metricsContext: MetricsContext,
      traceContext: TraceContext,
  ): P2PGrpcStreamingReceiver =
    if (!isClosing) {
      logger.info("Creating a peer sender for an incoming connection")
      Try(peerSender.onNext(createConnectionOpener(thisNode))) match {

        case Failure(exception) =>
          logger.info(
            s"Failed to send the connection opener message to sender $peerSender",
            exception,
          )
          // Close the sender and fail accepting the connection
          failGrpcStreamObserver(peerSender, exception, logger)
          throw exception

        case Success(_) =>
          logger.info("Successfully created a peer sender for an incoming connection")
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
                cleanupIncomingConnectionDueToRemoteCompletion(peerSender)
            }
          // A connecting node could omit the peer endpoint when P2P endpoint authentication is disabled,
          //  or send a wrong or different one; in that case, a subsequent send attempt by this node to an endpoint
          //  of that peer won't find the channel and will create a new one in the opposite direction that will
          //  effectively be a duplicate; however, when the sequencer ID of this duplicate connection is received,
          //  it will be detected as duplicate by the connection state and cleaned up.
          //  This also protects against potentially malicious peers that try to establish more than one connection.
          val maybeEndpoint = ServerAuthenticatingServerInterceptor.peerEndpointContextKey.get()
          logger.info(s"Peer endpoint communicated via the server context: $maybeEndpoint")
          // Add the connection to the state asynchronously as soon as a sequencer ID is available
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
          peerReceiver
      }
    } else {
      val msg =
        s"Not creating a P2P gRPC stream receiver for incoming connection with sender $peerSender " +
          "due to shutdown"
      logger.info(msg)
      throw new IllegalStateException(msg)
    }

  private def ensureChannelClosed(
      p2pEndpointId: P2PEndpoint.Id
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Unit = {
    logger.info(s"Cleaning up channel to $p2pEndpointId")
    mutex(this) {
      signalConnectWorkerToStop(p2pEndpointId)
      grpcSequencerClientAuths.remove(p2pEndpointId).foreach(_.close())
      channels.remove(p2pEndpointId)
    }.foreach(shutdownGrpcChannel(p2pEndpointId, _))
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
}

private[bftordering] object P2PGrpcConnectionManager {

  private final case class OpenChannel(
      channel: ManagedChannel,
      // A promise that will be present if authentication is enabled, and in that case it will be completed
      //  by the gRPC client interceptor with the sequencer ID
      maybeSequencerIdFromAuthenticationPromiseUS: Option[
        PromiseUnlessShutdown[SequencerId]
      ],
      asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
  )

  private def toUnitFutureUS[X](optionT: OptionT[FutureUnlessShutdown, X])(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[Unit] =
    optionT.value.map(_ => ())
}
