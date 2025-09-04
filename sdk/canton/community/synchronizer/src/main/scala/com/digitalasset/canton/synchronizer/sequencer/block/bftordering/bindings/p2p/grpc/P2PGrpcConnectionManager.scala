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
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication.{
  AddEndpointHeaderClientInterceptor,
  AuthenticateServerClientInterceptor,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.P2PConnectionManagementConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
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
      p2pEndpoint: P2PEndpoint
  )(implicit traceContext: TraceContext): Option[StreamObserver[BftOrderingMessage]] = {
    val p2pEndpointId = p2pEndpoint.id
    if (!isClosing) {
      p2pGrpcConnectionState.getPeerSender(p2pEndpointId) match {
        case found @ Some(_) =>
          // A sender may be present due to an incoming connection, so we need to stop trying to connect
          logger.debug(s"Found existing sender for $p2pEndpointId")
          signalConnectWorkerToStop(p2pEndpointId)
          found
        case _ =>
          logger.debug(
            s"Requested a send but no sender found for $p2pEndpointId, ensuring connection worker is running"
          )
          ensureConnectWorker(p2pEndpoint)
          None
      }
    } else {
      logger.debug(
        s"P2P gRPC connection manager not providing a sender for $p2pEndpointId due to shutdown"
      )
      None
    }
  }

  // Called by:
  // - The sender actor, if it fails to send a message to a node.
  // - The gRPC streaming client endpoint on error (and on completion, but it never occurs).
  // - close().
  def closeConnection(p2pEndpointId: P2PEndpoint.Id)(implicit traceContext: TraceContext): Unit = {
    logger.info(s"Closing connection to $p2pEndpointId")
    p2pGrpcConnectionState.removeClientPeerSender(p2pEndpointId).foreach { peerSender =>
      logger.info(
        s"Sender $peerSender to $p2pEndpointId found, notifying disconnection and closing it"
      )
      p2pConnectionEventListener.onDisconnect(p2pEndpointId)
      completeGrpcStreamObserver(peerSender)
    }
    signalConnectWorkerToStop(p2pEndpointId)
    mutex(this) {
      logger.info(s"Closing gRPC sequencer client auth to $p2pEndpointId")
      grpcSequencerClientAuths.remove(p2pEndpointId).foreach(_.close())
      logger.info(s"Removing and closing gRPC channel to $p2pEndpointId")
      channels.remove(p2pEndpointId)
    }.foreach(shutdownGrpcChannel(p2pEndpointId, _)(connectExecutionContext, traceContext))
  }

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
      )(asyncCloseClientConnectionState())
      .discard
    logger.info("Shutting down gRPC channels")
    mutex(this)(channels.values.toSeq).foreach(_.shutdown().discard)
    logger.info("Shutting down connection executor")
    connectExecutor.shutdown()
    logger.info("Cleaning up server peer senders")
    p2pGrpcConnectionState.cleanupServerPeerSenders(cleanupServerPeerSender)
    logger.info("Closed P2P gRPC connection manager")
  }

  private def asyncCloseClientConnectionState()(implicit traceContext: TraceContext) =
    Future.sequence(
      p2pGrpcConnectionState.knownP2PEndpointIds.map(p2pEndpointId =>
        Future(closeConnection(p2pEndpointId))
      )
    )

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
              asyncAddPeerEndpointOnAuthenticationCompletion(
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

  private def asyncAddPeerEndpointOnAuthenticationCompletion(
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
              addPeerEndpoint(
                sequencerId,
                peerSender,
                p2pEndpoint.id,
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
          closeConnection(p2pEndpointId)
          f
        case s: Success[_] => s
      }
  }

  private def addPeerEndpoint(
      sequencerId: SequencerId,
      peerSender: StreamObserver[BftOrderingMessage],
      p2pEndpointId: P2PEndpoint.Id,
  )(implicit traceContext: TraceContext): Unit = {
    p2pConnectionEventListener.onConnect(p2pEndpointId)
    // These streams are unidirectional: two of them (one per direction) are needed for a full-duplex P2P link.
    //  We avoid bidirectional streaming because client TLS certificate authentication is not well-supported
    //  by all network infrastructure, but we still want to be able to authenticate both ends with TLS.
    //  TLS support is however only about transport security; message-level authentication relies on
    //  signatures with keys registered in the Canton topology, that are unrelated to the TLS certificates.

    p2pGrpcConnectionState.setClientPeerSender(p2pEndpointId, peerSender).discard
    p2pConnectionEventListener.onConnect(p2pEndpointId)
    p2pConnectionEventListener.onSequencerId(
      p2pEndpointId,
      SequencerNodeId.toBftNodeId(sequencerId),
    )
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
    //  the server peer could use it to deduplicate connections:
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
            closeConnection(p2pEndpointId)
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
              logger.info(
                s"Stream to $p2pEndpointId created successfully, " +
                  "sending connection opener to preemptively check the connection"
              )

              Try(peerSender.onNext(createConnectionOpener(thisNode))) match {

                case Failure(exception) =>
                  // Close the connection by failing the sender; no need to close the receiver as it will be
                  //  uninstalled by closing the connection and no state has been updated yet.
                  peerSender.onError(exception)
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
                        peerSender.onError(exception)
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
      metricsContext: MetricsContext,
      traceContext: TraceContext,
  ): P2PGrpcStreamingReceiver =
    if (!isClosing) {
      addServerPeerSender(peerSender)
      logger.info("Creating a peer sender for an incoming connection")
      Try(peerSender.onNext(createConnectionOpener(thisNode))) match {

        case Failure(exception) =>
          logger.info(
            s"Failed to send the connection opener message to sender $peerSender",
            exception,
          )
          // Close the sender and fail accepting the connection
          peerSender.onError(exception)
          throw exception

        case Success(_) =>
          logger.info("Successfully created a peer sender for an incoming connection")
          val sequencerIdPromiseUS = PromiseUnlessShutdown.unsupervised[SequencerId]()
          if (isAuthenticationEnabled)
            extractSequencerIdFromGrpcContextInto(sequencerIdPromiseUS)
          new P2PGrpcStreamingReceiver(
            maybeP2PEndpointId = None, // No P2PEndpoint for incoming connections
            inputModule,
            sequencerIdPromiseUS,
            isAuthenticationEnabled,
            metrics,
            loggerFactory,
          ) {
            override def shutdown(): Unit =
              cleanupServerPeerSender(peerSender)
          }
      }
    } else {
      val msg =
        s"Not creating a P2P gRPC stream receiver for incoming connection with sender $peerSender " +
          "due to shutdown"
      logger.info(msg)
      throw new IllegalStateException(msg)
    }

  private def addServerPeerSender(peerSender: StreamObserver[BftOrderingMessage]): Unit =
    mutex(this) {
      p2pGrpcConnectionState.addServerPeerSender(peerSender).discard
    }

  // Called by the gRPC server endpoint when receiving an error or a completion from a client
  private def cleanupServerPeerSender(
      peerSender: StreamObserver[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(s"Completing and removing peer sender $peerSender")
    completeGrpcStreamObserver(peerSender)
    p2pGrpcConnectionState.removeServerPeerSender(peerSender).discard
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
