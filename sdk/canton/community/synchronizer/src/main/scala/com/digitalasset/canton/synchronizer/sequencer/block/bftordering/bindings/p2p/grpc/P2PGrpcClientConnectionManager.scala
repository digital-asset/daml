// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import cats.data.OptionT
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  ModuleRef,
  P2PConnectionEventListener,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.mutex
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

import java.time.{Duration, Instant}
import java.util.concurrent.{Executor, Executors}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.{Failure, Success, Try}

private[bftordering] final class P2PGrpcConnectionManager(
    thisNode: BftNodeId,
    // None if authentication is disabled
    authenticationInitialState: Option[AuthenticationInitialState],
    p2pConnectionEventListener: P2PConnectionEventListener,
    metrics: BftOrderingMetrics,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable { self =>

  import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcConnectionManager.*

  private val isAuthenticationEnabled: Boolean = authenticationInitialState.isDefined

  private val connectExecutor = Executors.newCachedThreadPool()
  private val connectExecutionContext = ExecutionContext.fromExecutor(connectExecutor)
  private val connectWorkers =
    mutable.Map[P2PEndpoint.Id, (ManagedChannel, FutureUnlessShutdown[Unit])]()
  private val peerSenders =
    mutable.Map[P2PEndpoint.Id, StreamObserver[BftOrderingMessage]]()
  private val serverPeerSenders = mutable.Set[StreamObserver[BftOrderingMessage]]()
  private val channels = mutable.Map[P2PEndpoint.Id, ManagedChannel]()
  private val grpcSequencerClientAuths = mutable.Map[P2PEndpoint.Id, GrpcSequencerClientAuth]()

  // Called by the client network manager when establishing a connection to an endpoint
  def getPeerSenderOrStartConnection(
      p2pEndpoint: P2PEndpoint
  )(implicit traceContext: TraceContext): Option[StreamObserver[BftOrderingMessage]] = {
    val p2pEndpointId = p2pEndpoint.id
    if (!isClosing) {
      mutex(this)(peerSenders.get(p2pEndpointId)) match {
        case found @ Some(_) =>
          // A sender may be present due to an incoming connection, so we need to stop trying to connect
          logger.debug(s"Found existing sender for $p2pEndpointId")
          signalConnectWorkerToStop(p2pEndpointId)
          found
        case _ =>
          logger.debug(s"No sender found for $p2pEndpointId")
          ensureConnectWorker(p2pEndpoint)
          None
      }
    } else {
      logger.debug(
        s"P2P gRPC connection manager not providing a sender to '$p2pEndpointId' due to shutdown"
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
    mutex(this) {
      peerSenders.remove(p2pEndpointId).foreach { peerSender =>
        logger.debug(
          s"Sender $peerSender to $p2pEndpointId found, notifying disconnection and closing it"
        )
        p2pConnectionEventListener.onDisconnect(p2pEndpointId)
        completeGrpcStreamObserver(peerSender)
      }
      signalConnectWorkerToStop(p2pEndpointId)
      logger.debug(s"Closing gRPC sequencer client auth to $p2pEndpointId")
      grpcSequencerClientAuths.remove(p2pEndpointId).foreach(_.close())
      logger.debug(s"Removing and closing gRPC channel to $p2pEndpointId")
      channels.remove(p2pEndpointId)
    }.foreach(shutdownGrpcChannel(p2pEndpointId, _))
  }

  override def onClosed(): Unit = {
    import TraceContext.Implicits.Empty.*
    logger.debug("Closing P2P gRPC client connection manager")
    logger.debug("Shutting down authenticators")
    // We cannot lock threads across a potentially long await, so we use finer-grained locks
    mutex(this)(grpcSequencerClientAuths.values.toSeq).foreach(_.close())
    timeouts.closing
      .await(
        "bft-ordering-grpc-networking-state-client-close",
        logFailing = Some(Level.WARN),
      )(Future.sequence(mutex(this)(peerSenders.keys.toSeq).map { peerEndpoint =>
        Future(closeConnection(peerEndpoint))
      }))
      .discard
    logger.debug("Shutting down gRPC channels")
    mutex(this)(channels.values.toSeq).foreach(_.shutdown().discard)
    logger.debug("Shutting down connection executor")
    connectExecutor.shutdown()
    logger.debug("Cleaning up server peer senders")
    serverPeerSenders.foreach(cleanupServerPeerSender)
    logger.debug("Closed P2P networking (client role)")
  }

  private def ensureConnectWorker(
      p2pEndpoint: P2PEndpoint
  )(implicit traceContext: TraceContext): Unit = {
    val p2pEndpointId = p2pEndpoint.id
    mutex(this)(connectWorkers.get(p2pEndpointId)) match {
      case Some(_ -> task) if !task.isCompleted =>
        logger.debug(s"Connection worker for $p2pEndpointId: is already running")
      case _ =>
        logger.debug(s"Starting connection worker for $p2pEndpointId:")
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
      logger.debug(
        s"Creating a gRPC channel and connecting to $p2pEndpointId"
      )
      openGrpcChannel(p2pEndpoint) match {
        case OpenChannel(
              channel,
              maybeSequencerIdFromAuthenticationPromiseUS,
              asyncStub,
            ) =>
          val sequencerIdUS =
            maybeSequencerIdFromAuthenticationPromiseUS.getOrElse(
              // Authentication is disabled, the peer receiver will use the first message's sentBy to backfill
              PromiseUnlessShutdown.unsupervised[SequencerId]()
            )
          val peerSenderOT =
            createPeerSender(p2pEndpoint, channel, asyncStub, sequencerIdUS)
          // Add the connection to the state asynchronously as soon as a sequencer ID is available
          Some(channel -> sequencerIdUS.futureUS.flatMap { sequencerId =>
            toUnitFutureUS(
              peerSenderOT
                .map { case (_, peerSender) =>
                  // We don't care about the communicated sequencer ID if authentication is enabled
                  logger.info(
                    s"Successfully connected to $p2pEndpointId " +
                      s"authenticated as ${sequencerId.toProtoPrimitive}"
                  )
                  addPeerEndpoint(
                    p2pEndpoint.id,
                    sequencerId,
                    peerSender,
                  )
                }
            )
          })
      }
    } else {
      logger.info(
        s"Not attempting to create a gRPC channel and connect to $p2pEndpointId due to shutdown"
      )
      None
    }
  }

  private def addPeerEndpoint(
      p2pEndpointId: P2PEndpoint.Id,
      sequencerId: SequencerId,
      peerSender: StreamObserver[BftOrderingMessage],
  )(implicit traceContext: TraceContext): Unit = {
    p2pConnectionEventListener.onConnect(p2pEndpointId)
    // These streams are unidirectional: two of them (one per direction) are needed for a full-duplex P2P link.
    //  We avoid bidirectional streaming because client TLS certificate authentication is not well-supported
    //  by all network infrastructure, but we still want to be able to authenticate both ends with TLS.
    //  TLS support is however only about transport security; message-level authentication relies on
    //  signatures with keys registered in the Canton topology, that are unrelated to the TLS certificates.
    mutex(this) {
      peerSenders.put(p2pEndpointId, peerSender).discard
    }
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
        ) ->
          auth.serverToClientAuthenticationEndpoint
      )
    mutex(this) {
      val p2PEndpointId = p2pEndpoint.id
      channels.put(p2PEndpointId, channel).discard
      maybeAuthenticationContext.foreach { case (grpcSequencerClientAuth, _) =>
        grpcSequencerClientAuths.put(p2PEndpointId, grpcSequencerClientAuth).discard
      }
      logger.debug(s"Created gRPC channel to endpoint in server role $p2pEndpoint")
    }

    def maybeAuthenticateStub[S <: AbstractStub[S]](stub: S) =
      maybeAuthenticationContext.fold(stub) {
        case (auth, maybeServerToClientAuthenticationEndpoint) =>
          maybeServerToClientAuthenticationEndpoint.fold(stub) {
            serverToClientAuthenticationEndpoint =>
              auth.apply(
                stub.withInterceptors(
                  new AddEndpointHeaderClientInterceptor(
                    serverToClientAuthenticationEndpoint,
                    loggerFactory,
                  )
                )
              )
          }
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
      connectRetryDelay: NonNegativeFiniteDuration = InitialConnectRetryDelay,
      attemptNumber: Int = 1,
  )(implicit traceContext: TraceContext): OptionT[
    FutureUnlessShutdown,
    (SequencerId, StreamObserver[BftOrderingMessage]),
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
        (SequencerId, StreamObserver[BftOrderingMessage]),
      ] = {
        def log(msg: => String, exc: Throwable): Unit =
          if (attemptNumber <= MaxConnectionAttemptsBeforeWarning)
            logger.info(msg, exc)
          else
            logger.warn(msg, exc)
        val retryDelay =
          MaxConnectRetryDelay.min(previousRetryDelay * NonNegativeInt.tryCreate(2))
        log(
          s"failed to $failedOperationName during attempt $attemptNumber, retrying in $retryDelay",
          exception,
        )
        for {
          _ <- OptionT[FutureUnlessShutdown, Unit](
            DelayUtil.delayIfNotClosing("grpc-networking", retryDelay.toScala, self).map(Some(_))
          ) // Wait for the retry delay
          result <-
            if (mutex(this)(connectWorkers.get(p2pEndpointId).exists(_._1 == channel))) {
              createPeerSender(
                p2pEndpoint,
                channel,
                asyncStub,
                sequencerIdPromiseUS,
                retryDelay,
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
                  (SequencerId, StreamObserver[BftOrderingMessage]),
                ]
            }
        } yield result
      }

      Try {
        val peerReceiver =
          new P2PGrpcStreamingReceiver(
            p2pEndpointId,
            sequencerIdPromiseUS,
            isAuthenticationEnabled,
            closeConnection,
            loggerFactory,
          )
        val peerSender = asyncStub.receive(peerReceiver)
        logger.debug(s"Sending connection opener to $p2pEndpointId")
        // Send a connection opener to preemptively check the connection
        peerSender.onNext(createConnectionOpener(thisNode))
        sequencerIdPromiseUS.futureUS.map(sequencerId => (sequencerId, peerSender))
      } match {
        case Success(futureUSResult) =>
          OptionT(
            futureUSResult.transformWith {
              case Success(value) =>
                FutureUnlessShutdown.lift(value.map(Option(_)))
              case Failure(exception) =>
                retry(
                  s"create a stream to '$p2pEndpoint'",
                  exception,
                  connectRetryDelay,
                  attemptNumber + 1,
                ).value
            }
          )
        case Failure(exception) =>
          retry(s"ping endpoint $p2pEndpoint", exception, connectRetryDelay, attemptNumber + 1)
      }
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
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Terminating gRPC channel to endpoint in server role $p2pEndpointId")
    val terminated =
      channel
        .shutdownNow()
        .awaitTermination(
          timeouts.closing.duration.toMillis,
          java.util.concurrent.TimeUnit.MILLISECONDS,
        )
    if (!terminated) {
      logger.warn(
        s"Failed to terminate in ${timeouts.closing.duration} the gRPC channel to endpoint in server role $p2pEndpointId"
      )
    } else {
      logger.info(
        s"Successfully terminated gRPC channel to endpoint in server role $p2pEndpointId"
      )
    }
  }

  // Must be called in a `mutex(this)` block
  private def signalConnectWorkerToStop(
      p2pEndpointId: P2PEndpoint.Id
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(s"Signalling connect worker to $p2pEndpointId to stop")
    connectWorkers.remove(p2pEndpointId).discard
  }

  def tryCreateServerSidePeerReceiver(
      inputModule: ModuleRef[BftOrderingMessage],
      peerSender: StreamObserver[BftOrderingMessage],
  )(implicit
      traceContext: TraceContext,
      metricsContext: MetricsContext,
  ): P2PGrpcStreamingServerSideReceiver =
    if (!isClosing) {
      addServerPeerSender(peerSender)
      logger.debug("Creating a peer sender for an incoming connection")
      Try(peerSender.onNext(createConnectionOpener(thisNode))) match {
        case Failure(exception) =>
          logger.debug(
            s"Failed to send the connection opener message to sender $peerSender",
            exception,
          )
          peerSender.onError(exception) // Required by the gRPC streaming API
          throw exception
        case Success(value) =>
          logger.debug("Successfully created a peer sender for an incoming connection")
          new P2PGrpcStreamingServerSideReceiver(
            inputModule,
            peerSender,
            cleanupServerPeerSender,
            loggerFactory,
            metrics,
          )
      }
    } else {
      val msg =
        s"Not creating a P2P gRPC stream receiver for incoming connection with sender $peerSender " +
          "due to shutdown"
      logger.info(msg)
      throw new IllegalStateException(msg)
    }

  // Called by the gRPC server when receiving a connection
  private def addServerPeerSender(peerSender: StreamObserver[BftOrderingMessage]): Unit =
    mutex(this) {
      serverPeerSenders.add(peerSender).discard
    }

  // Called by the gRPC server endpoint when receiving an error or a completion from a client
  private def cleanupServerPeerSender(
      peerSender: StreamObserver[BftOrderingMessage]
  )(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Completing and removing peer sender $peerSender")
    completeGrpcStreamObserver(peerSender)
    mutex(this) {
      serverPeerSenders.remove(peerSender).discard
    }
  }
}

private[bftordering] object P2PGrpcConnectionManager {

  // The maximum number of connection attempts before we log a warning.
  //  Together with the retry delays, it limits the maximum time spent trying to connect to a peer before
  //  failure is logged at as a warning.
  //  This time must be long enough to allow the sequencer to start up and shut down gracefully.
  private val MaxConnectionAttemptsBeforeWarning = 30

  private val InitialConnectRetryDelay =
    NonNegativeFiniteDuration.tryCreate(Duration.ofMillis(300))

  private val MaxConnectRetryDelay =
    NonNegativeFiniteDuration.tryCreate(Duration.ofSeconds(2))

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
