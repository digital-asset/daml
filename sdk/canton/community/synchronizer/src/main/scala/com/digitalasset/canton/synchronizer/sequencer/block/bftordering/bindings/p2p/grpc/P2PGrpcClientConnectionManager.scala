// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import cats.data.OptionT
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.canton.topology.SequencerNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcClientConnectionManager.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.{
  AuthenticationInitialState,
  P2PEndpoint,
  completeGrpcStreamObserver,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication.{
  AddEndpointHeaderClientInterceptor,
  AuthenticateServerClientInterceptor,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.P2PConnectionEventListener
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.mutex
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingMessageBody,
  BftOrderingServiceGrpc,
  BftOrderingServiceReceiveRequest,
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

class P2PGrpcClientConnectionManager(
    thisNode: BftNodeId,
    // None if authentication is disabled
    authenticationInitialState: Option[AuthenticationInitialState],
    p2pConnectionEventListener: P2PConnectionEventListener,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable { self =>

  import TraceContext.Implicits.Empty.emptyTraceContext

  private val isAuthenticationEnabled: Boolean = authenticationInitialState.isDefined

  private val connectExecutor = Executors.newCachedThreadPool()
  private val connectExecutionContext = ExecutionContext.fromExecutor(connectExecutor)
  private val connectWorkers =
    mutable.Map[P2PEndpoint, (ManagedChannel, FutureUnlessShutdown[Unit])]()
  private val peerSenders =
    mutable.Map[P2PEndpoint, StreamObserver[BftOrderingServiceReceiveRequest]]()
  private val channels = mutable.Map[P2PEndpoint, ManagedChannel]()
  private val grpcSequencerClientAuths = mutable.Map[P2PEndpoint, GrpcSequencerClientAuth]()

  // Called by the client network manager when establishing a connection to an endpoint
  def getPeerSenderOrStartConnection(
      peerEndpoint: P2PEndpoint
  ): Option[StreamObserver[BftOrderingServiceReceiveRequest]] =
    if (!isClosing) {
      mutex(this)(peerSenders.get(peerEndpoint)).orElse {
        ensureConnectWorker(peerEndpoint)
        None
      }
    } else {
      logger.debug(
        s"P2P gRPC connection manager not providing a sender to '${peerEndpoint.id}' due to shutdown"
      )
      None
    }

  // Called by:
  // - The sender actor, if it fails to send a message to a node.
  // - The gRPC streaming client endpoint on error (and on completion, but it never occurs).
  // - close().
  def closeConnection(peerEndpoint: P2PEndpoint): Unit = {
    logger.info(s"Closing connection to ${peerEndpoint.id}")
    mutex(this) {
      peerSenders.remove(peerEndpoint).foreach { handle =>
        p2pConnectionEventListener.onDisconnect(peerEndpoint.id)
        completeGrpcStreamObserver(handle)
      }
      signalConnectWorkerToStop(peerEndpoint)
      grpcSequencerClientAuths.remove(peerEndpoint).foreach(_.close())
      channels.remove(peerEndpoint)
    }.foreach(shutdownGrpcChannel(peerEndpoint, _))
  }

  override def onClosed(): Unit = {
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
    logger.debug("Closed P2P networking (client role)")
  }

  private def ensureConnectWorker(peerEndpoint: P2PEndpoint): Unit =
    mutex(this)(connectWorkers.get(peerEndpoint)) match {
      case Some(_ -> task) if !task.isCompleted => ()
      case _ =>
        connect(peerEndpoint)(connectExecutionContext)
          .foreach(channelAndWorker =>
            mutex(this)(connectWorkers.put(peerEndpoint, channelAndWorker)).discard
          )
    }

  private def connect(
      peerEndpoint: P2PEndpoint
  )(implicit
      executionContext: ExecutionContext
  ): Option[(ManagedChannel, FutureUnlessShutdown[Unit])] = {
    val endpointId = peerEndpoint.id
    if (!isClosing) {
      logger.debug(
        s"Creating a gRPC channel and connecting to $endpointId"
      )
      openGrpcChannel(peerEndpoint) match {
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
            createPeerSender(peerEndpoint, channel, asyncStub, sequencerIdUS)
          // Add the connection to the state asynchronously as soon as a sequencer ID is available
          Some(channel -> sequencerIdUS.futureUS.flatMap { sequencerId =>
            toUnitFutureUS(
              peerSenderOT
                .map { case (_, peerSender) =>
                  // We don't care about the communicated sequencer ID if authentication is enabled
                  logger.info(
                    s"Successfully connected to $endpointId " +
                      s"authenticated as ${sequencerId.toProtoPrimitive}"
                  )
                  addPeerEndpoint(
                    peerEndpoint,
                    sequencerId,
                    peerSender,
                  )
                }
            )
          })
      }
    } else {
      logger.info(
        s"Not attempting to create a gRPC channel and connect to $endpointId due to shutdown"
      )
      None
    }
  }

  private def addPeerEndpoint(
      peerEndpoint: P2PEndpoint,
      sequencerId: SequencerId,
      peerSender: StreamObserver[BftOrderingServiceReceiveRequest],
  ): Unit = {
    p2pConnectionEventListener.onConnect(peerEndpoint.id)
    // These streams are unidirectional: two of them (one per direction) are needed for a full-duplex P2P link.
    //  We avoid bidirectional streaming because client TLS certificate authentication is not well-supported
    //  by all network infrastructure, but we still want to be able to authenticate both ends with TLS.
    //  TLS support is however only about transport security; message-level authentication relies on
    //  signatures with keys registered in the Canton topology, that are unrelated to the TLS certificates.
    mutex(this) {
      peerSenders.put(peerEndpoint, peerSender).discard
    }
    p2pConnectionEventListener.onConnect(peerEndpoint.id)
    p2pConnectionEventListener.onSequencerId(
      peerEndpoint.id,
      SequencerNodeId.toBftNodeId(sequencerId),
    )
  }

  private def openGrpcChannel(peerEndpoint: P2PEndpoint): OpenChannel = {
    implicit val executor: Executor = (command: Runnable) => executionContext.execute(command)
    val channel = createChannelBuilder(peerEndpoint.endpointConfig).build()
    val maybeAuthenticationContext =
      authenticationInitialState.map(auth =>
        new GrpcSequencerClientAuth(
          auth.psId,
          member = auth.sequencerId,
          crypto = auth.authenticationServices.syncCryptoForAuthentication.crypto,
          channelPerEndpoint =
            NonEmpty(Map, Endpoint(peerEndpoint.address, peerEndpoint.port) -> channel),
          supportedProtocolVersions = Seq(auth.psId.protocolVersion),
          tokenManagerConfig = auth.authTokenConfig,
          clock = auth.clock,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        ) ->
          auth.serverToClientAuthenticationEndpoint
      )
    mutex(this) {
      channels.put(peerEndpoint, channel).discard
      maybeAuthenticationContext.foreach { case (grpcSequencerClientAuth, _) =>
        grpcSequencerClientAuths.put(peerEndpoint, grpcSequencerClientAuth).discard
      }
      logger.debug(s"Created gRPC channel to endpoint in server role $peerEndpoint")
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

    val (checkedChannel, maybeSequencerIdFromAuthenticationPromiseUS) =
      maybeApplyServerAuthenticatingClientInterceptor(channel)

    OpenChannel(
      channel,
      maybeSequencerIdFromAuthenticationPromiseUS,
      maybeAuthenticateStub(BftOrderingServiceGrpc.stub(checkedChannel)),
    )
  }

  // Returns a tuple of the channel, with the server-authenticating gRPC client interceptor applied if authentication is enabled,
  //  and in that case also a promise that is completed with the sequencer ID by the gRPC client interceptor
  //  before the call is activated.
  //  NOTE: we should research again a less convoluted mechanism to retrieve the sequencer ID from the gRPC client
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
            // Authentication runs on both the ping and the bidi stream,
            //  but we must complete the sequencer ID promise only once.
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
      peerEndpoint: P2PEndpoint,
      channel: ManagedChannel,
      asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
      sequencerIdPromiseUS: PromiseUnlessShutdown[SequencerId],
      connectRetryDelay: NonNegativeFiniteDuration = InitialConnectRetryDelay,
      attemptNumber: Int = 1,
  ): OptionT[
    FutureUnlessShutdown,
    (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest]),
  ] =
    synchronizeWithClosing("p2p-create-peer-sender") {
      def retry(
          failureDescription: String,
          exception: Throwable,
          previousRetryDelay: NonNegativeFiniteDuration,
          attemptNumber: Int,
      ): OptionT[
        FutureUnlessShutdown,
        (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest]),
      ] = {
        def log(msg: => String, exc: Throwable): Unit =
          if (attemptNumber <= MaxConnectionAttemptsBeforeWarning)
            logger.info(msg, exc)
          else
            logger.warn(msg, exc)
        val retryDelay =
          MaxConnectRetryDelay.min(previousRetryDelay * NonNegativeInt.tryCreate(2))
        log(
          s"in client role failed to $failureDescription during attempt $attemptNumber, retrying in $retryDelay",
          exception,
        )
        for {
          _ <- OptionT[FutureUnlessShutdown, Unit](
            DelayUtil.delayIfNotClosing("grpc-networking", retryDelay.toScala, self).map(Some(_))
          ) // Wait for the retry delay
          result <-
            if (mutex(this)(connectWorkers.get(peerEndpoint).exists(_._1 == channel))) {
              createPeerSender(
                peerEndpoint,
                channel,
                asyncStub,
                sequencerIdPromiseUS,
                retryDelay,
                attemptNumber,
              ) // Async-trampolined
            } else {
              logger.info(
                s"in client role failed to $failureDescription during attempt $attemptNumber, " +
                  "but not retrying because the connection is being closed",
                exception,
              )
              OptionT
                .none[
                  FutureUnlessShutdown,
                  (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest]),
                ]
            }
        } yield result
      }

      Try {
        val peerReceiver =
          new P2PGrpcStreamingClientSideReceiver(
            peerEndpoint,
            sequencerIdPromiseUS,
            isAuthenticationEnabled,
            closeConnection,
            loggerFactory,
          )
        val peerSender = asyncStub.receive(peerReceiver)
        logger.debug(s"Sending connection opener to ${peerEndpoint.id}")
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
                  s"create a stream to '$peerEndpoint'",
                  exception,
                  connectRetryDelay,
                  attemptNumber + 1,
                ).value
            }
          )
        case Failure(exception) =>
          retry(s"ping endpoint $peerEndpoint", exception, connectRetryDelay, attemptNumber + 1)
      }
    }

  private def createConnectionOpener(
      thisNode: BftNodeId
  ): BftOrderingServiceReceiveRequest = {
    val networkSendInstant = Instant.now()
    BftOrderingServiceReceiveRequest(
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
      peerEndpoint: P2PEndpoint,
      channel: ManagedChannel,
  ): Unit = {
    logger.debug(s"Terminating gRPC channel to endpoint in server role $peerEndpoint")
    val terminated =
      channel
        .shutdownNow()
        .awaitTermination(
          timeouts.closing.duration.toMillis,
          java.util.concurrent.TimeUnit.MILLISECONDS,
        )
    if (!terminated) {
      logger.warn(
        s"Failed to terminate in ${timeouts.closing.duration} the gRPC channel to endpoint in server role $peerEndpoint"
      )
    } else {
      logger.info(
        s"Successfully terminated gRPC channel to endpoint in server role $peerEndpoint"
      )
    }
  }

  // Must be called from within a mutex(this) block
  private def signalConnectWorkerToStop(peerEndpoint: P2PEndpoint): Unit =
    connectWorkers.remove(peerEndpoint).discard
}

private[bftordering] object P2PGrpcClientConnectionManager {

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
