// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking

import cats.data.OptionT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.{ProcessingTimeout, TlsClientConfig}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder.createChannelBuilder
import com.digitalasset.canton.sequencing.authentication.AuthenticationTokenManagerConfig
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientAuth
import com.digitalasset.canton.synchronizer.sequencer.AuthenticationServices
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.authentication.{
  AddEndpointHeaderClientInterceptor,
  AuthenticateServerClientInterceptor,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.p2p.grpc.GrpcClientHandle
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingServiceGrpc,
  BftOrderingServiceReceiveRequest,
  BftOrderingServiceReceiveResponse,
  PingRequest,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.{SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.stub.{AbstractStub, StreamObserver}
import io.grpc.{Channel, ClientInterceptors, ManagedChannel}
import org.slf4j.event.Level

import java.time.Duration
import java.util.concurrent.{Executor, Executors}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.jdk.CollectionConverters.*
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

final class GrpcNetworking(
    maybeServerUS: Option[UnlessShutdown[LifeCycle.CloseableServer]],
    authenticationInitialState: Option[AuthenticationInitialState],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  import TraceContext.Implicits.Empty.emptyTraceContext

  object clientRole {

    private val connectExecutor = Executors.newCachedThreadPool()
    private val connectExecutionContext = ExecutionContext.fromExecutor(connectExecutor)
    private val connectWorkers = mutable.Map[P2PEndpoint, FutureUnlessShutdown[Unit]]()
    private val serverHandles =
      mutable.Map[P2PEndpoint, ServerHandleInfo[BftOrderingServiceReceiveRequest]]()
    private val channels = mutable.Map[P2PEndpoint, ManagedChannel]()
    private val grpcSequencerClientAuths = mutable.Map[P2PEndpoint, GrpcSequencerClientAuth]()

    // Called by the client network manager when establishing a connection to an endpoint
    def getServerHandleOrStartConnection(
        serverEndpoint: P2PEndpoint
    ): Option[ServerHandleInfo[BftOrderingServiceReceiveRequest]] =
      mutex(this) {
        val res = serverHandles.get(serverEndpoint)
        res.foreach { case ServerHandleInfo(sequencerId, handle, _) =>
          serverHandles.put(
            serverEndpoint,
            ServerHandleInfo(sequencerId, handle, isNewlyConnected = false),
          )
        }
        res
      } match {
        case attemptCompleted @ Some(_) =>
          attemptCompleted
        case _ =>
          ensureConnectWorker(serverEndpoint)
          None
      }

    // Called by:
    // - The sender actor, if it fails to send a message to a node.
    // - The gRPC streaming client endpoint on error (and on completion, but it never occurs).
    // - close().
    def closeConnection(serverEndpoint: P2PEndpoint): Unit = {
      logger.info(s"Closing connection to endpoint in server role $serverEndpoint")
      mutex(this) {
        serverHandles.remove(serverEndpoint).map(_.serverHandle).foreach(completeHandle)
        connectWorkers.remove(serverEndpoint).discard // Signals "stop" to the connect worker
        grpcSequencerClientAuths.remove(serverEndpoint).foreach(_.close())
        channels.remove(serverEndpoint)
      }.foreach(shutdownGrpcChannel(serverEndpoint, _))
    }

    def close(): Unit = {
      logger.debug("Closing P2P networking (client role)")
      logger.debug("Shutting down authenticators")
      grpcSequencerClientAuths.values.foreach(_.close())
      timeouts.closing
        .await(
          "bft-ordering-grpc-networking-state-client-close",
          logFailing = Some(Level.WARN),
        )(Future.sequence(serverHandles.keys.map { serverEndpoint =>
          Future(closeConnection(serverEndpoint))
        }))
        .discard
      logger.debug("Shutting down connection executor")
      connectExecutor.shutdown()
      logger.debug("Closed P2P networking (client role)")
    }

    private def ensureConnectWorker(serverEndpoint: P2PEndpoint): Unit =
      mutex(this) {
        connectWorkers.get(serverEndpoint) match {
          case Some(task) if !task.isCompleted => ()
          case _ =>
            connectWorkers
              .put(
                serverEndpoint,
                connect(serverEndpoint),
              )
              .discard
        }
      }

    private def connect(serverEndpoint: P2PEndpoint): FutureUnlessShutdown[Unit] =
      performUnlessClosingF("p2p-connect") {
        logger.debug(
          s"Creating a gRPC channel and connecting to endpoint in server role $serverEndpoint"
        )
        val OpenChannel(
          channel,
          maybeSequencerIdFromAuthenticationFuture,
          asyncStub,
          blockingStub,
        ) =
          openGrpcChannel(serverEndpoint)

        val serverHandleOptionT =
          createServerHandle(serverEndpoint, channel, asyncStub, blockingStub)
        maybeSequencerIdFromAuthenticationFuture match {
          case Some(sequencerIdFromAuthenticationFuture) =>
            sequencerIdFromAuthenticationFuture.flatMap { sequencerIdFromAuthentication =>
              toUnitFuture(
                serverHandleOptionT
                  .map { case (_, streamFromServer) =>
                    // We don't care about the communicated sequencer ID if authentication is enabled
                    logger.info(
                      s"Successfully connected to endpoint in server role $serverEndpoint " +
                        s"authenticated as sequencer with ID $sequencerIdFromAuthentication"
                    )
                    addServerEndpoint(
                      serverEndpoint,
                      sequencerIdFromAuthentication,
                      streamFromServer,
                    )
                  }
              )
            }
          case _ =>
            toUnitFuture(
              serverHandleOptionT
                .map { case (communicatedSequencerId, streamFromServer) =>
                  logger.info(
                    s"Successfully connected to endpoint in server role $serverEndpoint " +
                      s"claiming to be sequencer with ID $communicatedSequencerId (authentication is disabled)"
                  )
                  addServerEndpoint(serverEndpoint, communicatedSequencerId, streamFromServer)
                }
            )
        }
      }(connectExecutionContext, TraceContext.empty)

    private def addServerEndpoint(
        serverEndpoint: P2PEndpoint,
        sequencerId: SequencerId,
        streamFromServer: StreamObserver[BftOrderingServiceReceiveRequest],
    ): Unit =
      // These streams are unidirectional: two of them (one per direction) are needed for a full-duplex P2P link.
      //  We avoid bidirectional streaming because client TLS certificate authentication is not well-supported
      //  by all network infrastructure, but we still want to be able to authenticate both ends with TLS.
      //  TLS support is however only about transport security; message-level authentication relies on
      //  signatures with keys registered in the Canton topology, that are unrelated to the TLS certificates.
      mutex(this) {
        serverHandles
          .put(
            serverEndpoint,
            ServerHandleInfo(sequencerId, streamFromServer, isNewlyConnected = true),
          )
          .discard
      }

    private def openGrpcChannel(serverEndpoint: P2PEndpoint): OpenChannel = {
      implicit val executor: Executor = (command: Runnable) => executionContext.execute(command)
      val channel = createChannelBuilder(serverEndpoint.endpointConfig).build()
      val maybeGrpcSequencerClientAuthAndServerEndpoint =
        authenticationInitialState.map(auth =>
          new GrpcSequencerClientAuth(
            auth.synchronizerId,
            member = auth.sequencerId,
            crypto = auth.authenticationServices.syncCryptoForAuthentication.crypto,
            channelPerEndpoint =
              NonEmpty(Map, Endpoint(serverEndpoint.address, serverEndpoint.port) -> channel),
            supportedProtocolVersions = Seq(auth.protocolVersion),
            tokenManagerConfig = auth.authTokenConfig,
            clock = auth.clock,
            timeouts = timeouts,
            loggerFactory = loggerFactory,
          ) -> auth.serverEndpoint
        )
      mutex(this) {
        channels.put(serverEndpoint, channel).discard
        maybeGrpcSequencerClientAuthAndServerEndpoint.foreach { case (auth, _) =>
          grpcSequencerClientAuths.put(serverEndpoint, auth).discard
        }
        logger.debug(s"Created gRPC channel to endpoint in server role $serverEndpoint")
      }

      def maybeAuthenticateStub[S <: AbstractStub[S]](stub: S) =
        maybeGrpcSequencerClientAuthAndServerEndpoint.fold(stub) { case (auth, serverEndpoint) =>
          serverEndpoint.fold(stub) { serverEndpoint =>
            auth.apply(
              stub.withInterceptors(
                new AddEndpointHeaderClientInterceptor(serverEndpoint, loggerFactory)
              )
            )
          }
        }

      val (checkedChannel, maybeSequencerIdFromAuthenticationPromise) =
        checkServerAuthentication(channel)

      OpenChannel(
        channel,
        maybeSequencerIdFromAuthenticationPromise.map(_.future),
        maybeAuthenticateStub(BftOrderingServiceGrpc.stub(checkedChannel)),
        maybeAuthenticateStub(BftOrderingServiceGrpc.blockingStub(checkedChannel)),
      )
    }

    private def checkServerAuthentication(
        channel: Channel
    ): (Channel, Option[Promise[SequencerId]]) =
      authenticationInitialState.fold[(Channel, Option[Promise[SequencerId]])](channel -> None) {
        auth =>
          val memberAuthenticationService = auth.authenticationServices.memberAuthenticationService
          val sequencerIdFromAuthenticationPromise = Promise[SequencerId]()
          val interceptor =
            new AuthenticateServerClientInterceptor(
              memberAuthenticationService,
              // Authentication runs on both the ping and the bidi stream,
              //  but we must complete the sequencer ID promise only once.
              onAuthenticationSuccess = sequencerId =>
                if (!sequencerIdFromAuthenticationPromise.isCompleted)
                  sequencerIdFromAuthenticationPromise.success(sequencerId),
              onAuthenticationFailure = throwable =>
                if (!sequencerIdFromAuthenticationPromise.isCompleted)
                  sequencerIdFromAuthenticationPromise.failure(throwable),
              loggerFactory,
            )
          ClientInterceptors.intercept(channel, List(interceptor).asJava) -> Some(
            sequencerIdFromAuthenticationPromise
          )
      }

    @SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
    private def createServerHandle(
        serverEndpoint: P2PEndpoint,
        channel: ManagedChannel,
        asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
        blockingStub: BftOrderingServiceGrpc.BftOrderingServiceBlockingStub,
        connectRetryDelay: NonNegativeFiniteDuration = InitialConnectRetryDelay,
        attemptNumber: Int = 1,
    ): OptionT[Future, (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest])] = {

      def retry(
          failureDescription: String,
          exception: Throwable,
          previousRetryDelay: NonNegativeFiniteDuration,
          attemptNumber: Int,
      ): OptionT[Future, (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest])] = {
        def log(msg: => String, exc: Throwable): Unit =
          if (attemptNumber <= MaxConnectionAttemptsBeforeWarning)
            logger.info(msg, exc)
          else
            logger.warn(msg, exc)
        val retryDelay = MaxConnectRetryDelay.min(previousRetryDelay * NonNegativeInt.tryCreate(2))
        log(
          s"in client role failed to $failureDescription during attempt $attemptNumber, retrying in $retryDelay",
          exception,
        )
        for {
          _ <- OptionT[Future, Unit](
            DelayUtil.delay(retryDelay.toScala).map(Some(_))
          ) // Wait for the retry delay
          result <-
            if (
              !isClosing && mutex(this) {
                connectWorkers.contains(serverEndpoint)
              }
            ) {
              createServerHandle(
                serverEndpoint,
                channel,
                asyncStub,
                blockingStub,
                retryDelay,
                attemptNumber,
              ) // Async-trampolined
            } else {
              logger.info(
                s"in client role failed to $failureDescription during attempt $attemptNumber, " +
                  "but not retrying because the connection is being closed",
                exception,
              )
              OptionT.none[Future, (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest])]
            }
        } yield result
      }

      Try {
        // Unfortunately the async client fails asynchronously, so a synchronous ping comes in useful to check that
        //  at least the initial connection can be established.
        blockingStub.ping(PingRequest.defaultInstance).discard
        val sequencerIdPromise = Promise[SequencerId]()
        val streamFromServer = asyncStub.receive(
          new GrpcClientHandle(
            serverEndpoint,
            sequencerIdPromise,
            closeConnection,
            authenticationEnabled = authenticationInitialState.isDefined,
            loggerFactory,
          )
        )
        sequencerIdPromise.future.map(sequencerId => (sequencerId, streamFromServer))
      } match {
        case Success(futureResult) =>
          OptionT(
            futureResult.transformWith {
              case Success(value) =>
                Future.successful(Option(value))
              case Failure(exception) =>
                retry(
                  s"create a stream to '$serverEndpoint''",
                  exception,
                  connectRetryDelay,
                  attemptNumber + 1,
                ).value
            }
          )
        case Failure(exception) =>
          retry(s"ping endpoint $serverEndpoint", exception, connectRetryDelay, attemptNumber + 1)
      }
    }

    private def shutdownGrpcChannel(
        serverEndpoint: P2PEndpoint,
        channel: ManagedChannel,
    ): Unit = {
      logger.debug(s"Terminating gRPC channel to endpoint in server role $serverEndpoint")
      val terminated =
        channel
          .shutdownNow()
          .awaitTermination(
            timeouts.closing.duration.toMillis,
            java.util.concurrent.TimeUnit.MILLISECONDS,
          )
      if (!terminated) {
        logger.warn(
          s"Failed to terminate in ${timeouts.closing.duration} the gRPC channel to endpoint in server role $serverEndpoint"
        )
      } else {
        logger.info(
          s"Successfully terminated gRPC channel to endpoint in server role $serverEndpoint"
        )
      }
    }
  }

  object serverRole {
    private val clientHandles = mutable.Set[StreamObserver[BftOrderingServiceReceiveResponse]]()

    def startServer(): Unit = maybeServerUS.foreach(_.foreach(_.server.start().discard)).discard

    // Called by the gRPC server when receiving a connection
    def addClientHandle(clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]): Unit =
      mutex(this) {
        clientHandles.add(clientEndpoint).discard
      }

    // Called by the gRPC server endpoint when receiving an error or a completion from a client
    def cleanupClientHandle(
        clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]
    ): Unit = {
      logger.debug("Completing and removing client endpoint")
      completeHandle(clientEndpoint)
      mutex(this) {
        clientHandles.remove(clientEndpoint).discard
      }
    }

    def close(): Unit = {
      logger.debug("Closing P2P networking (server role)")
      clientHandles.foreach(cleanupClientHandle)
      shutdownGrpcServers()
      logger.debug("Closed P2P networking (server role)")
    }

    private def shutdownGrpcServers(): Unit =
      maybeServerUS.foreach(_.foreach { serverHandle =>
        logger.info(s"Shutting down gRPC server")
        shutdownGrpcServer(serverHandle)
      })

    private def shutdownGrpcServer(server: LifeCycle.CloseableServer): Unit = {
      // https://github.com/grpc/grpc-java/issues/8770
      val serverPort = server.server.getPort
      logger.debug(s"Terminating gRPC server on port $serverPort")
      server.close()
      logger.info(s"Successfully terminated the gRPC server on port $serverPort")
    }
  }

  override def onClosed(): Unit = {
    clientRole.close()
    serverRole.close()
  }

  private def completeHandle(endpoint: StreamObserver[?]): Unit =
    try {
      endpoint.onCompleted()
    } catch {
      case NonFatal(_) => () // Already completed
    }

  private def mutex[T](lock: AnyRef)(action: => T): T =
    blocking {
      lock.synchronized {
        action
      }
    }
}

object GrpcNetworking {

  private val MaxConnectionAttemptsBeforeWarning: Int = 10

  private val InitialConnectRetryDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryCreate(Duration.ofMillis(300))

  private val MaxConnectRetryDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryCreate(Duration.ofSeconds(2))

  final case class ServerHandleInfo[P2PMessageT](
      sequencerId: SequencerId,
      serverHandle: StreamObserver[P2PMessageT],
      isNewlyConnected: Boolean,
  )

  /** The BFT orderer's internal representation of a P2P network endpoint.
    *
    * Non-networked tests (unit and simulation tests) use the [[PlainTextP2PEndpoint]] concrete
    * subclass and fill in fake ports.
    *
    * It is possible to generalize further and introduce a symbolic endpoint type for non-networked
    * tests, so that they don't have to deal with networking-only information, but it's not worth
    * the added complexity and required time/maintenance investment, because:
    *
    *   - The endpoint type should become part of the environment definition, and endpoint admin
    *     messages would thus become parametric in the environment.
    *   - Furthermore, endpoint administration is only supported for networked endpoints, so the
    *     network output module would have to be split into a networked and a non-networked part and
    *     networked-only functionality should be tested separately.
    */
  sealed trait P2PEndpoint extends Product {

    def address: String
    def port: Port
    def transportSecurity: Boolean
    def endpointConfig: BftBlockOrdererConfig.P2PEndpointConfig

    final lazy val id: P2PEndpoint.Id = P2PEndpoint.Id(address, port, transportSecurity)
  }

  object P2PEndpoint {

    final case class Id(
        address: String,
        port: Port,
        transportSecurity: Boolean,
    ) extends Ordered[Id]
        with Product
        with PrettyPrinting {

      // Used for metrics
      lazy val url = s"${if (transportSecurity) "https" else "http"}://$address:$port"

      override def compare(that: Id): Int =
        Id.unapply(this).compare(Id.unapply(that))

      override protected def pretty: Pretty[Id] =
        prettyOfClass(param("url", _.url.doubleQuoted), param("tls", _.transportSecurity))
    }

    def fromEndpointConfig(
        config: BftBlockOrdererConfig.P2PEndpointConfig
    ): P2PEndpoint =
      config.tlsConfig match {
        case Some(TlsClientConfig(_, _, enabled)) =>
          if (!enabled)
            PlainTextP2PEndpoint.fromEndpointConfig(config)
          else
            TlsP2PEndpoint.fromEndpointConfig(config)
        case _ =>
          PlainTextP2PEndpoint.fromEndpointConfig(config)
      }
  }

  final case class PlainTextP2PEndpoint(
      override val address: String,
      override val port: Port,
  ) extends P2PEndpoint {

    override val transportSecurity: Boolean = false

    override lazy val endpointConfig: BftBlockOrdererConfig.P2PEndpointConfig =
      BftBlockOrdererConfig.P2PEndpointConfig(
        address,
        port,
        Some(TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)),
      )
  }

  object PlainTextP2PEndpoint {

    private[networking] def fromEndpointConfig(
        config: BftBlockOrdererConfig.P2PEndpointConfig
    ): PlainTextP2PEndpoint =
      PlainTextP2PEndpoint(config.address, config.port)
  }

  final case class TlsP2PEndpoint(
      override val endpointConfig: BftBlockOrdererConfig.P2PEndpointConfig
  ) extends P2PEndpoint {

    override val transportSecurity: Boolean = true

    override val address: String = endpointConfig.address

    override def port: Port = endpointConfig.port
  }

  object TlsP2PEndpoint {

    private[networking] def fromEndpointConfig(
        endpointConfig: BftBlockOrdererConfig.P2PEndpointConfig
    ): TlsP2PEndpoint =
      TlsP2PEndpoint(endpointConfig)
  }

  private[bftordering] final case class AuthenticationInitialState(
      protocolVersion: ProtocolVersion,
      synchronizerId: SynchronizerId,
      sequencerId: SequencerId,
      authenticationServices: AuthenticationServices,
      authTokenConfig: AuthenticationTokenManagerConfig,
      serverEndpoint: Option[P2PEndpoint],
      clock: Clock,
  )

  private final case class OpenChannel(
      channel: ManagedChannel,
      maybeSequencerIdFromAuthenticationFuture: Option[Future[SequencerId]],
      asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
      blockingStub: BftOrderingServiceGrpc.BftOrderingServiceBlockingStub,
  )

  private def toUnitFuture[X](optionT: OptionT[Future, X])(implicit
      ec: ExecutionContext
  ): Future[Unit] =
    optionT.value.map(_ => ())
}
