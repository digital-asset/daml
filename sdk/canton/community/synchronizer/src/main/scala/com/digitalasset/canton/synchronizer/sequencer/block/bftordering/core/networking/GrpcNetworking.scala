// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking

import cats.data.OptionT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, Port}
import com.digitalasset.canton.config.{ProcessingTimeout, TlsClientConfig}
import com.digitalasset.canton.crypto.SynchronizerCryptoClient
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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrderer.P2PEndpointConfig
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  InitialConnectRetryDelay,
  MaxConnectRetryDelay,
  MaxConnectionAttemptsBeforeWarning,
  P2PEndpoint,
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
import io.grpc.ManagedChannel
import io.grpc.stub.{AbstractStub, StreamObserver}
import org.slf4j.event.Level

import java.time.Duration
import java.util.concurrent.{Executor, Executors}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

final class GrpcNetworking(
    servers: List[UnlessShutdown[LifeCycle.CloseableServer]],
    authentication: Option[GrpcNetworking.Authentication],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  import TraceContext.Implicits.Empty.emptyTraceContext

  object clientRole {

    private val connectExecutor = Executors.newCachedThreadPool()
    private val connectExecutionContext = ExecutionContext.fromExecutor(connectExecutor)
    private val connectWorkers =
      mutable.Map[P2PEndpoint, FutureUnlessShutdown[Unit]]()
    private val serverEndpoints =
      mutable.Map[
        P2PEndpoint,
        (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest]),
      ]()
    private val channels = mutable.Map[P2PEndpoint, ManagedChannel]()

    // Called by the client network manager when establishing a connection to a peer
    def getServerHandleOrStartConnection(
        serverPeer: P2PEndpoint
    ): Option[(SequencerId, StreamObserver[BftOrderingServiceReceiveRequest])] =
      mutex(this) {
        serverEndpoints.get(serverPeer)
      } match {
        case attemptCompleted @ Some(_) =>
          attemptCompleted
        case _ =>
          ensureConnectWorker(serverPeer)
          None
      }

    // Called by:
    // - The sender actor, if it fails to send a message to a peer.
    // - The gRPC streaming client endpoint on error (and on completion, but it never occurs).
    // - close().
    def closeConnection(serverPeer: P2PEndpoint): Unit = {
      logger.info(s"Closing connection to peer in server role $serverPeer")
      mutex(this) {
        serverEndpoints.remove(serverPeer).map(_._2).foreach(completeHandle)
        val _ = connectWorkers.remove(serverPeer) // Signals "stop" to the connect worker
        channels.remove(serverPeer)
      }.foreach(shutdownGrpcChannel(serverPeer, _))
    }

    def close(): Unit = {
      logger.debug("Closing P2P networking (client role)")
      val _ = timeouts.closing.await(
        "bft-ordering-grpc-networking-state-client-close",
        logFailing = Some(Level.WARN),
      )(Future.sequence(serverEndpoints.keys.map { serverPeer =>
        Future(closeConnection(serverPeer))
      }))
      logger.debug("Shutting down connection executor")
      connectExecutor.shutdown()
      logger.debug("Closed P2P networking (client role)")
    }

    private def ensureConnectWorker(serverPeer: P2PEndpoint): Unit =
      mutex(this) {
        connectWorkers.get(serverPeer) match {
          case Some(task) if !task.isCompleted => ()
          case _ =>
            val _ = connectWorkers.put(
              serverPeer,
              connect(serverPeer),
            )
        }
      }

    private def connect(serverPeer: P2PEndpoint): FutureUnlessShutdown[Unit] =
      performUnlessClosingF("p2p-connect") {
        logger.debug(s"Creating a gRPC channel and connecting to peer in server role $serverPeer")
        val (channel, asyncStub, blockingStub) = openGrpcChannel(serverPeer)
        createServerHandle(serverPeer, channel, asyncStub, blockingStub)
          .map { case (sequencerId, streamFromServer) =>
            logger.info(
              s"Successfully connected to peer in server role $serverPeer with sequencer ID $sequencerId"
            )
            // Peer streams are unidirectional: two of them (one per direction) are needed for a full-duplex P2P link.
            //  We avoid bidirectional streaming because client TLS certificate authentication is not well-supported
            //  by all network infrastructure, but we still want to be able to authenticate both ends with TLS.
            //  TLS support is however only about transport security; message-level authentication relies on
            //  signatures with keys registered in the Canton topology, that are unrelated to the TLS certificates.
            mutex(this) {
              val _ = serverEndpoints.put(serverPeer, sequencerId -> streamFromServer)
            }
          }
          .value
          .map(_ => ())
      }(connectExecutionContext, TraceContext.empty)

    private def openGrpcChannel(
        serverPeer: P2PEndpoint
    ): (
        ManagedChannel,
        BftOrderingServiceGrpc.BftOrderingServiceStub,
        BftOrderingServiceGrpc.BftOrderingServiceBlockingStub,
    ) = {
      implicit val executor: Executor = (command: Runnable) => executionContext.execute(command)
      val channel = createChannelBuilder(serverPeer.endpointConfig).build()
      val grpcSequencerClientAuth =
        authentication.map(auth =>
          new GrpcSequencerClientAuth(
            auth.synchronizerId,
            member = auth.sequencerId,
            crypto = auth.syncCrypto.crypto,
            channelPerEndpoint =
              NonEmpty(Map, Endpoint(serverPeer.address, serverPeer.port) -> channel),
            supportedProtocolVersions = Seq(auth.protocolVersion),
            // TODO(#20668) make it configurable
            tokenManagerConfig = AuthenticationTokenManagerConfig(),
            clock = auth.clock,
            timeouts = timeouts,
            loggerFactory = loggerFactory,
          )
        )
      mutex(this) {
        val _ = channels.put(serverPeer, channel)
        logger.debug(s"Created gRPC channel to peer in server role $serverPeer")
      }

      def authenticateStub[S <: AbstractStub[S]](stub: S) =
        grpcSequencerClientAuth.fold(stub)(_.apply(stub))

      (
        channel,
        authenticateStub(BftOrderingServiceGrpc.stub(channel)),
        authenticateStub(BftOrderingServiceGrpc.blockingStub(channel)),
      )
    }

    @SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
    private def createServerHandle(
        serverPeer: P2PEndpoint,
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
                connectWorkers.contains(serverPeer)
              }
            ) {
              createServerHandle(
                serverPeer,
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
        val _ = blockingStub.ping(PingRequest.defaultInstance)
        val sequencerIdPromise = Promise[SequencerId]()
        val streamFromServer = asyncStub.receive(
          new GrpcClientHandle(
            serverPeer,
            sequencerIdPromise,
            closeConnection,
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
                  s"create a stream to peer $serverPeer",
                  exception,
                  connectRetryDelay,
                  attemptNumber + 1,
                ).value
            }
          )
        case Failure(exception) =>
          retry(s"ping peer $serverPeer", exception, connectRetryDelay, attemptNumber + 1)
      }
    }

    private def shutdownGrpcChannel(
        serverPeer: P2PEndpoint,
        channel: ManagedChannel,
    ): Unit = {
      logger.debug(s"Terminating gRPC channel to peer in server role $serverPeer")
      val terminated =
        channel
          .shutdownNow()
          .awaitTermination(
            timeouts.closing.duration.toMillis,
            java.util.concurrent.TimeUnit.MILLISECONDS,
          )
      if (!terminated) {
        logger.warn(
          s"Failed to terminate in ${timeouts.closing.duration} the gRPC channel to peer in server role $serverPeer"
        )
      } else {
        logger.info(s"Successfully terminated gRPC channel to peer in server role $serverPeer")
      }
    }
  }

  object serverRole {

    private val grpcServers = servers
    private val clientEndpoints = mutable.Set[StreamObserver[BftOrderingServiceReceiveResponse]]()

    def startServers(): Unit =
      servers.foreach(_.foreach(_.server.start().discard))

    // Called by the gRPC server when receiving a connection
    def addClientHandle(clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]): Unit =
      mutex(this) {
        val _ = clientEndpoints.add(clientEndpoint)
      }

    // Called by the gRPC server endpoint when receiving an error or a completion from a client
    def cleanupClientHandle(
        clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]
    ): Unit = {
      logger.debug("Completing and removing client endpoint")
      completeHandle(clientEndpoint)
      mutex(this) {
        val _ = clientEndpoints.remove(clientEndpoint)
      }
    }

    def close(): Unit = {
      logger.debug("Closing P2P networking (server role)")
      clientEndpoints.foreach(cleanupClientHandle)
      shutdownGrpcServers()
      logger.debug("Closed P2P networking (server role)")
    }

    private def shutdownGrpcServers(): Unit = {
      logger.info(s"Shutting down gRPC servers")
      // We don't bother shutting down in parallel because typically there will be only 1 endpoint
      grpcServers.foreach(_.foreach(shutdownGrpcServer))
    }

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

  // TODO(#23926): generalize further to insulate internals from details and add simple string-typed endpoint for tests
  /** The BFT orderer's internal representation of a P2P endpoint */
  sealed trait P2PEndpoint extends Product {
    def address: String
    def port: Port
    def transportSecurity: Boolean

    def endpointConfig: P2PEndpointConfig

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
        config: P2PEndpointConfig
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
      address: String,
      port: Port,
  ) extends P2PEndpoint {

    override val transportSecurity: Boolean = false

    override lazy val endpointConfig: P2PEndpointConfig =
      P2PEndpointConfig(
        address,
        port,
        Some(TlsClientConfig(trustCollectionFile = None, clientCert = None, enabled = false)),
      )

    // TODO(#23926): currently used to build fake sequencer IDs in tests, can be removed once unit/sim-tests switch to symbolic endpoints
    override def toString: String = s"$address:$port"
  }

  object PlainTextP2PEndpoint {

    private[networking] def fromEndpointConfig(config: P2PEndpointConfig): PlainTextP2PEndpoint =
      PlainTextP2PEndpoint(config.address, config.port)
  }

  final case class TlsP2PEndpoint(
      override val endpointConfig: P2PEndpointConfig
  ) extends P2PEndpoint {

    override val transportSecurity: Boolean = true

    override val address: String = endpointConfig.address

    override def port: Port = endpointConfig.port
  }

  object TlsP2PEndpoint {

    private[networking] def fromEndpointConfig(endpointConfig: P2PEndpointConfig): TlsP2PEndpoint =
      TlsP2PEndpoint(endpointConfig)
  }

  private[bftordering] final case class Authentication(
      protocolVersion: ProtocolVersion,
      synchronizerId: SynchronizerId,
      sequencerId: SequencerId,
      syncCrypto: SynchronizerCryptoClient,
      clock: Clock,
  )
}
