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
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.GrpcClientConnectionManager.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.GrpcNetworking.{
  AuthenticationInitialState,
  P2PEndpoint,
  completeHandle,
  mutex,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.authentication.{
  AddEndpointHeaderClientInterceptor,
  AuthenticateServerClientInterceptor,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  BftOrderingServiceGrpc,
  BftOrderingServiceReceiveRequest,
  PingRequest,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import io.grpc.stub.{AbstractStub, StreamObserver}
import io.grpc.{Channel, ClientInterceptors, ManagedChannel}
import org.slf4j.event.Level

import java.time.Duration
import java.util.concurrent.{Executor, Executors}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.{Failure, Success, Try}

class GrpcClientConnectionManager(
    authenticationInitialState: Option[AuthenticationInitialState],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable { self =>

  import TraceContext.Implicits.Empty.emptyTraceContext

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

  override def onClosed(): Unit = {
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
          connect(serverEndpoint)(connectExecutionContext)
            .foreach(worker => connectWorkers.put(serverEndpoint, worker).discard)
      }
    }

  private def connect(
      serverEndpoint: P2PEndpoint
  )(implicit executionContext: ExecutionContext): Option[FutureUnlessShutdown[Unit]] =
    synchronizeWithClosingSync("open-grpc-channel") {
      logger.debug(
        s"Creating a gRPC channel and connecting to endpoint in server role $serverEndpoint"
      )
      Some(openGrpcChannel(serverEndpoint))
    }
      .onShutdown {
        logger.info(
          s"Not attempting to create a gRPC channel and connect to endpoint in server role $serverEndpoint " +
            "due to shutdown"
        )
        None
      }
      .map {
        case OpenChannel(
              channel,
              maybeSequencerIdFromAuthenticationFutureUS,
              asyncStub,
              blockingStub,
            ) =>
          val serverHandleOptionT =
            createServerHandle(serverEndpoint, channel, asyncStub, blockingStub)
          maybeSequencerIdFromAuthenticationFutureUS match {
            case Some(sequencerIdFromAuthenticationFutureUS) =>
              sequencerIdFromAuthenticationFutureUS.flatMap { sequencerIdFromAuthentication =>
                toUnitFutureUS(
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
              toUnitFutureUS(
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
      }

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
    val maybeAuthenticationContext =
      authenticationInitialState.map(auth =>
        new GrpcSequencerClientAuth(
          auth.psid,
          member = auth.sequencerId,
          crypto = auth.authenticationServices.syncCryptoForAuthentication.crypto,
          channelPerEndpoint =
            NonEmpty(Map, Endpoint(serverEndpoint.address, serverEndpoint.port) -> channel),
          supportedProtocolVersions = Seq(auth.psid.protocolVersion),
          tokenManagerConfig = auth.authTokenConfig,
          clock = auth.clock,
          timeouts = timeouts,
          loggerFactory = loggerFactory,
        ) ->
          auth.serverToClientAuthenticationEndpoint
      )
    mutex(this) {
      channels.put(serverEndpoint, channel).discard
      maybeAuthenticationContext.foreach { case (grpcSequencerClientAuth, _) =>
        grpcSequencerClientAuths.put(serverEndpoint, grpcSequencerClientAuth).discard
      }
      logger.debug(s"Created gRPC channel to endpoint in server role $serverEndpoint")
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
      checkServerAuthentication(channel)

    OpenChannel(
      channel,
      maybeSequencerIdFromAuthenticationPromiseUS.map(_.futureUS),
      maybeAuthenticateStub(BftOrderingServiceGrpc.stub(checkedChannel)),
      maybeAuthenticateStub(BftOrderingServiceGrpc.blockingStub(checkedChannel)),
    )
  }

  private def checkServerAuthentication(
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
  private def createServerHandle(
      serverEndpoint: P2PEndpoint,
      channel: ManagedChannel,
      asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
      blockingStub: BftOrderingServiceGrpc.BftOrderingServiceBlockingStub,
      connectRetryDelay: NonNegativeFiniteDuration = InitialConnectRetryDelay,
      attemptNumber: Int = 1,
  ): OptionT[
    FutureUnlessShutdown,
    (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest]),
  ] =
    synchronizeWithClosing("create-server-handle") {
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
            if (mutex(this)(connectWorkers.contains(serverEndpoint))) {
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
              OptionT
                .none[
                  FutureUnlessShutdown,
                  (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest]),
                ]
            }
        } yield result
      }

      Try {
        // Unfortunately the async client fails asynchronously, so a synchronous ping comes in useful to check that
        //  at least the initial connection can be established.
        blockingStub.ping(PingRequest.defaultInstance).discard
        val sequencerIdPromiseUS = PromiseUnlessShutdown.unsupervised[SequencerId]()
        val streamFromServer = asyncStub.receive(
          new GrpcClientHandle(
            serverEndpoint,
            sequencerIdPromiseUS,
            closeConnection,
            authenticationEnabled = authenticationInitialState.isDefined,
            loggerFactory,
          )
        )
        sequencerIdPromiseUS.futureUS.map(sequencerId => (sequencerId, streamFromServer))
      } match {
        case Success(futureUSResult) =>
          OptionT(
            futureUSResult.transformWith {
              case Success(value) =>
                FutureUnlessShutdown.lift(value.map(Option(_)))
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

private[bftordering] object GrpcClientConnectionManager {

  final case class ServerHandleInfo[REQ](
      sequencerId: SequencerId,
      serverHandle: StreamObserver[REQ],
      isNewlyConnected: Boolean,
  )

  private val MaxConnectionAttemptsBeforeWarning = 10

  private val InitialConnectRetryDelay =
    NonNegativeFiniteDuration.tryCreate(Duration.ofMillis(300))

  private val MaxConnectRetryDelay =
    NonNegativeFiniteDuration.tryCreate(Duration.ofSeconds(2))

  private final case class OpenChannel(
      channel: ManagedChannel,
      maybeSequencerIdFromAuthenticationFutureUS: Option[
        FutureUnlessShutdown[SequencerId]
      ],
      asyncStub: BftOrderingServiceGrpc.BftOrderingServiceStub,
      blockingStub: BftOrderingServiceGrpc.BftOrderingServiceBlockingStub,
  )

  private def toUnitFutureUS[X](optionT: OptionT[FutureUnlessShutdown, X])(implicit
      ec: ExecutionContext
  ): FutureUnlessShutdown[Unit] =
    optionT.value.map(_ => ())
}
