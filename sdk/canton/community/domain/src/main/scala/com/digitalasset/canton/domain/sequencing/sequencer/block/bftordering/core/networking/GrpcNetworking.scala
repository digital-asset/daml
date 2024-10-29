// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking

import cats.data.OptionT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.sequencing.sequencer.bftordering.v1.{
  BftOrderingServiceGrpc,
  BftOrderingServiceReceiveRequest,
  BftOrderingServiceReceiveResponse,
  PingRequest,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.core.networking.GrpcNetworking.{
  InitialConnectRetryDelay,
  MaxConnectRetryDelay,
}
import com.digitalasset.canton.domain.sequencing.sequencer.block.bftordering.p2p.grpc.GrpcClientEndpoint
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.Endpoint
import com.digitalasset.canton.networking.grpc.CommunityClientChannelBuilder
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.SequencerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.DelayUtil
import io.grpc.ManagedChannel
import io.grpc.stub.StreamObserver
import org.slf4j.event.Level

import java.time.Duration
import java.util.concurrent.{Executor, Executors}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

final class GrpcNetworking(
    servers: List[UnlessShutdown[Lifecycle.CloseableServer]],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  import TraceContext.Implicits.Empty.emptyTraceContext

  object clientRole {

    private val connectExecutor = Executors.newCachedThreadPool()
    private val connectExecutionContext = ExecutionContext.fromExecutor(connectExecutor)
    private val connectWorkers = mutable.Map[Endpoint, FutureUnlessShutdown[Unit]]()
    private val serverEndpoints =
      mutable.Map[Endpoint, (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest])]()
    private val channels = mutable.Map[Endpoint, ManagedChannel]()

    // Called by the client network manager when establishing a connection to a peer
    def getServerEndpointOrStartConnection(
        serverPeer: Endpoint
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
    def closeConnection(serverPeer: Endpoint): Unit = {
      logger.info(s"Closing connection to peer in server role $serverPeer")
      mutex(this) {
        serverEndpoints.remove(serverPeer).map(_._2).foreach(completeEndpoint)
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

    private def ensureConnectWorker(serverPeer: Endpoint): Unit =
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

    private def connect(serverPeer: Endpoint): FutureUnlessShutdown[Unit] =
      performUnlessClosingF("p2p-connect") {
        logger.debug(s"Creating a gRPC channel and connecting to peer in server role $serverPeer")
        val (channel, stub) = openGrpcChannel(serverPeer)
        createServerEndpoint(serverPeer, channel, stub)
          .map { case (sequencerId, streamFromServer) =>
            logger.info(
              s"Successfully connected to peer in server role $serverPeer with sequencer ID $sequencerId"
            )
            // Peer streams are unidirectional, else the server could not identify the client or we'd need mTLS,
            //  which is not supported by all network infrastructure.
            mutex(this) {
              val _ = serverEndpoints.put(serverPeer, sequencerId -> streamFromServer)
            }
          }
          .value
          .map(_ => ())
      }(connectExecutionContext, TraceContext.empty)

    private def openGrpcChannel(
        serverPeer: Endpoint
    ): (ManagedChannel, BftOrderingServiceGrpc.BftOrderingServiceStub) = {
      val executor: Executor = (command: Runnable) => executionContext.execute(command)
      val channel = CommunityClientChannelBuilder(loggerFactory)
        .create(
          NonEmpty(Seq, serverPeer),
          useTls = false,
          executor,
        )
        .build()
      mutex(this) {
        val _ = channels.put(serverPeer, channel)
        logger.debug(s"Created gRPC channel to peer in server role $serverPeer")
      }
      (channel, BftOrderingServiceGrpc.stub(channel))
    }

    @SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
    private def createServerEndpoint(
        serverPeer: Endpoint,
        channel: ManagedChannel,
        stub: BftOrderingServiceGrpc.BftOrderingServiceStub,
        connectRetryDelay: NonNegativeFiniteDuration = InitialConnectRetryDelay,
    ): OptionT[Future, (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest])] = {

      def retry(
          failureDescription: String,
          exception: Throwable,
          previousRetryDelay: NonNegativeFiniteDuration,
      ): OptionT[Future, (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest])] = {
        val retryDelay = MaxConnectRetryDelay.min(previousRetryDelay * NonNegativeInt.tryCreate(2))
        logger.info(
          s"in client role failed to $failureDescription, retrying in $retryDelay",
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
              // This call is not tail recursive but it also doesn't need to be, since it's going to be
              //  performed in a separate executor task and thus doesn't increase the stack.
              createServerEndpoint(serverPeer, channel, stub, retryDelay)
            } else {
              logger.info(
                s"in client role failed to $failureDescription but not retrying because the connection is being closed",
                exception,
              )
              OptionT.none[Future, (SequencerId, StreamObserver[BftOrderingServiceReceiveRequest])]
            }
        } yield result
      }

      Try {
        // Unfortunately the async client fails asynchronously, so a synchronous ping comes in useful to check that
        //  at least the initial connection can be established.
        val _ = BftOrderingServiceGrpc.blockingStub(channel).ping(PingRequest.defaultInstance)
        val sequencerIdPromise = Promise[SequencerId]()
        val streamFromServer = stub.receive(
          new GrpcClientEndpoint(
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
                retry(s"create a stream to peer $serverPeer", exception, connectRetryDelay).value
            }
          )
        case Failure(exception) =>
          retry(s"ping peer $serverPeer", exception, connectRetryDelay)
      }
    }

    private def shutdownGrpcChannel(serverPeer: Endpoint, channel: ManagedChannel): Unit = {
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
    def addClientEndpoint(clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]): Unit =
      mutex(this) {
        val _ = clientEndpoints.add(clientEndpoint)
      }

    // Called by the gRPC server endpoint when receiving an error or a completion from a client
    def cleanupClientEndpoint(
        clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]
    ): Unit = {
      logger.debug("Completing and removing client endpoint")
      completeEndpoint(clientEndpoint)
      mutex(this) {
        val _ = clientEndpoints.remove(clientEndpoint)
      }
    }

    def close(): Unit = {
      logger.debug("Closing P2P networking (server role)")
      clientEndpoints.foreach(cleanupClientEndpoint)
      shutdownGrpcServers()
      logger.debug("Closed P2P networking (server role)")
    }

    private def shutdownGrpcServers(): Unit = {
      logger.info(s"Shutting down gRPC servers")
      // We don't bother shutting down in parallel because typically there will be only 1 endpoint
      grpcServers.foreach(_.foreach(shutdownGrpcServer))
    }

    private def shutdownGrpcServer(server: Lifecycle.CloseableServer): Unit = {
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

  private def completeEndpoint(endpoint: StreamObserver[?]): Unit =
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

private object GrpcNetworking {
  val InitialConnectRetryDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryCreate(Duration.ofMillis(300))
  val MaxConnectRetryDelay: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.tryCreate(Duration.ofSeconds(2))
}
