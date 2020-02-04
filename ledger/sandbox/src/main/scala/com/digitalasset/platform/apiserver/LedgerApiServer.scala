// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.io.IOException
import java.net.{BindException, InetAddress, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.TimeUnit.{MILLISECONDS, SECONDS}

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.MetricRegistry
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.logging.{ContextualizedLogger, LoggingContext}
import com.digitalasset.resources.{Resource, ResourceOwner}
import io.grpc.netty.NettyServerBuilder
import io.grpc.{Server, ServerInterceptor}
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.ssl.SslContext
import io.netty.util.concurrent.DefaultThreadFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try
import scala.util.control.NoStackTrace

trait ApiServer {

  /** the API port the server is listening on */
  def port: Int

  /** completes when all services have been closed during the shutdown */
  def servicesClosed(): Future[Unit]

}

final class LedgerApiServer(
    createApiServices: (Materializer, ExecutionSequencerFactory) => Future[ApiServices],
    desiredPort: Int,
    maxInboundMessageSize: Int,
    address: Option[String],
    sslContext: Option[SslContext] = None,
    interceptors: List[ServerInterceptor] = List.empty,
    metrics: MetricRegistry,
)(implicit actorSystem: ActorSystem, materializer: Materializer, logCtx: LoggingContext)
    extends ResourceOwner[ApiServer] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit executionContext: ExecutionContext): Resource[ApiServer] = {
    val servicesClosedPromise = Promise[Unit]()

    for {
      serverEsf <- new ExecutionSequencerFactoryOwner().acquire()
      workerEventLoopGroup <- new EventLoopGroupOwner(
        actorSystem.name + "-nio-worker",
        parallelism = Runtime.getRuntime.availableProcessors).acquire()
      bossEventLoopGroup <- new EventLoopGroupOwner(actorSystem.name + "-nio-boss", parallelism = 1)
        .acquire()
      apiServicesResource = ResourceOwner
        .forFutureCloseable(() => createApiServices(materializer, serverEsf))
        .acquire()
      apiServices <- apiServicesResource
      server <- new GrpcServerOwner(
        address,
        desiredPort,
        maxInboundMessageSize,
        sslContext,
        interceptors,
        metrics,
        bossEventLoopGroup,
        workerEventLoopGroup,
        apiServices,
      ).acquire()
      _ <- new ReorderApiServices(apiServicesResource, servicesClosedPromise).acquire()
    } yield {
      val host = address.getOrElse("localhost")
      val actualPort = server.getPort
      val transportMedium = if (sslContext.isDefined) "TLS" else "plain text"
      logger.info(s"Listening on $host:$actualPort over $transportMedium.")
      new ApiServer {
        override val port: Int =
          server.getPort

        override def servicesClosed(): Future[Unit] =
          servicesClosedPromise.future
      }
    }
  }

  private final class ExecutionSequencerFactoryOwner(implicit actorSystem: ActorSystem)
      extends ResourceOwner[ExecutionSequencerFactory] {
    // NOTE: Pick a unique pool name as we want to allow multiple LedgerApiServer instances,
    // and it's pretty difficult to wait for the name to become available again.
    // The name deregistration is asynchronous and the close method does not wait, and it isn't
    // trivial to implement.
    // https://doc.akka.io/docs/akka/2.5/actors.html#graceful-stop
    private val poolName = s"ledger-api-server-rs-grpc-bridge-${UUID.randomUUID}"

    private val ActorCount = Runtime.getRuntime.availableProcessors() * 8

    override def acquire()(
        implicit executionContext: ExecutionContext
    ): Resource[ExecutionSequencerFactory] =
      Resource(Future(new AkkaExecutionSequencerPool(poolName, ActorCount)))(_.closeAsync())
  }

  private final class EventLoopGroupOwner(threadPoolName: String, parallelism: Int)
      extends ResourceOwner[EventLoopGroup] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[EventLoopGroup] =
      Resource(
        Future(new NioEventLoopGroup(
          parallelism,
          new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop-${UUID.randomUUID()}", true))))(
        group => {
          val promise = Promise[Unit]()
          val future = group.shutdownGracefully(0, 0, MILLISECONDS)
          future.addListener((f: io.netty.util.concurrent.Future[_]) =>
            promise.complete(Try(f.get).map(_ => ())))
          promise.future
        }
      )
  }

  private final class GrpcServerOwner(
      address: Option[String],
      desiredPort: Int,
      maxInboundMessageSize: Int,
      sslContext: Option[SslContext] = None,
      interceptors: List[ServerInterceptor] = List.empty,
      metrics: MetricRegistry,
      bossEventLoopGroup: EventLoopGroup,
      workerEventLoopGroup: EventLoopGroup,
      apiServices: ApiServices,
  ) extends ResourceOwner[Server] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[Server] = {
      val host = address.map(InetAddress.getByName).getOrElse(InetAddress.getLoopbackAddress)
      Resource(Future {
        val builder =
          NettyServerBuilder.forAddress(new InetSocketAddress(host, desiredPort))
        builder.sslContext(sslContext.orNull)
        builder.channelType(classOf[NioServerSocketChannel])
        builder.permitKeepAliveTime(10, SECONDS)
        builder.permitKeepAliveWithoutCalls(true)
        builder.directExecutor()
        builder.maxInboundMessageSize(maxInboundMessageSize)
        interceptors.foreach(builder.intercept)
        builder.intercept(new MetricsInterceptor(metrics))
        builder.bossEventLoopGroup(bossEventLoopGroup)
        builder.workerEventLoopGroup(workerEventLoopGroup)
        apiServices.services.foreach(builder.addService)
        val server = builder.build()
        try {
          server.start()
        } catch {
          case e: IOException if e.getCause != null && e.getCause.isInstanceOf[BindException] =>
            throw new UnableToBind(desiredPort, e.getCause)
        }
        server
      })(server => Future(server.shutdown().awaitTermination()))
    }
  }

  // This is necessary because we need to signal to the ResetService that we have shut down the
  // APIs so it can consider the reset "done". Once it's finished, the reset request will finish,
  // the gRPC connection will be closed and we can safely shut down the gRPC server. If we don't
  // do that, the server won't shut down and we'll enter a deadlock.
  private final class ReorderApiServices(
      apiServices: Resource[ApiServices],
      servicesClosedPromise: Promise[Unit],
  ) extends ResourceOwner[ApiServices] {
    override def acquire()(implicit executionContext: ExecutionContext): Resource[ApiServices] =
      Resource(apiServices.asFuture)(_ =>
        apiServices.release().map(_ => servicesClosedPromise.success(())))
  }

  final class UnableToBind(port: Int, cause: Throwable)
      extends RuntimeException(
        s"LedgerApiServer was unable to bind to port $port. " +
          "User should terminate the process occupying the port, or choose a different one.",
        cause)
      with NoStackTrace
}
