// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.io.IOException
import java.net.{BindException, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.TimeUnit.{MILLISECONDS, SECONDS}

import akka.stream.ActorMaterializer
import com.codahale.metrics.MetricRegistry
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.resources.{Resource, ResourceOwner}
import io.grpc.netty.NettyServerBuilder
import io.grpc.{Server, ServerInterceptor}
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.ssl.SslContext
import io.netty.util.concurrent.DefaultThreadFactory

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future, Promise}
import scala.util.Try
import scala.util.control.NoStackTrace

trait ApiServer extends AutoCloseable {

  /** the API port the server is listening on */
  def port: Int

  /** completes when all services have been closed during the shutdown */
  def servicesClosed(): Future[Unit]

}

object LedgerApiServer {
  def start(
      createApiServices: (ActorMaterializer, ExecutionSequencerFactory) => Future[ApiServices],
      desiredPort: Int,
      maxInboundMessageSize: Int,
      address: Option[String],
      loggerFactory: NamedLoggerFactory,
      sslContext: Option[SslContext] = None,
      interceptors: List[ServerInterceptor] = List.empty,
      metrics: MetricRegistry,
  )(implicit mat: ActorMaterializer): Future[ApiServer] = {
    implicit val executionContext: ExecutionContextExecutor = mat.executionContext

    val logger = loggerFactory.getLogger(this.getClass)
    val servicesClosedPromise = Promise[Unit]()

    def apiServicesResourceOwner(serverEsf: ExecutionSequencerFactory): ResourceOwner[ApiServices] =
      new ResourceOwner[ApiServices] {
        override def acquire()(
            implicit _executionContext: ExecutionContext
        ): Resource[ApiServices] =
          new Resource[ApiServices] {
            override protected val executionContext: ExecutionContext = _executionContext

            override protected val future: Future[ApiServices] = createApiServices(mat, serverEsf)

            override def release(): Future[Unit] =
              future.map(apiServices => {
                apiServices.close()
                servicesClosedPromise.success(())
              })
          }
      }

    val server = for {
      serverEsf <- ResourceOwner.forCloseable(executionSequencerFactoryResource _).acquire()
      workerEventLoopGroup <- eventLoopGroup(
        mat.system.name + "-nio-worker",
        parallelism = Runtime.getRuntime.availableProcessors).acquire()
      bossEventLoopGroup <- eventLoopGroup(mat.system.name + "-nio-boss", parallelism = 1).acquire()
      apiServices <- apiServicesResourceOwner(serverEsf).acquire()
      server <- grpcServer(
        address,
        desiredPort,
        maxInboundMessageSize,
        sslContext,
        interceptors,
        metrics,
        bossEventLoopGroup,
        workerEventLoopGroup,
        apiServices).acquire()
    } yield {
      val host = address.getOrElse("localhost")
      val actualPort = server.getPort
      val transportMedium = if (sslContext.isDefined) "TLS" else "plain text"
      logger.info(s"Listening on $host:$actualPort over $transportMedium.")
      server
    }

    server.asFuture.map(grpcServer =>
      new ApiServer {
        override val port: Int =
          grpcServer.getPort

        override def servicesClosed(): Future[Unit] =
          servicesClosedPromise.future

        override def close(): Unit =
          Await.result(server.release(), 10.seconds)
    })
  }

  private def executionSequencerFactoryResource()(
      implicit mat: ActorMaterializer
  ): ExecutionSequencerFactory =
    new AkkaExecutionSequencerPool(
      // NOTE(JM): Pick a unique pool name as we want to allow multiple ledger api server
      // instances, and it's pretty difficult to wait for the name to become available
      // again (the name deregistration is asynchronous and the close method is not waiting for
      // it, and it isn't trivial to implement).
      // https://doc.akka.io/docs/akka/2.5/actors.html#graceful-stop
      poolName = s"ledger-api-server-rs-grpc-bridge-${UUID.randomUUID}",
      actorCount = Runtime.getRuntime.availableProcessors() * 8
    )(mat.system)

  private def eventLoopGroup(
      threadPoolName: String,
      parallelism: Int
  ): ResourceOwner[EventLoopGroup] = new ResourceOwner[EventLoopGroup] {
    override def acquire()(
        implicit _executionContext: ExecutionContext
    ): Resource[EventLoopGroup] = {
      val group = new NioEventLoopGroup(
        parallelism,
        new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop-${UUID.randomUUID()}", true))
      new Resource[EventLoopGroup] {
        override protected val executionContext: ExecutionContext = _executionContext

        override protected val future: Future[EventLoopGroup] = Future.successful(group)

        override def release(): Future[Unit] = {
          val future = group.shutdownGracefully(0, 0, MILLISECONDS)
          val promise = Promise[Unit]()
          future.addListener((future: io.netty.util.concurrent.Future[_]) =>
            promise.complete(Try(future.get).map(_ => ())))
          promise.future
        }
      }
    }
  }

  private def grpcServer(
      address: Option[String],
      desiredPort: Int,
      maxInboundMessageSize: Int,
      sslContext: Option[SslContext] = None,
      interceptors: List[ServerInterceptor] = List.empty,
      metrics: MetricRegistry,
      bossEventLoopGroup: EventLoopGroup,
      workerEventLoopGroup: EventLoopGroup,
      apiServices: ApiServices,
  ) = new ResourceOwner[Server] {
    override def acquire()(implicit _executionContext: ExecutionContext): Resource[Server] = {
      new Resource[Server] {
        override protected val executionContext: ExecutionContext = _executionContext

        override protected val future: Future[Server] = Future {
          val builder = address.fold(NettyServerBuilder.forPort(desiredPort))(address =>
            NettyServerBuilder.forAddress(new InetSocketAddress(address, desiredPort)))
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
        }

        override def release(): Future[Unit] =
          asFuture.map(server => server.shutdown().awaitTermination())
      }
    }
  }

  class UnableToBind(port: Int, cause: Throwable)
      extends RuntimeException(
        s"LedgerApiServer was unable to bind to port $port. " +
          "User should terminate the process occupying the port, or choose a different one.",
        cause)
      with NoStackTrace
}
