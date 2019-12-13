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
import io.grpc.ServerInterceptor
import io.grpc.netty.NettyServerBuilder
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.ssl.SslContext
import io.netty.util.concurrent.DefaultThreadFactory

import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.util.control.{NoStackTrace, NonFatal}

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

    val logger = loggerFactory.getLogger(this.getClass)

    val closeables = mutable.Stack[AutoCloseable]()

    def stop(): Unit = {
      while (closeables.nonEmpty) {
        closeables.pop().close()
      }
    }

    val serverEsf = new AkkaExecutionSequencerPool(
      // NOTE(JM): Pick a unique pool name as we want to allow multiple ledger api server
      // instances, and it's pretty difficult to wait for the name to become available
      // again (the name deregistration is asynchronous and the close method is not waiting for
      // it, and it isn't trivial to implement).
      // https://doc.akka.io/docs/akka/2.5/actors.html#graceful-stop
      poolName = s"ledger-api-server-rs-grpc-bridge-${UUID.randomUUID}",
      actorCount = Runtime.getRuntime.availableProcessors() * 8
    )(mat.system)
    closeables.push(serverEsf)

    val workerEventLoopGroup = createEventLoopGroup(
      mat.system.name + "-nio-worker",
      parallelism = Runtime.getRuntime.availableProcessors)
    val bossEventLoopGroup = createEventLoopGroup(mat.system.name + "-nio-boss", parallelism = 1)
    closeables.push(() => {
      // `shutdownGracefully` has a "quiet period" which specifies a time window in which
      // _no requests_ must be witnessed before shutdown is _initiated_. Here we want to
      // start immediately, so no quiet period -- by default it's 2 seconds.
      // Moreover, there's also a "timeout" parameter
      // which caps the time to wait for the quiet period to be fullfilled. Since we have
      // no quiet period, this can also be 0.
      // See <https://netty.io/4.1/api/io/netty/util/concurrent/EventExecutorGroup.html#shutdownGracefully-long-long-java.util.concurrent.TimeUnit->.
      // The 10 seconds to wait is sort of arbitrary, it's long enough to be noticeable though.
      Seq(
        workerEventLoopGroup.shutdownGracefully(0, 0, MILLISECONDS),
        bossEventLoopGroup.shutdownGracefully(0, 0, MILLISECONDS),
      ).foreach(_.await(10, SECONDS))
    })

    createApiServices(mat, serverEsf).map { apiServices =>
      val builder = address.fold(NettyServerBuilder.forPort(desiredPort))(address =>
        NettyServerBuilder.forAddress(new InetSocketAddress(address, desiredPort)))
      builder.sslContext(sslContext.orNull)
      builder.directExecutor()
      builder.channelType(classOf[NioServerSocketChannel])
      builder.bossEventLoopGroup(bossEventLoopGroup)
      builder.workerEventLoopGroup(workerEventLoopGroup)
      builder.permitKeepAliveTime(10, SECONDS)
      builder.permitKeepAliveWithoutCalls(true)
      builder.maxInboundMessageSize(maxInboundMessageSize)
      interceptors.foreach(builder.intercept)
      builder.intercept(new MetricsInterceptor(metrics))
      apiServices.services.foreach(builder.addService)
      val grpcServer = builder.build()

      try {
        grpcServer.start()
      } catch {
        case io: IOException if io.getCause != null && io.getCause.isInstanceOf[BindException] =>
          stop()
          throw new UnableToBind(desiredPort, io.getCause)
        case NonFatal(e) =>
          stop()
          throw e
      }
      closeables.push(() => {
        grpcServer.shutdown()
        if (!grpcServer.awaitTermination(10, SECONDS)) {
          logger.warn(
            "Server did not terminate gracefully in one second. " +
              "Clients probably did not disconnect. " +
              "Proceeding with forced termination.")
          val _ = grpcServer.shutdownNow()
        }
      })

      val host = address.getOrElse("localhost")
      val actualPort = grpcServer.getPort
      val transportMedium = if (sslContext.isDefined) "TLS" else "plain text"
      logger.info(s"Listening on $host:$actualPort over $transportMedium.")

      val servicesClosedP = Promise[Unit]()
      closeables.push(() => {
        apiServices.close()
        servicesClosedP.success(())
      })

      new ApiServer {
        override val port: Int = actualPort

        override def servicesClosed(): Future[Unit] = servicesClosedP.future

        override def close(): Unit = stop()
      }
    }(mat.executionContext)
  }

  private def createEventLoopGroup(threadPoolName: String, parallelism: Int): EventLoopGroup =
    new NioEventLoopGroup(
      parallelism,
      new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop-${UUID.randomUUID()}", true))

  class UnableToBind(port: Int, cause: Throwable)
      extends RuntimeException(
        s"LedgerApiServer was unable to bind to port $port. " +
          "User should terminate the process occupying the port, or choose a different one.",
        cause)
      with NoStackTrace
}
