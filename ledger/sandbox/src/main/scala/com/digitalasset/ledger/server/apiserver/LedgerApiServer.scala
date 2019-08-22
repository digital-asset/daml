// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.server.apiserver

import java.io.IOException
import java.net.{BindException, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.stream.ActorMaterializer
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import io.grpc.netty.NettyServerBuilder
import io.grpc.ServerInterceptor
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.ssl.SslContext
import io.netty.util.concurrent.DefaultThreadFactory
import io.netty.channel.socket.nio.NioServerSocketChannel
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.util.control.NoStackTrace

trait ApiServer extends AutoCloseable {

  /** returns the api port the server is listening on */
  def port: Int

  /** returns when all services have been closed during the shutdown */
  def servicesClosed(): Future[Unit]

}

object LedgerApiServer {
  def create(
      createApiServices: (ActorMaterializer, ExecutionSequencerFactory) => Future[ApiServices],
      desiredPort: Int,
      maxInboundMessageSize: Int,
      address: Option[String],
      sslContext: Option[SslContext] = None,
      interceptors: List[ServerInterceptor] = List.empty)(
      implicit mat: ActorMaterializer): Future[ApiServer] = {

    val serverEsf = new AkkaExecutionSequencerPool(
      // NOTE(JM): Pick a unique pool name as we want to allow multiple ledger api server
      // instances, and it's pretty difficult to wait for the name to become available
      // again (the name deregistration is asynchronous and the close method is not waiting for
      // it, and it isn't trivial to implement).
      // https://doc.akka.io/docs/akka/2.5/actors.html#graceful-stop
      poolName = s"ledger-api-server-rs-grpc-bridge-${UUID.randomUUID}",
      actorCount = Runtime.getRuntime.availableProcessors() * 8
    )(mat.system)

    createApiServices(mat, serverEsf).map { apiServices =>
      new ApiServer {
        private val impl = new LedgerApiServer(
          apiServices,
          desiredPort,
          maxInboundMessageSize,
          address,
          sslContext,
          interceptors
        )

        /** returns the api port the server is listening on */
        override def port: Int = impl.port

        /** returns when all services have been closed during the shutdown */
        override def servicesClosed(): Future[Unit] = impl.servicesClosed()

        override def close(): Unit = {
          impl.close()
          serverEsf.close()
        }
      }
    }(mat.executionContext)
  }

}

private class LedgerApiServer(
    apiServices: ApiServices,
    desiredPort: Int,
    maxInboundMessageSize: Int,
    address: Option[String],
    sslContext: Option[SslContext] = None,
    interceptors: List[ServerInterceptor] = List.empty)(implicit mat: ActorMaterializer)
    extends ApiServer {

  private val logger = LoggerFactory.getLogger(this.getClass)

  class UnableToBind(port: Int, cause: Throwable)
      extends RuntimeException(
        s"LedgerApiServer was unable to bind to port $port. " +
          "User should terminate the process occupying the port, or choose a different one.",
        cause)
      with NoStackTrace

  private val workerEventLoopGroup = createEventLoopGroup(mat.system.name + "-nio-worker")

  private val bossEventLoopGroup = createEventLoopGroup(mat.system.name + "-nio-boss", 1)

  private val (grpcServer, actualPort) = startServer()

  override def port: Int = actualPort

  private def startServer() = {
    val builder = address.fold(NettyServerBuilder.forPort(desiredPort))(address =>
      NettyServerBuilder.forAddress(new InetSocketAddress(address, desiredPort)))

    sslContext
      .fold {
        logger.info("Starting plainText server")
      } { sslContext =>
        logger.info("Starting TLS server")
        val _ = builder.sslContext(sslContext)
      }

    builder.directExecutor()
    builder.channelType(classOf[NioServerSocketChannel])
    builder.bossEventLoopGroup(bossEventLoopGroup)
    builder.workerEventLoopGroup(workerEventLoopGroup)
    builder.permitKeepAliveTime(10, TimeUnit.SECONDS)
    builder.permitKeepAliveWithoutCalls(true)
    builder.maxInboundMessageSize(maxInboundMessageSize)
    interceptors.foreach(builder.intercept)
    val grpcServer = apiServices.services.foldLeft(builder)(_ addService _).build
    try {
      grpcServer.start()
      logger.info(s"listening on ${address.getOrElse("localhost")}:${grpcServer.getPort}")
      (grpcServer, grpcServer.getPort)
    } catch {
      case io: IOException if io.getCause != null && io.getCause.isInstanceOf[BindException] =>
        throw new UnableToBind(desiredPort, io.getCause)
    }
  }

  private def createEventLoopGroup(
      threadPoolName: String,
      parallelism: Int = Runtime.getRuntime.availableProcessors): NioEventLoopGroup = {
    val threadFactory =
      new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop-${UUID.randomUUID}", true)
    new NioEventLoopGroup(parallelism, threadFactory)
  }

  private val servicesClosedP = Promise[Unit]()

  /** returns when all services have been closed during the shutdown */
  override def servicesClosed(): Future[Unit] = servicesClosedP.future

  override def close(): Unit = {
    apiServices.close()
    servicesClosedP.success(())
    grpcServer.shutdown()

    if (!grpcServer.awaitTermination(10L, TimeUnit.SECONDS)) {
      logger.warn(
        "Server did not terminate gracefully in one second. " +
          "Clients probably did not disconnect. " +
          "Proceeding with forced termination.")
      grpcServer.shutdownNow()
    }
    // `shutdownGracefully` has a "quiet period" which specifies a time window in which
    // _no requests_ must be witnessed before shutdown is _initiated_. Here we want to
    // start immediately, so no quiet period -- by default it's 2 seconds.
    // Moreover, there's also a "timeout" parameter
    // which caps the time to wait for the quiet period to be fullfilled. Since we have
    // no quiet period, this can also be 0.
    // See <https://netty.io/4.1/api/io/netty/util/concurrent/EventExecutorGroup.html#shutdownGracefully-long-long-java.util.concurrent.TimeUnit->.
    // The 10 seconds to wait is sort of arbitrary, it's long enough to be noticeable though.
    val workerEventLoopGroupShutdown =
      workerEventLoopGroup.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS)
    val bossEventLoopGroupShutdown =
      bossEventLoopGroup.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS)

    val workerShutdownComplete = workerEventLoopGroupShutdown.await(10L, TimeUnit.SECONDS)
    val bossShutdownComplete = bossEventLoopGroupShutdown.await(10L, TimeUnit.SECONDS)
  }

}
