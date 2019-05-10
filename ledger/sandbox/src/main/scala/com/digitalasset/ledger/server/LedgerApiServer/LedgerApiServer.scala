// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.server.LedgerApiServer

import java.io.IOException
import java.net.{BindException, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.TimeUnit

import akka.stream.ActorMaterializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.backend.api.v1.LedgerBackend
import com.digitalasset.platform.sandbox.config.SandboxConfig
import com.digitalasset.platform.sandbox.services._
import com.digitalasset.platform.server.services.testing.TimeServiceBackend
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.handler.ssl.SslContext
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory

import scala.util.control.NoStackTrace

object LedgerApiServer {
  def apply(
      ledgerBackend: LedgerBackend,
      timeProvider: TimeProvider,
      engine: Engine,
      config: SandboxConfig,
      //even though the port is in the config as well, in case of a reset we have to keep the port to what it was originally set for the first time
      serverPort: Int,
      timeServiceBackend: Option[TimeServiceBackend],
      resetService: Option[SandboxResetService])(
      implicit mat: ActorMaterializer): LedgerApiServer = {

    new LedgerApiServer(
      (am: ActorMaterializer, esf: ExecutionSequencerFactory) =>
        ApiServices.create(config, ledgerBackend, engine, timeProvider, timeServiceBackend)(
          am,
          esf),
      config,
      serverPort,
      timeServiceBackend,
      resetService,
      config.address,
      config.tlsConfig.flatMap(_.server)
    ).start()
  }
}

class LedgerApiServer(
    createApiServices: (ActorMaterializer, ExecutionSequencerFactory) => ApiServices,
    config: SandboxConfig,
    serverPort: Int,
    timeServiceBackend: Option[TimeServiceBackend],
    resetService: Option[SandboxResetService],
    address: Option[String],
    sslContext: Option[SslContext] = None)(implicit mat: ActorMaterializer)
    extends AutoCloseable {

  class UnableToBind(port: Int, cause: Throwable)
      extends RuntimeException(
        s"LedgerApiServer was unable to bind to port $port. " +
          "User should terminate the process occupying the port, or choose a different one.",
        cause)
      with NoStackTrace

  private implicit val serverEsf = new AkkaExecutionSequencerPool(
    // NOTE(JM): Pick a unique pool name as we want to allow multiple ledger api server
    // instances, and it's pretty difficult to wait for the name to become available
    // again (the name deregistration is asynchronous and the close method is not waiting for
    // it, and it isn't trivial to implement).
    // https://doc.akka.io/docs/akka/2.5/actors.html#graceful-stop
    poolName = s"ledger-api-server-rs-grpc-bridge-${UUID.randomUUID}",
    actorCount = Runtime.getRuntime.availableProcessors() * 8
  )(mat.system)

  private val serverEventLoopGroup = createEventLoopGroup(mat.system.name)

  private val apiServices = createApiServices(mat, serverEsf)

  private val logger = LoggerFactory.getLogger(this.getClass)
  @volatile
  private var actualPort
    : Int = -1 // we need this to remember ephemeral ports when using ResetService
  def port: Int = if (actualPort == -1) serverPort else actualPort

  def getServer = grpcServer

  @volatile
  private var grpcServer: Server = _

  private def start(): LedgerApiServer = {
    val builder = address.fold(NettyServerBuilder.forPort(port))(address =>
      NettyServerBuilder.forAddress(new InetSocketAddress(address, port)))

    sslContext
      .fold {
        logger.info("Starting plainText server")
      } { sslContext =>
        logger.info("Starting TLS server")
        val _ = builder.sslContext(sslContext)
      }

    builder.directExecutor()
    builder.workerEventLoopGroup(serverEventLoopGroup)
    builder.permitKeepAliveTime(10, TimeUnit.SECONDS)
    builder.permitKeepAliveWithoutCalls(true)
    grpcServer = resetService.toList
      .foldLeft(apiServices.services.foldLeft(builder)(_ addService _))(_ addService _)
      .build
    try {
      grpcServer.start()
      actualPort = grpcServer.getPort
    } catch {
      case io: IOException if io.getCause != null && io.getCause.isInstanceOf[BindException] =>
        throw new UnableToBind(port, io.getCause)
    }
    logger.info(s"listening on ${address.getOrElse("localhost")}:${grpcServer.getPort}")
    this
  }

  private def createEventLoopGroup(threadPoolName: String): NioEventLoopGroup = {
    val threadFactory =
      new DefaultThreadFactory(s"$threadPoolName-grpc-eventloop-${UUID.randomUUID}", true)
    val parallelism = Runtime.getRuntime.availableProcessors
    new NioEventLoopGroup(parallelism, threadFactory)
  }

  override def close(): Unit = {
    apiServices.close()
    Option(grpcServer).foreach { s =>
      s.shutdownNow()
      s.awaitTermination(10, TimeUnit.SECONDS)
      grpcServer = null
    }
    // `shutdownGracefully` has a "quiet period" which specifies a time window in which
    // _no requests_ must be witnessed before shutdown is _initiated_. Here we want to
    // start immediately, so no quiet period -- by default it's 2 seconds.
    // Moreover, there's also a "timeout" parameter
    // which caps the time to wait for the quiet period to be fullfilled. Since we have
    // no quiet period, this can also be 0.
    // See <https://netty.io/4.1/api/io/netty/util/concurrent/EventExecutorGroup.html#shutdownGracefully-long-long-java.util.concurrent.TimeUnit->.
    // The 10 seconds to wait is sort of arbitrary, it's long enough to be noticeable though.
    Option(serverEventLoopGroup).foreach(
      _.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS).await(10L, TimeUnit.SECONDS))
    Option(serverEsf).foreach(_.close())
  }

}
