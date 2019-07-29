// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.acceptance.infrastructure

import java.util.concurrent.TimeUnit

import com.digitalasset.platform.sandbox.SandboxServer
import com.digitalasset.platform.sandbox.config.SandboxConfig
import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Try}

private[acceptance] sealed abstract case class LedgerSession(
    configuration: LedgerSessionConfiguration) {

  protected val channel: ManagedChannel

  def close(): Unit

  lazy val services = new LedgerServices(channel)

}

private[acceptance] object LedgerSession {

  private val logger = LoggerFactory.getLogger(classOf[LedgerSession])

  final case class UnsupportedConfiguration(configuration: LedgerSessionConfiguration)
      extends RuntimeException {
    override val getMessage: String =
      s"Unsupported configuration to instantiate the channel ($configuration)"
  }

  private[this] val channels = TrieMap.empty[LedgerSessionConfiguration, LedgerSession]

  private def create(configuration: LedgerSessionConfiguration): Try[LedgerSession] =
    configuration match {
      case LedgerSessionConfiguration.Managed(config) if config.jdbcUrl.isDefined =>
        Failure(new IllegalArgumentException("The Postgres-backed sandbox is not yet supported"))
      case LedgerSessionConfiguration.Managed(config) if config.port != 0 =>
        Failure(new IllegalArgumentException("The sandbox port must be 0"))
      case LedgerSessionConfiguration.Managed(config) =>
        Try(spinUpManagedSandbox(config))
      case _ =>
        Failure(UnsupportedConfiguration(configuration))
    }

  def getOrCreate(configuration: LedgerSessionConfiguration): Try[LedgerSession] =
    Try(channels.getOrElseUpdate(configuration, create(configuration).get))

  def closeAll(): Unit =
    for ((_, session) <- channels) {
      session.close()
    }

  private def spinUpManagedSandbox(config: SandboxConfig): LedgerSession = {
    logger.debug("Starting a new managed ledger session...")
    val threadFactoryPoolName = "grpc-event-loop"
    val daemonThreads = true
    val threadFactory: DefaultThreadFactory =
      new DefaultThreadFactory(threadFactoryPoolName, daemonThreads)
    logger.trace(
      s"gRPC thread factory instantiated with pool '$threadFactoryPoolName' (daemon threads: $daemonThreads)")
    val threadCount = Runtime.getRuntime.availableProcessors
    val eventLoopGroup: NioEventLoopGroup =
      new NioEventLoopGroup(threadCount, threadFactory)
    logger.trace(
      s"gRPC event loop thread group instantiated with $threadCount threads using pool '$threadFactoryPoolName'")
    val sandbox = SandboxServer(config)
    logger.trace(s"Sandbox started on port ${sandbox.port}!")
    val managedChannel = NettyChannelBuilder
      .forAddress("127.0.0.1", sandbox.port)
      .eventLoopGroup(eventLoopGroup)
      .usePlaintext()
      .directExecutor()
      .build()
    logger.trace("Sandbox communication channel instantiated")
    new LedgerSession(LedgerSessionConfiguration.Managed(config.copy(port = sandbox.port))) {
      private[this] val logger = LoggerFactory.getLogger(classOf[LedgerSession])
      override final val channel: ManagedChannel = managedChannel
      override final def close(): Unit = {
        logger.trace("Closing managed ledger session...")
        channel.shutdownNow()
        if (!channel.awaitTermination(5L, TimeUnit.SECONDS)) {
          sys.error(
            "Unable to shutdown channel to a remote API under tests. Unable to recover. Terminating.")
        }
        logger.trace("Sandbox communication channel shut down.")
        if (!eventLoopGroup
            .shutdownGracefully(0, 0, TimeUnit.SECONDS)
            .await(10L, TimeUnit.SECONDS)) {
          sys.error("Unable to shutdown event loop. Unable to recover. Terminating.")
        }
        logger.trace("Sandbox event loop group shut down.")
        sandbox.close()
        logger.trace("Sandbox fully shut down.")
      }
    }
  }

}
