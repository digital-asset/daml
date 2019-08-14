// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import java.util.concurrent.TimeUnit

import io.grpc.ManagedChannel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[testtool] final class LedgerSession private (
    val config: LedgerSessionConfiguration,
    channel: ManagedChannel,
    eventLoopGroup: NioEventLoopGroup)(implicit ec: ExecutionContext) {

  private[this] val logger = LoggerFactory.getLogger(classOf[LedgerSession])

  private[this] val services: LedgerServices = new LedgerServices(channel)

  private[this] val bindings: LedgerBindings = new LedgerBindings(services, config.commandTtlFactor)

  private[testtool] def createTestContext(applicationId: String): Future[LedgerTestContext] =
    bindings.ledgerEnd.map(new LedgerTestContext(applicationId, _, bindings))

  private[testtool] def close(): Unit = {
    logger.info(s"Disconnecting from ledger at ${config.host}:${config.port}...")
    channel.shutdownNow()
    if (!channel.awaitTermination(10L, TimeUnit.SECONDS)) {
      sys.error("Channel shutdown stuck. Unable to recover. Terminating.")
    }
    logger.info(s"Connection to ledger under test at ${config.host}:${config.port} shut down.")
    if (!eventLoopGroup
        .shutdownGracefully(0, 0, TimeUnit.SECONDS)
        .await(10L, TimeUnit.SECONDS)) {
      sys.error("Unable to shutdown event loop. Unable to recover. Terminating.")
    }
    logger.info(s"Ledger connection to ${config.host}:${config.port} closed.")
  }

}

private[testtool] object LedgerSession {

  private val logger = LoggerFactory.getLogger(classOf[LedgerSession])

  private[this] val channels = TrieMap.empty[LedgerSessionConfiguration, LedgerSession]

  @throws[RuntimeException]
  private def create(config: LedgerSessionConfiguration)(
      implicit ec: ExecutionContext): LedgerSession = {
    logger.info(s"Connecting to ledger at ${config.host}:${config.port}...")
    val threadFactoryPoolName = s"grpc-event-loop-${config.host}-${config.port}"
    val daemonThreads = false
    val threadFactory: DefaultThreadFactory =
      new DefaultThreadFactory(threadFactoryPoolName, daemonThreads)
    logger.info(
      s"gRPC thread factory instantiated with pool '$threadFactoryPoolName' (daemon threads: $daemonThreads)")
    val threadCount = Runtime.getRuntime.availableProcessors
    val eventLoopGroup: NioEventLoopGroup =
      new NioEventLoopGroup(threadCount, threadFactory)
    logger.info(
      s"gRPC event loop thread group instantiated with $threadCount threads using pool '$threadFactoryPoolName'")
    val managedChannelBuilder = NettyChannelBuilder
      .forAddress(config.host, config.port)
      .eventLoopGroup(eventLoopGroup)
      .channelType(classOf[NioSocketChannel])
      .directExecutor()
      .usePlaintext()
    for (ssl <- config.ssl; sslContext <- ssl.client) {
      logger.info("Setting up managed communication channel with transport security")
      managedChannelBuilder
        .useTransportSecurity()
        .sslContext(sslContext)
        .negotiationType(NegotiationType.TLS)
    }
    val managedChannel = managedChannelBuilder.build()
    logger.info(s"Connection to ledger under test open on ${config.host}:${config.port}")
    new LedgerSession(config, managedChannel, eventLoopGroup)
  }

  def getOrCreate(configuration: LedgerSessionConfiguration)(
      implicit ec: ExecutionContext): Try[LedgerSession] =
    Try(channels.getOrElseUpdate(configuration, create(configuration)))

  def close(configuration: LedgerSessionConfiguration) =
    channels.get(configuration).foreach(_.close())

  def closeAll(): Unit =
    for ((_, session) <- channels) {
      session.close()
    }

}
