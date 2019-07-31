// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.rewrite.testtool.infrastructure

import java.util.UUID
import java.util.concurrent.TimeUnit

import io.grpc.ManagedChannel
import io.grpc.netty.NettyChannelBuilder
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.Try

private[testtool] final class LedgerSession private (
    val config: LedgerSessionConfiguration,
    channel: ManagedChannel,
    eventLoopGroup: NioEventLoopGroup)(implicit ec: ExecutionContext) {

  private[this] val logger = LoggerFactory.getLogger(classOf[LedgerSession])

  private[this] val bindings: LedgerBindings = new LedgerBindings(channel)

  private[testtool] def createTestContext(): LedgerTestContext =
    new LedgerTestContext(UUID.randomUUID.toString, bindings)

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

  private def create(config: LedgerSessionConfiguration)(
      implicit ec: ExecutionContext): Try[LedgerSession] = Try {
    logger.info(s"Connecting to ledger at ${config.host}:${config.port}...")
    val threadFactoryPoolName = s"grpc-event-loop-${config.host}-${config.port}"
    val daemonThreads = true
    val threadFactory: DefaultThreadFactory =
      new DefaultThreadFactory(threadFactoryPoolName, daemonThreads)
    logger.info(
      s"gRPC thread factory instantiated with pool '$threadFactoryPoolName' (daemon threads: $daemonThreads)")
    val threadCount = Runtime.getRuntime.availableProcessors
    val eventLoopGroup: NioEventLoopGroup =
      new NioEventLoopGroup(threadCount, threadFactory)
    logger.info(
      s"gRPC event loop thread group instantiated with $threadCount threads using pool '$threadFactoryPoolName'")
    val managedChannel = NettyChannelBuilder
      .forAddress(config.host, config.port)
      .eventLoopGroup(eventLoopGroup)
      .usePlaintext()
      .directExecutor()
      .build()
    logger.info(s"Connection to ledger under test open on ${config.host}:${config.port}")
    new LedgerSession(config, managedChannel, eventLoopGroup)
  }

  def getOrCreate(configuration: LedgerSessionConfiguration)(
      implicit ec: ExecutionContext): Try[LedgerSession] =
    Try(channels.getOrElseUpdate(configuration, create(configuration).get))

  def closeAll(): Unit =
    for ((_, session) <- channels) {
      session.close()
    }

}
