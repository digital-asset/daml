// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

private[infrastructure] final class ParticipantSessionManager(
    sessions: immutable.Map[ParticipantSessionConfiguration, ParticipantSession]
) {
  lazy val all: immutable.Seq[ParticipantSession] = sessions.values.toVector

  def get(configuration: ParticipantSessionConfiguration): ParticipantSession =
    sessions(configuration)

  def closeAll(): Unit =
    for ((_, session) <- sessions) {
      session.close()
    }
}

object ParticipantSessionManager {
  private val logger = LoggerFactory.getLogger(classOf[ParticipantSession])

  def apply(configs: immutable.Seq[ParticipantSessionConfiguration])(
      implicit executionContext: ExecutionContext
  ): Future[ParticipantSessionManager] =
    for {
      participantSessions <- Future
        .traverse(configs)(config => create(config).map(config -> _))
        .map(_.toMap)
    } yield new ParticipantSessionManager(participantSessions)

  private def create(
      config: ParticipantSessionConfiguration,
  )(implicit ec: ExecutionContext): Future[ParticipantSession] = {
    logger.info(s"Connecting to participant at ${config.host}:${config.port}...")
    val threadFactoryPoolName = s"grpc-event-loop-${config.host}-${config.port}"
    val daemonThreads = false
    val threadFactory: DefaultThreadFactory =
      new DefaultThreadFactory(threadFactoryPoolName, daemonThreads)
    logger.info(
      s"gRPC thread factory instantiated with pool '$threadFactoryPoolName' (daemon threads: $daemonThreads)",
    )
    val threadCount = Runtime.getRuntime.availableProcessors
    val eventLoopGroup: NioEventLoopGroup =
      new NioEventLoopGroup(threadCount, threadFactory)
    logger.info(
      s"gRPC event loop thread group instantiated with $threadCount threads using pool '$threadFactoryPoolName'",
    )
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
    managedChannelBuilder.maxInboundMessageSize(10000000)
    val managedChannel = managedChannelBuilder.build()
    logger.info(s"Connected to participant at ${config.host}:${config.port}.")
    ParticipantSession(config, managedChannel, eventLoopGroup)
  }
}
