// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure.participant

import com.daml.ledger.api.testtool.infrastructure.participant.ParticipantSessionManager._
import io.grpc.ManagedChannel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
import org.slf4j.LoggerFactory

import scala.collection.immutable
import scala.concurrent.duration.SECONDS
import scala.concurrent.{ExecutionContext, Future}

private[infrastructure] final class ParticipantSessionManager(
    sessions: immutable.Map[ParticipantSessionConfiguration, SessionParts],
) {
  lazy val all: immutable.Seq[ParticipantSession] = sessions.values.toVector.map(_._1)

  def get(configuration: ParticipantSessionConfiguration): ParticipantSession =
    sessions(configuration)._1

  def closeAll(): Unit =
    for ((config, (_, channel, eventLoopGroup)) <- sessions) {
      logger.info(s"Disconnecting from participant at ${config.address}...")
      channel.shutdownNow()
      if (!channel.awaitTermination(10L, SECONDS)) {
        sys.error("Channel shutdown stuck. Unable to recover. Terminating.")
      }
      logger.info(s"Connection to participant at ${config.address} shut down.")
      if (!eventLoopGroup
          .shutdownGracefully(0, 0, SECONDS)
          .await(10L, SECONDS)) {
        sys.error("Unable to shutdown event loop. Unable to recover. Terminating.")
      }
      logger.info(s"Connection to participant at ${config.address} closed.")
    }
}

object ParticipantSessionManager {
  private type SessionParts = (ParticipantSession, ManagedChannel, EventLoopGroup)

  private val logger = LoggerFactory.getLogger(classOf[ParticipantSession])

  def apply(configs: immutable.Seq[ParticipantSessionConfiguration])(
      implicit executionContext: ExecutionContext
  ): Future[ParticipantSessionManager] =
    for {
      participantSessions <- Future
        .traverse(configs)(config => connect(config).map(config -> _))
        .map(_.toMap)
    } yield new ParticipantSessionManager(participantSessions)

  private def connect(
      config: ParticipantSessionConfiguration,
  )(implicit ec: ExecutionContext): Future[SessionParts] = {
    logger.info(s"Connecting to participant at ${config.address}...")
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
    val channelBuilder = NettyChannelBuilder
      .forAddress(config.host, config.port)
      .eventLoopGroup(eventLoopGroup)
      .channelType(classOf[NioSocketChannel])
      .directExecutor()
      .usePlaintext()
    for (ssl <- config.ssl; sslContext <- ssl.client) {
      logger.info("Setting up managed communication channel with transport security.")
      channelBuilder
        .useTransportSecurity()
        .sslContext(sslContext)
        .negotiationType(NegotiationType.TLS)
    }
    channelBuilder.maxInboundMessageSize(10000000)
    val channel = channelBuilder.build()
    logger.info(s"Connected to participant at ${config.address}.")
    ParticipantSession(config, channel).map(session => (session, channel, eventLoopGroup))
  }
}
