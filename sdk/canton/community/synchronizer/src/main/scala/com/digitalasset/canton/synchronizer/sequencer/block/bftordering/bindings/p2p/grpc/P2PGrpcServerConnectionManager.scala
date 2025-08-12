// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.P2PGrpcNetworking.completeGrpcStreamObserver
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.utils.Miscellaneous.mutex
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.BftOrderingServiceReceiveResponse
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.stub.StreamObserver

import scala.collection.mutable

final class P2PGrpcServerConnectionManager(
    maybeServerUS: Option[UnlessShutdown[LifeCycle.CloseableServer]],
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging
    with FlagCloseable { self =>

  import TraceContext.Implicits.Empty.emptyTraceContext

  private val peerSenders = mutable.Set[StreamObserver[BftOrderingServiceReceiveResponse]]()

  def startServer(): Unit =
    if (!isClosing)
      maybeServerUS.foreach(_.foreach(_.server.start().discard)).discard
    else
      logger.info("Not starting P2P gRPC server due to shutdown")

  // Called by the gRPC server when receiving a connection
  def addPeerSender(peerSender: StreamObserver[BftOrderingServiceReceiveResponse]): Unit =
    mutex(this) {
      peerSenders.add(peerSender).discard
    }

  // Called by the gRPC server endpoint when receiving an error or a completion from a client
  def cleanupPeerSender(
      peerSender: StreamObserver[BftOrderingServiceReceiveResponse]
  ): Unit = {
    logger.debug("Completing and removing client endpoint")
    completeGrpcStreamObserver(peerSender)
    mutex(this) {
      peerSenders.remove(peerSender).discard
    }
  }

  private def shutdownGrpcServers(): Unit =
    maybeServerUS.foreach(_.foreach { serverHandle =>
      logger.info(s"Shutting down gRPC server")
      shutdownGrpcServer(serverHandle)
    })

  private def shutdownGrpcServer(server: LifeCycle.CloseableServer): Unit = {
    // https://github.com/grpc/grpc-java/issues/8770
    val serverPort = server.server.getPort
    logger.debug(s"Terminating gRPC server on port $serverPort")
    server.close()
    logger.info(s"Successfully terminated the gRPC server on port $serverPort")
  }

  override def onClosed(): Unit = {
    logger.debug("Closing P2P networking (server role)")
    peerSenders.foreach(cleanupPeerSender)
    shutdownGrpcServers()
    logger.debug("Closed P2P networking (server role)")
  }
}
