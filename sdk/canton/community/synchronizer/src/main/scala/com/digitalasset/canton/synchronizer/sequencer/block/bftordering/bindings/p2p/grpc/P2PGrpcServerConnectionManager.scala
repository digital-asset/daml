// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.p2p.grpc.GrpcNetworking.{
  completeHandle,
  mutex,
}
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

  private val clientHandles = mutable.Set[StreamObserver[BftOrderingServiceReceiveResponse]]()

  def startServer(): Unit = maybeServerUS.foreach(_.foreach(_.server.start().discard)).discard

  // Called by the gRPC server when receiving a connection
  def addClientHandle(clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]): Unit =
    mutex(this) {
      clientHandles.add(clientEndpoint).discard
    }

  // Called by the gRPC server endpoint when receiving an error or a completion from a client
  def cleanupClientHandle(
      clientEndpoint: StreamObserver[BftOrderingServiceReceiveResponse]
  ): Unit = {
    logger.debug("Completing and removing client endpoint")
    completeHandle(clientEndpoint)
    mutex(this) {
      clientHandles.remove(clientEndpoint).discard
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
    clientHandles.foreach(cleanupClientHandle)
    shutdownGrpcServers()
    logger.debug("Closed P2P networking (server role)")
  }
}
