// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.GrpcServiceInvocationMethod
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  ClientChannelBuilder,
  GrpcClient,
  GrpcError,
  GrpcManagedChannel,
  ManagedChannelBuilderProxy,
}
import com.digitalasset.canton.sequencing.ConnectionX.{
  ConnectionXConfig,
  ConnectionXError,
  ConnectionXHealth,
  ConnectionXState,
}
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import io.grpc.Channel
import io.grpc.stub.AbstractStub

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContextExecutor, Future, blocking}

/** Connection specialized for gRPC transport.
  */
final case class GrpcConnectionX(
    config: ConnectionXConfig,
    override val timeouts: ProcessingTimeout,
    protected override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends ConnectionX
    with PrettyPrinting {

  private val channelRef = new AtomicReference[Option[GrpcManagedChannel]](None)

  override val health: ConnectionXHealth = new ConnectionXHealth(
    name = name,
    associatedOnShutdownRunner = this,
    logger = logger,
  )

  override def name: String = s"connection-${config.name}"

  override def start()(implicit traceContext: TraceContext): Unit = blocking {
    synchronized {
      channelRef.get match {
        case Some(_) => logger.warn("Starting an already-started connection. Ignoring.")

        case None =>
          val clientChannelBuilder = ClientChannelBuilder(loggerFactory)
          val builder = mkChannelBuilder(clientChannelBuilder, config.tracePropagation)
          val channel = GrpcManagedChannel(
            s"GrpcConnectionX-$name",
            builder.build(),
            associatedShutdownRunner = this,
            logger,
          )

          channelRef.set(Some(channel))
          health.reportHealthState(ConnectionXState.Started)
      }
    }
  }

  override def stop()(implicit traceContext: TraceContext): Unit = blocking {
    synchronized {
      channelRef.get match {
        case Some(_) =>
          closeChannel()
          health.reportHealthState(ConnectionXState.Stopped)

        // Not logging at WARN level because concurrent calls may happen (e.g. at closing time)
        case None => logger.info("Stopping an already-stopped connection. Ignoring.")
      }
    }
  }

  override def onClosed(): Unit = closeChannel()

  private def closeChannel(): Unit = blocking {
    synchronized {
      channelRef.getAndSet(None).foreach(LifeCycle.close(_)(logger))
    }
  }

  @GrpcServiceInvocationMethod
  def sendRequest[Svc <: AbstractStub[Svc], Res](
      requestDescription: String,
      stubFactory: Channel => Svc,
      logPolicy: CantonGrpcUtil.GrpcLogPolicy = CantonGrpcUtil.DefaultGrpcLogPolicy,
      retryPolicy: GrpcError => Boolean,
  )(
      send: Svc => Future[Res]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, ConnectionXError, Res] =
    // We don't need to add synchronization in this method, because:
    // 1. If the channel gets closed during the call, the call will fail with an appropriate gRPC error.
    // 2. The gRPC channels and stubs are thread-safe.
    channelRef.get() match {
      case Some(channel) =>
        val client = GrpcClient.create(channel, stubFactory)

        CantonGrpcUtil
          .sendGrpcRequest(client, s"server-${config.name}")(
            send = send,
            requestDescription = requestDescription,
            timeout = timeouts.network.unwrap,
            logger = logger,
            logPolicy = logPolicy,
            retryPolicy = retryPolicy,
          )
          .leftMap(ConnectionXError.TransportError.apply)

      case None =>
        EitherT.leftT[FutureUnlessShutdown, Res](
          ConnectionXError.InvalidStateError("Connection is not started")
        )
    }

  private def mkChannelBuilder(
      clientChannelBuilder: ClientChannelBuilder,
      tracePropagation: TracingConfig.Propagation,
  )(implicit
      executor: Executor
  ): ManagedChannelBuilderProxy = ManagedChannelBuilderProxy(
    clientChannelBuilder
      .create(
        NonEmpty.mk(Seq, config.endpoint),
        config.transportSecurity,
        executor,
        config.customTrustCertificates,
        tracePropagation,
      )
  )

  override protected def pretty: Pretty[GrpcConnectionX] =
    prettyOfString(conn => s"Connection ${conn.name.singleQuoted}")
}
