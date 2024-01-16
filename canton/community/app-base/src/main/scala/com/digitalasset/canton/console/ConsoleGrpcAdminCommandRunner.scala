// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.admin.api.client.GrpcCtlRunner
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  CustomClientTimeout,
  DefaultBoundedTimeout,
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
}
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{ClientConfig, ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.lifecycle.Lifecycle.CloseableChannel
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.ClientChannelBuilder
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.TimeUnit
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContextExecutor, blocking}

/** Attempt to run a grpc admin-api command against whatever is pointed at in the config
  */
class GrpcAdminCommandRunner(
    environment: Environment,
    val commandTimeouts: ConsoleCommandTimeout,
)(implicit tracer: Tracer)
    extends NamedLogging
    with AutoCloseable
    with Spanning {

  private implicit val executionContext: ExecutionContextExecutor =
    environment.executionContext
  override val loggerFactory: NamedLoggerFactory = environment.loggerFactory

  private val grpcRunner = new GrpcCtlRunner(
    environment.config.monitoring.logging.api.maxMessageLines,
    environment.config.monitoring.logging.api.maxStringLength,
    loggerFactory,
  )
  private val channels = TrieMap[(String, String, Port), CloseableChannel]()

  def runCommandAsync[Result](
      instanceName: String,
      command: GrpcAdminCommand[_, _, Result],
      clientConfig: ClientConfig,
      token: Option[String],
  )(implicit traceContext: TraceContext) = {
    val awaitTimeout = command.timeoutType match {
      case CustomClientTimeout(timeout) => timeout
      // If a custom timeout for a console command is set, it involves some non-gRPC timeout mechanism
      // -> we set the gRPC timeout to Inf, so gRPC never times out before the other timeout mechanism
      case ServerEnforcedTimeout => NonNegativeDuration(Duration.Inf)
      case DefaultBoundedTimeout => commandTimeouts.bounded
      case DefaultUnboundedTimeout => commandTimeouts.unbounded
    }
    val callTimeout = awaitTimeout.duration match {
      // Abort the command shortly before the console times out, to get a better error message
      case x: FiniteDuration => Duration((x.toMillis * 9) / 10, TimeUnit.MILLISECONDS)
      case x => x
    }
    val closeableChannel = getOrCreateChannel(instanceName, clientConfig)
    logger.debug(s"Running on ${instanceName} command ${command} against ${clientConfig}")(
      traceContext
    )
    (
      awaitTimeout,
      grpcRunner.run(instanceName, command, closeableChannel.channel, token, callTimeout),
    )
  }

  def runCommand[Result](
      instanceName: String,
      command: GrpcAdminCommand[_, _, Result],
      clientConfig: ClientConfig,
      token: Option[String],
  ): ConsoleCommandResult[Result] =
    withNewTrace[ConsoleCommandResult[Result]](command.fullName) { implicit traceContext => span =>
      span.setAttribute("instance_name", instanceName)
      val (awaitTimeout, commandET) = runCommandAsync(instanceName, command, clientConfig, token)
      val apiResult =
        awaitTimeout.await(
          s"Running on ${instanceName} command ${command} against ${clientConfig}"
        )(
          commandET.value
        )
      // convert to a console command result
      apiResult.toResult
    }

  private def getOrCreateChannel(
      instanceName: String,
      clientConfig: ClientConfig,
  ): CloseableChannel =
    blocking(synchronized {
      val addr = (instanceName, clientConfig.address, clientConfig.port)
      channels.getOrElseUpdate(
        addr,
        new CloseableChannel(
          ClientChannelBuilder.createChannelToTrustedServer(clientConfig),
          logger,
          s"ConsoleCommand",
        ),
      )
    })

  override def close(): Unit = {
    closeChannels()
  }

  def closeChannels(): Unit = {
    channels.values.foreach(_.close())
    channels.clear()
  }
}

class ConsoleGrpcAdminCommandRunner(consoleEnvironment: ConsoleEnvironment)
    extends GrpcAdminCommandRunner(
      consoleEnvironment.environment,
      consoleEnvironment.commandTimeouts,
    )(consoleEnvironment.tracer)
