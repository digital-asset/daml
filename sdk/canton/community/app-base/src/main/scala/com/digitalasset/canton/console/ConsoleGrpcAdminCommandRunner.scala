// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import cats.data.EitherT
import com.digitalasset.canton.admin.api.client.GrpcCtlRunner
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  CustomClientTimeout,
  DefaultBoundedTimeout,
  DefaultUnboundedTimeout,
  ServerEnforcedTimeout,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.config.{ClientConfig, ConsoleCommandTimeout, NonNegativeDuration}
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.lifecycle.Lifecycle.CloseableChannel
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, ClientChannelBuilder}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import io.opentelemetry.api.trace.Tracer

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContextExecutor, Future, blocking}

/** Attempt to run a grpc admin-api command against whatever is pointed at in the config
  * @param environment the environment to run in
  * @param commandTimeouts the timeouts to use for the commands
  * @param apiName the name of the api to check against the grpc server-side
  */
class GrpcAdminCommandRunner(
    environment: Environment,
    val commandTimeouts: ConsoleCommandTimeout,
    apiName: String,
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
      case _: FiniteDuration =>
        config.NonNegativeDuration.tryFromDuration(awaitTimeout.duration * 0.9)
      case _ => awaitTimeout
    }

    val resultET = for {
      _ <- {
        channels.get((instanceName, clientConfig.address, clientConfig.port)) match {
          case Some(_) =>
            EitherT.pure[Future, String](())
          case None =>
            logger.debug(
              s"Checking the endpoint at $clientConfig for $instanceName to provide the API '$apiName'"
            )
            CantonGrpcUtil
              .checkCantonApiInfo(
                serverName = instanceName,
                expectedName = apiName,
                channel = ClientChannelBuilder.createChannelToTrustedServer(clientConfig),
                logger = logger,
                timeout = commandTimeouts.bounded,
              )
        }
      }
      closeableChannel = getOrCreateChannel(instanceName, clientConfig, callTimeout)
      _ = logger.debug(s"Running command $command on $instanceName against $clientConfig")
      result <- grpcRunner.run(
        instanceName,
        command,
        closeableChannel.channel,
        token,
        callTimeout.duration,
      )
    } yield result
    (
      awaitTimeout,
      resultET,
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
      callTimeout: config.NonNegativeDuration,
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

/** A console-specific version of the GrpcAdminCommandRunner that uses the console environment
  * @param consoleEnvironment the console environment to run in
  * @param apiName the name of the api to check against the grpc server-side
  */
class ConsoleGrpcAdminCommandRunner(consoleEnvironment: ConsoleEnvironment, apiName: String)
    extends GrpcAdminCommandRunner(
      consoleEnvironment.environment,
      consoleEnvironment.commandTimeouts,
      apiName,
    )(consoleEnvironment.tracer)
