// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.config.{
  ApiLoggingConfig,
  ClientConfig,
  ConsoleCommandTimeout,
  NonNegativeDuration,
}
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.lifecycle.OnShutdownRunner
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.{
  CantonGrpcUtil,
  ClientChannelBuilder,
  GrpcManagedChannel,
}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import io.opentelemetry.api.trace.Tracer

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{ExecutionContextExecutor, Future, blocking}

/** Attempt to run a grpc admin-api command against whatever is pointed at in the config
  * @param commandTimeouts
  *   callback for the timeouts to use for the commands, can be changed dynamically in console env
  * @param apiName
  *   the name of the api to check against the grpc server-side
  */
class GrpcAdminCommandRunner(
    commandTimeouts: => ConsoleCommandTimeout,
    apiName: String,
    apiLoggingConfig: ApiLoggingConfig,
    override val loggerFactory: NamedLoggerFactory,
)(implicit tracer: Tracer, executionContext: ExecutionContextExecutor)
    extends NamedLogging
    with AutoCloseable
    with OnShutdownRunner
    with Spanning {

  private val grpcRunner = new GrpcCtlRunner(
    apiLoggingConfig.maxMessageLines,
    apiLoggingConfig.maxStringLength,
    loggerFactory,
  )
  private val channels = TrieMap[(String, String, Port), GrpcManagedChannel]()

  def runCommandAsync[Result](
      instanceName: String,
      command: GrpcAdminCommand[_, _, Result],
      clientConfig: ClientConfig,
      token: Option[String],
  )(implicit traceContext: TraceContext): (NonNegativeDuration, EitherT[Future, String, Result]) = {
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
            CantonGrpcUtil.shutdownAsGrpcErrorE(
              CantonGrpcUtil.checkCantonApiInfo(
                serverName = instanceName,
                expectedName = apiName,
                channelBuilder =
                  ClientChannelBuilder.createChannelBuilderToTrustedServer(clientConfig),
                logger = logger,
                timeout = commandTimeouts.bounded,
                hasRunOnClosing = this,
                token = token,
              )
            )
        }
      }
      channel = getOrCreateChannel(instanceName, clientConfig)
      _ = logger.debug(s"Running command $command on $instanceName against $clientConfig")
      result <- grpcRunner.run(
        instanceName,
        command,
        channel,
        token,
        callTimeout.duration,
      )
    } yield result
    (
      awaitTimeout,
      resultET,
    )
  }

  def runCommandWithExistingTrace[Result](
      instanceName: String,
      command: GrpcAdminCommand[_, _, Result],
      clientConfig: ClientConfig,
      token: Option[String],
  )(implicit traceContext: TraceContext): ConsoleCommandResult[Result] = {
    val (awaitTimeout, commandET) = runCommandAsync(instanceName, command, clientConfig, token)
    val apiResult =
      awaitTimeout.await(
        s"Running on $instanceName command $command against $clientConfig"
      )(
        commandET.value
      )
    // convert to a console command result
    apiResult.toResult
  }

  def runCommand[Result](
      instanceName: String,
      command: GrpcAdminCommand[_, _, Result],
      clientConfig: ClientConfig,
      token: Option[String],
  ): ConsoleCommandResult[Result] =
    withNewTrace[ConsoleCommandResult[Result]](command.fullName) { implicit traceContext => span =>
      span.setAttribute("instance_name", instanceName)
      runCommandWithExistingTrace(instanceName, command, clientConfig, token)
    }

  private def getOrCreateChannel(
      instanceName: String,
      clientConfig: ClientConfig,
  ): GrpcManagedChannel =
    blocking(synchronized {
      val addr = (instanceName, clientConfig.address, clientConfig.port)
      channels.getOrElseUpdate(
        addr,
        GrpcManagedChannel(
          s"ConsoleCommand",
          ClientChannelBuilder.createChannelBuilderToTrustedServer(clientConfig).build(),
          this,
          logger,
        ),
      )
    })

  override def close(): Unit = super.close()
  override def onFirstClose(): Unit =
    closeChannels()

  def closeChannels(): Unit = blocking(synchronized {
    channels.values.foreach(_.close())
    channels.clear()
  })
}

object GrpcAdminCommandRunner {
  def apply(
      environment: Environment,
      apiName: String,
  ): GrpcAdminCommandRunner =
    new GrpcAdminCommandRunner(
      environment.config.parameters.timeouts.console,
      apiName,
      environment.config.monitoring.logging.api,
      environment.loggerFactory,
    )(environment.tracerProvider.tracer, environment.executionContext)

  def apply(consoleEnvironment: ConsoleEnvironment, apiName: String): GrpcAdminCommandRunner =
    new GrpcAdminCommandRunner(
      consoleEnvironment.commandTimeouts,
      apiName,
      consoleEnvironment.environment.config.monitoring.logging.api,
      consoleEnvironment.environment.loggerFactory,
    )(consoleEnvironment.tracer, consoleEnvironment.environment.executionContext)

}
