// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.File
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands.NodeStatusCommand
import com.digitalasset.canton.admin.api.client.data.{CantonStatus, NodeStatus}
import com.digitalasset.canton.config.LocalNodeConfig
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.metrics.MetricsSnapshot
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ReleaseVersion
import io.circe.Encoder
import io.circe.syntax.*
import io.grpc.Status as GrpcStatus

import scala.annotation.nowarn
import scala.concurrent.ExecutionContextExecutor

/** Generates a health dump zip file containing information about the current Canton process
  * This is the core of the implementation of the HealthDump gRPC endpoint.
  */
trait HealthDumpGenerator[Status <: CantonStatus] {
  def status(): Status
  def environment: Environment
  def grpcAdminCommandRunner: GrpcAdminCommandRunner

  protected implicit val executionContext: ExecutionContextExecutor =
    environment.executionContext

  protected implicit val statusEncoder: Encoder[Status]

  /** First try to get the status using the node specific endpoint introduced in 2.10
    * If it does not work, get the status from the old/shared status endpoint
    *
    * @param deserializer Deserializer for the old proto (with node specific data encoded in `bytes extra` attribute)
    * @param nodeStatusCommand Command that allows to query the new, node specific status endpoint
    */
  protected def getStatusForNode[S <: NodeStatus.Status](
      nodeName: String,
      nodeConfig: LocalNodeConfig,
      deserializer: v0.NodeStatus.Status => ParsingResult[S],
      nodeStatusCommand: NodeStatusCommand[S, _, _],
  ): NodeStatus[S] = {

    def fallback =
      grpcAdminCommandRunner
        .runCommand(
          nodeName,
          new StatusAdminCommands.GetStatus[S](deserializer),
          nodeConfig.clientAdminApi,
          None,
        )
        .toEither match {
        case Left(value) => CommandSuccessful(NodeStatus.Failure(value))
        case Right(value) => CommandSuccessful(value)
      }

    val commandResult
        : ConsoleCommandResult[Either[GrpcStatus.Code.UNIMPLEMENTED.type, NodeStatus[S]]] =
      grpcAdminCommandRunner
        .runCommand(
          nodeName,
          nodeStatusCommand,
          nodeConfig.clientAdminApi,
          None,
        )

    val result = commandResult.toEither match {
      case Left(errorMessage) => CommandSuccessful(NodeStatus.Failure(errorMessage))
      /* For backward compatibility:
      Assumes getting a left of gRPC code means that the node specific status endpoint
      is not available (because that Canton version does not include it), and thus we
      want to fall back to the original status command.
       */
      case Right(Left(_code)) => fallback
      case Right(Right(nodeStatus)) => CommandSuccessful(nodeStatus)
    }
    result.value
  }

  protected def statusMap[S <: NodeStatus.Status](
      nodes: Map[String, LocalNodeConfig],
      deserializer: v0.NodeStatus.Status => ParsingResult[S],
      nodeStatusCommand: NodeStatusCommand[S, _, _],
  ): Map[String, () => NodeStatus[S]] =
    nodes.map { case (nodeName, nodeConfig) =>
      nodeName -> (() => getStatusForNode[S](nodeName, nodeConfig, deserializer, nodeStatusCommand))
    }

  @nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
  def generateHealthDump(
      outputFile: File,
      extraFilesToZip: Seq[File] = Seq.empty,
  ): File = {
    import io.circe.generic.auto.*
    import CantonHealthAdministrationEncoders.*

    final case class EnvironmentInfo(os: String, javaVersion: String)

    final case class CantonDump(
        releaseVersion: String,
        environment: EnvironmentInfo,
        config: String,
        status: Status,
        metrics: MetricsSnapshot,
        traces: Map[Thread, Array[StackTraceElement]],
    )

    val javaVersion = System.getProperty("java.version")
    val cantonVersion = ReleaseVersion.current.fullVersion
    val env = EnvironmentInfo(sys.props("os.name"), javaVersion)

    val metricsSnapshot = MetricsSnapshot(
      environment.metricsFactory.registry,
      environment.configuredOpenTelemetry.onDemandMetricsReader,
    )
    val config = environment.config.dumpString

    val traces = {
      import scala.jdk.CollectionConverters.*
      Thread.getAllStackTraces.asScala.toMap
    }

    val dump = CantonDump(cantonVersion, env, config, status(), metricsSnapshot, traces)

    val logFile =
      File(
        sys.env
          .get("LOG_FILE_NAME")
          .orElse(sys.props.get("LOG_FILE_NAME")) // This is set in Cli.installLogging
          .getOrElse("log/canton.log")
      )

    val logLastErrorsFile = File(
      sys.env
        .get("LOG_LAST_ERRORS_FILE_NAME")
        .orElse(sys.props.get("LOG_LAST_ERRORS_FILE_NAME"))
        .getOrElse("log/canton_errors.log")
    )

    // This is a guess based on the default logback config as to what the rolling log files look like
    // If we want to be more robust we'd have to access logback directly, extract the pattern from there, and use it to
    // glob files.
    val rollingLogs = logFile.siblings
      .filter { f =>
        f.name.contains(logFile.name) && f.extension.contains(".gz")
      }
      .toSeq
      .sortBy(_.name)
      .take(environment.config.monitoring.dumpNumRollingLogFiles.unwrap)

    File.usingTemporaryFile("canton-dump-", ".json") { tmpFile =>
      tmpFile.append(dump.asJson.spaces2)
      val files = Iterator(logFile, logLastErrorsFile, tmpFile).filter(_.nonEmpty)
      outputFile.zipIn(files ++ extraFilesToZip.iterator ++ rollingLogs)
    }

    outputFile
  }
}
