// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.File
import com.digitalasset.canton.admin.api.client.commands.StatusAdminCommands
import com.digitalasset.canton.admin.api.client.data.CantonStatus
import com.digitalasset.canton.config.LocalNodeConfig
import com.digitalasset.canton.console.CommandErrors.CommandError
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.health.admin.data.NodeStatus
import com.digitalasset.canton.health.admin.{data, v30}
import com.digitalasset.canton.metrics.MetricsSnapshot
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.version.ReleaseVersion
import io.circe.Encoder
import io.circe.syntax.*

import scala.annotation.nowarn

/** Generates a health dump zip file containing information about the current Canton process
  * This is the core of the implementation of the HealthDump gRPC endpoint.
  */
trait HealthDumpGenerator[Status <: CantonStatus] {
  def status(): Status
  def environment: Environment
  def grpcAdminCommandRunner: GrpcAdminCommandRunner
  protected implicit val statusEncoder: Encoder[Status]

  protected def getStatusForNode[S <: NodeStatus.Status](
      nodeName: String,
      nodeConfig: LocalNodeConfig,
      deserializer: v30.NodeStatus.Status => ParsingResult[S],
  ): NodeStatus[S] = {
    grpcAdminCommandRunner
      .runCommand(
        nodeName,
        new StatusAdminCommands.GetStatus[S](deserializer),
        nodeConfig.clientAdminApi,
        None,
      ) match {
      case CommandSuccessful(value) => value
      case err: CommandError => data.NodeStatus.Failure(err.cause)
    }
  }

  protected def statusMap[S <: NodeStatus.Status](
      nodes: Map[String, LocalNodeConfig],
      deserializer: v30.NodeStatus.Status => ParsingResult[S],
  ): Map[String, () => NodeStatus[S]] = {
    nodes.map { case (nodeName, nodeConfig) =>
      nodeName -> (() => getStatusForNode[S](nodeName, nodeConfig, deserializer))
    }
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
      environment.configuredOpenTelemetry.onDemandMetricsReader
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
