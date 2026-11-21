// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import better.files.File
import com.digitalasset.canton.admin.api.client.commands.{
  GrpcAdminCommand,
  MediatorAdminCommands,
  ParticipantAdminCommands,
  SequencerAdminCommands,
  TopologyAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.{
  CantonStatus,
  DynamicSynchronizerParameters as ConsoleDynamicSynchronizerParameters,
  NodeStatus,
}
import com.digitalasset.canton.config.LocalNodeConfig
import com.digitalasset.canton.console.CommandErrors.CommandError
import com.digitalasset.canton.console.HealthDumpGenerator.ParametersWithValidity
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricsSnapshot
import com.digitalasset.canton.topology.PhysicalSynchronizerId
import com.digitalasset.canton.topology.admin.grpc.BaseQuery
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId.Synchronizer
import com.digitalasset.canton.topology.store.TimeQuery
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.version.ReleaseVersion
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.*

import java.time.Instant

/** Generates a health dump zip file containing information about the current Canton process This is
  * the core of the implementation of the HealthDump gRPC endpoint.
  */
class HealthDumpGenerator(
    val environment: Environment,
    val grpcAdminCommandRunner: GrpcAdminCommandRunner,
    override val loggerFactory: NamedLoggerFactory,
) extends NamedLogging {
  private implicit val statusEncoder: Encoder[CantonStatus] = {
    import io.circe.generic.auto.*
    import CantonHealthAdministrationEncoders.*
    deriveEncoder[CantonStatus]
  }

  private implicit val dynamicSynchronizerParametersEncoder
      : Encoder[ConsoleDynamicSynchronizerParameters] = {
    import io.circe.generic.auto.*
    import CantonHealthAdministrationEncoders.*
    deriveEncoder[ConsoleDynamicSynchronizerParameters]
  }

  def status(): CantonStatus =
    CantonStatus.getStatus(
      statusMap(
        environment.config.sequencersByString,
        SequencerAdminCommands.Health.SequencerStatusCommand(),
      ),
      statusMap(
        environment.config.mediatorsByString,
        MediatorAdminCommands.Health.MediatorStatusCommand(),
      ),
      statusMap(
        environment.config.participantsByString,
        ParticipantAdminCommands.Health.ParticipantStatusCommand(),
      ),
    )

  private def getSynchronizerParametersForNode(
      nodeName: String,
      nodeConfig: LocalNodeConfig,
      synchronizerId: PhysicalSynchronizerId,
  ): List[ParametersWithValidity] =
    grpcAdminCommandRunner
      .runCommand(
        nodeName,
        TopologyAdminCommands.Read.ListSynchronizerParametersState(
          BaseQuery(
            store = Synchronizer(synchronizerId),
            proposals = false,
            TimeQuery.HeadState,
            Some(TopologyChangeOp.Replace),
            filterSigningKey = "",
            protocolVersion = None,
          ),
          filterSynchronizerId = "",
        ),
        nodeConfig.clientAdminApi,
        token = None,
      ) match {
      case CommandSuccessful(value) =>
        value
          .map(result =>
            ParametersWithValidity(
              validFrom = result.context.validFrom,
              validUntil = result.context.validUntil,
              parameters = ConsoleDynamicSynchronizerParameters(result.item),
            )
          )
          .toList
      case err: CommandError =>
        logger.underlying.warn(
          s"Ignoring a failure to retrieve synchronizer parameters for node $nodeName: $err"
        )
        List.empty
    }

  private def getStatusForNode[S <: NodeStatus.Status](
      nodeName: String,
      nodeConfig: LocalNodeConfig,
      nodeStatusCommand: GrpcAdminCommand[?, ?, NodeStatus[S]],
  ): NodeStatus[S] =
    grpcAdminCommandRunner
      .runCommand(
        nodeName,
        nodeStatusCommand,
        nodeConfig.clientAdminApi,
        None,
      ) match {
      case CommandSuccessful(value) => value
      case err: CommandError => NodeStatus.Failure(err.cause)
    }

  private def statusMap[S <: NodeStatus.Status](
      nodes: Map[String, LocalNodeConfig],
      nodeStatusCommand: GrpcAdminCommand[?, ?, NodeStatus[S]],
  ): Map[String, () => NodeStatus[S]] =
    nodes.map { case (nodeName, nodeConfig) =>
      nodeName -> (() => getStatusForNode[S](nodeName, nodeConfig, nodeStatusCommand))
    }

  def generateHealthDump(
      outputFile: File,
      extraFilesToZip: Seq[File] = Seq.empty,
  ): Unit = {
    import io.circe.generic.auto.*
    import CantonHealthAdministrationEncoders.*

    final case class EnvironmentInfo(os: String, javaVersion: String)

    final case class CantonDump(
        releaseVersion: String,
        environment: EnvironmentInfo,
        config: String,
        status: CantonStatus,
        metrics: MetricsSnapshot,
        traces: Map[Thread, Array[StackTraceElement]],
        synchronizerParameters: Map[String, Map[PhysicalSynchronizerId, List[
          ParametersWithValidity
        ]]],
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

    val currentStatus = status()
    val sequencersParameters = environment.config.sequencersByString.flatMap {
      case (nodeName, nodeConfig) =>
        currentStatus.sequencerStatus.get(nodeName).flatMap { status =>
          val synchronizerId = status.synchronizerId
          val parameters = getSynchronizerParametersForNode(
            nodeName,
            nodeConfig,
            synchronizerId,
          )
          Some(nodeName -> Map(synchronizerId -> parameters))
        }
    }

    val mediatorsParameters = environment.config.mediatorsByString.flatMap {
      case (nodeName, nodeConfig) =>
        currentStatus.mediatorStatus.get(nodeName).flatMap { status =>
          val synchronizerId = status.synchronizerId
          val parameters =
            getSynchronizerParametersForNode(
              nodeName,
              nodeConfig,
              synchronizerId,
            )
          Some(nodeName -> Map(synchronizerId -> parameters))
        }
    }

    val participantsParameters = environment.config.participantsByString.flatMap {
      case (nodeName, nodeConfig) =>
        val synchronizerIds = currentStatus.participantStatus.get(nodeName).toList.flatMap {
          status => status.connectedSynchronizers.keys
        }

        Some(nodeName -> synchronizerIds.flatMap { synchronizerId =>
          val parameters = getSynchronizerParametersForNode(
            nodeName,
            nodeConfig,
            synchronizerId,
          )
          Some((synchronizerId -> parameters))
        }.toMap)
    }

    val allParameters: Map[String, Map[PhysicalSynchronizerId, List[ParametersWithValidity]]] =
      sequencersParameters ++ mediatorsParameters ++ participantsParameters

    val dump =
      CantonDump(cantonVersion, env, config, currentStatus, metricsSnapshot, traces, allParameters)

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
  }
}

object HealthDumpGenerator {
  private final case class ParametersWithValidity(
      validFrom: Instant,
      validUntil: Option[Instant],
      parameters: ConsoleDynamicSynchronizerParameters,
  )
}
