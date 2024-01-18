// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import org.apache.pekko.actor.ActorSystem
import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import com.daml.SdkVersion
import com.daml.fs.Utils.deleteRecursively
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.daml.ledger.api.refinements.ApiTypes.Party
import com.digitalasset.canton.ledger.api.tls.TlsConfiguration
import com.daml.lf.engine.script.{ParticipantMode, RunnerMain, RunnerMainConfig}
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.google.protobuf.ByteString

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.Duration

case class ExampleExportClientConfig(
    darPath: File,
    targetPort: Int,
    scriptIdentifier: String,
    parties: Seq[Party],
    outputExportDaml: Path,
    outputArgsJson: Path,
    outputDamlYaml: Path,
)

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
object ExampleExportClientConfig {
  def parse(args: Array[String]): Option[ExampleExportClientConfig] =
    parser.parse(
      args,
      ExampleExportClientConfig(
        darPath = null,
        targetPort = -1,
        scriptIdentifier = null,
        parties = Seq.empty[Party],
        outputExportDaml = null,
        outputArgsJson = null,
        outputDamlYaml = null,
      ),
    )

  private def parseExportOut(
      envVar: String
  ): Either[String, ExampleExportClientConfig => ExampleExportClientConfig] = {
    envVar.split(" ").map(s => Paths.get(s)) match {
      case Array(export_daml, args_json, daml_yaml) =>
        Right(c =>
          c.copy(
            outputExportDaml = export_daml,
            outputArgsJson = args_json,
            outputDamlYaml = daml_yaml,
          )
        )
      case _ => Left("Environment variable EXPORT_OUT must contain three paths")
    }
  }

  private val parser = new scopt.OptionParser[ExampleExportClientConfig]("script-export") {
    help("help")
      .text("Show this help message.")
    opt[Int]("target-port")
      .required()
      .action((x, c) => c.copy(targetPort = x))
      .text("Daml ledger port to connect to.")
    opt[String]("script-identifier")
      .required()
      .action((x, c) => c.copy(scriptIdentifier = x))
      .text("Daml script to run for export.")
    opt[Seq[String]]("party")
      .unbounded()
      .action((x, c) => c.copy(parties = c.parties ++ Party.subst(x.toList)))
      .text(
        "Export ledger state as seen by these parties. " +
          "Pass --party multiple times or use a comma-separated list of party names to specify multiple parties."
      )
    opt[String]("output")
      .hidden()
      .withFallback(() => sys.env.getOrElse("EXPORT_OUT", ""))
      .validate(x => parseExportOut(x).map(_ => ()))
      .action { (x, c) =>
        parseExportOut(x) match {
          case Left(msg) =>
            throw new RuntimeException(s"Failed to validate EXPORT_OUT environment variable: $msg")
          case Right(f) => f(c)
        }
      }
    arg[File]("dar")
      .required()
      .action((f, c) => c.copy(darPath = f))
      .text("Path to the dar file containing the initialization script")
  }
}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
object ExampleExportClient {
  def main(args: Array[String]): Unit = {
    ExampleExportClientConfig.parse(args) match {
      case Some(clientConfig) => main(clientConfig)
      case None => sys.exit(1)
    }
  }
  def main(clientConfig: ExampleExportClientConfig): Unit = {
    implicit val system: ActorSystem = ActorSystem("ScriptRunner")
    implicit val sequencer: ExecutionSequencerFactory =
      new PekkoExecutionSequencerPool("ScriptRunnerPool")(system)
    implicit val ec: ExecutionContext = system.dispatcher
    val hostIp = "localhost"
    val port = clientConfig.targetPort

    val config = RunnerMainConfig(
      darPath = clientConfig.darPath,
      runMode = RunnerMainConfig.RunMode.RunOne(clientConfig.scriptIdentifier, None, None),
      participantMode = ParticipantMode.RemoteParticipantHost(hostIp, port),
      timeMode = RunnerMainConfig.DefaultTimeMode,
      accessTokenFile = None,
      tlsConfig = TlsConfiguration(enabled = false, None, None, None),
      jsonApi = false,
      maxInboundMessageSize = RunnerMainConfig.DefaultMaxInboundMessageSize,
      applicationId = None,
      uploadDar = false,
      enableContractUpgrading = false,
    )
    val adminClient = LedgerClient.singleHost(
      hostIp,
      port,
      configuration = LedgerClientConfiguration(
        applicationId = "admin-client",
        commandClient = CommandClientConfiguration.default,
        token = None,
      ),
      channelConfig = LedgerClientChannelConfiguration(None),
      loggerFactory = NamedLoggerFactory.root,
    )
    adminClient.foreach(
      _.packageManagementClient.uploadDarFile(
        ByteString.copyFrom(Files.readAllBytes(config.darPath.toPath))
      )
    )
    RunnerMain.main(config)
    withTemporaryDirectory { outputPath =>
      Main.main(
        Config.Empty.copy(
          ledgerHost = "localhost",
          ledgerPort = clientConfig.targetPort,
          partyConfig = PartyConfig(
            allParties = false,
            parties = clientConfig.parties,
          ),
          exportType = Some(
            Config.EmptyExportScript.copy(
              sdkVersion = SdkVersion.sdkVersion,
              outputPath = outputPath,
            )
          ),
        )
      )
      moveFile(outputPath.resolve("Export.daml").toFile, clientConfig.outputExportDaml.toFile)
      moveFile(outputPath.resolve("args.json").toFile, clientConfig.outputArgsJson.toFile)
      moveFile(outputPath.resolve("daml.yaml").toFile, clientConfig.outputDamlYaml.toFile)
    }
    val _ = Await.result(system.terminate(), Duration.Inf)
  }

  private def moveFile(src: File, dst: File): Unit = {
    if (!dst.getParentFile.exists()) {
      if (!dst.getParentFile.mkdirs())
        throw new RuntimeException(
          s"Failed to move $src to $dst. Could not create parent directory ${dst.getParent}."
        )
    }
    val _ = Files.move(src.toPath, dst.toPath, StandardCopyOption.REPLACE_EXISTING)
  }

  private def withTemporaryDirectory(f: Path => Unit): Unit = {
    val tmpDir = Files.createTempDirectory("daml-ledger-export")
    try {
      f(tmpDir)
    } finally {
      deleteRecursively(tmpDir)
    }
  }
}
