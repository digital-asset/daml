// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientChannelConfiguration,
  LedgerClientConfiguration,
}
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.logging.NamedLoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

object Main {
  def main(args: Array[String]): Unit = {
    Config.parse(args) match {
      case None => sys.exit(1)
      case Some(config) => main(config)
    }
  }

  def main(config: Config): Unit = {
    implicit val sys: ActorSystem = ActorSystem("script-export")
    implicit val ec: ExecutionContext = sys.dispatcher
    implicit val seq: ExecutionSequencerFactory = new PekkoExecutionSequencerPool("script-export")
    implicit val mat: Materializer = Materializer(sys)
    run(config)
      .recoverWith { case NonFatal(fail) =>
        Future {
          println(fail)
        }
      }
      .onComplete(_ => sys.terminate())
    discard(Await.result(sys.whenTerminated, Duration.Inf))
  }

  def run(config: Config)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] = {
    config.exportType match {
      case Some(exportScript: ExportScript) =>
        for {
          client <- LedgerClient.singleHost(
            config.ledgerHost,
            config.ledgerPort,
            clientConfig(config),
            clientChannelConfig(config),
            NamedLoggerFactory.root,
          )
          parties <- LedgerUtils.getAllParties(client, config.accessToken, config.partyConfig)
          acs <- LedgerUtils.getACS(client, parties, config.start)
          trees <- LedgerUtils.getTransactionTrees(client, parties, config.start, config.end)
          acsPkgRefs = TreeUtils.contractsReferences(acs.values)
          treePkgRefs = TreeUtils.treesReferences(trees)
          pkgRefs = acsPkgRefs ++ treePkgRefs
          pkgs <- Dependencies.fetchPackages(client, pkgRefs.toList)
          _ = Export.writeExport(
            exportScript.sdkVersion,
            exportScript.damlScriptLib,
            exportScript.outputPath,
            acs,
            trees,
            pkgRefs,
            pkgs,
            exportScript.acsBatchSize,
            exportScript.setTime,
          )
        } yield ()
      case None =>
        Future.successful(())
    }
  }

  private def clientConfig(config: Config): LedgerClientConfiguration = LedgerClientConfiguration(
    applicationId = "script-export",
    commandClient = CommandClientConfiguration.default,
    token = config.accessToken.flatMap(_.token),
  )

  private def clientChannelConfig(config: Config): LedgerClientChannelConfiguration =
    LedgerClientChannelConfiguration(
      sslContext = config.tlsConfig.client(),
      maxInboundMessageSize = config.maxInboundMessageSize,
    )
}
