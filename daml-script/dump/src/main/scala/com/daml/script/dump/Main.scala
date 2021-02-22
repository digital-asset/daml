// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}

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
    implicit val sys: ActorSystem = ActorSystem("script-dump")
    implicit val ec: ExecutionContext = sys.dispatcher
    implicit val seq: ExecutionSequencerFactory = new AkkaExecutionSequencerPool("script-dump")
    implicit val mat: Materializer = Materializer(sys)
    run(config)
      .recoverWith { case NonFatal(fail) =>
        Future {
          println(fail)
        }
      }
      .onComplete(_ => sys.terminate())
    Await.result(sys.whenTerminated, Duration.Inf)
    ()
  }

  def run(config: Config)(implicit
      ec: ExecutionContext,
      esf: ExecutionSequencerFactory,
      mat: Materializer,
  ): Future[Unit] =
    for {
      client <- LedgerClient.singleHost(config.ledgerHost, config.ledgerPort, clientConfig)
      acs <- LedgerUtils.getACS(client, config.parties, config.start)
      trees <- LedgerUtils.getTransactionTrees(client, config.parties, config.start, config.end)
      acsPkgRefs = TreeUtils.contractsReferences(acs.values)
      treePkgRefs = TreeUtils.treesReferences(trees)
      pkgRefs = acsPkgRefs ++ treePkgRefs
      pkgs <- Dependencies.fetchPackages(client, pkgRefs.toList)
      _ = Dump.writeDump(
        config.sdkVersion,
        config.damlScriptLib,
        config.outputPath,
        acs,
        trees,
        pkgRefs,
        pkgs,
      )
    } yield ()

  val clientConfig: LedgerClientConfiguration = LedgerClientConfiguration(
    applicationId = "script-dump",
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
  )
}
