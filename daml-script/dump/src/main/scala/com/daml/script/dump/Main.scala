// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.lf.archive.Dar
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.google.protobuf.ByteString

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
      acsPkgRefs = Dependencies.contractsReferences(acs.values)
      treePkgRefs = Dependencies.treesReferences(trees)
      pkgRefs = acsPkgRefs ++ treePkgRefs
      pkgs <- Dependencies.fetchPackages(client, pkgRefs.toList)
      _ = writeDump(
        config.sdkVersion,
        config.damlScriptLib,
        config.outputPath,
        acs,
        trees,
        pkgRefs,
        pkgs,
      )
    } yield ()

  def writeDump(
      sdkVersion: String,
      damlScriptLib: String,
      targetDir: Path,
      acs: Map[String, CreatedEvent],
      trees: Seq[TransactionTree],
      pkgRefs: Set[PackageId],
      pkgs: Map[PackageId, (ByteString, Ast.Package)],
  ) = {
    // Needed for map on Dar
    import scalaz.syntax.traverse._
    val dir = Files.createDirectories(targetDir)
    Files.write(
      dir.resolve("Dump.daml"),
      Encode.encodeTransactionTreeStream(acs, trees).render(80).getBytes(StandardCharsets.UTF_8),
    )
    val dars: Seq[Dar[(PackageId, ByteString, Ast.Package)]] =
      pkgRefs.view.collect(Function.unlift(Dependencies.toDar(_, pkgs))).toSeq
    val deps = Files.createDirectory(dir.resolve("deps"))
    val depFiles = dars.zipWithIndex.map { case (dar, i) =>
      val file = deps.resolve(dar.main._3.metadata.fold(i.toString)(_.name) + ".dar")
      Dependencies.writeDar(sdkVersion, file, dar)
      file
    }
    val lfTarget = Dependencies.targetLfVersion(dars.view.map(_.map(_._3.languageVersion)).toSeq)
    val targetFlag = lfTarget.fold("")(Dependencies.targetFlag(_))

    Files.write(
      dir.resolve("daml.yaml"),
      s"""sdk-version: $sdkVersion
         |name: dump
         |version: 1.0.0
         |source: .
         |dependencies: [daml-stdlib, daml-prim, $damlScriptLib]
         |data-dependencies: [${depFiles.mkString(",")}]
         |build-options: [$targetFlag]
         |""".stripMargin.getBytes(StandardCharsets.UTF_8),
    )
  }

  val clientConfig: LedgerClientConfiguration = LedgerClientConfiguration(
    applicationId = "script-dump",
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
  )
}
