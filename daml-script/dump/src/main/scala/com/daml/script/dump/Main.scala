// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.dump

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.client.LedgerClient
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.google.protobuf.ByteString
import scalaz.std.list._
import scalaz.std.set._
import scalaz.syntax.foldable._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

object Main {
  import TreeUtils._

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
      trees <- client.transactionClient
        .getTransactionTrees(
          LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)),
          Some(LedgerOffset(LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_END))),
          filter(config.parties),
          verbose = true,
        )
        .runWith(Sink.seq)
      pkgRefs: Set[PackageId] = trees.toList
        .foldMap(treeRefs(_))
        .map(i => PackageId.assertFromString(i.packageId))
      pkgs <- Dependencies.fetchPackages(client, pkgRefs.toList)
      _ = writeDump(
        config.sdkVersion,
        config.damlScriptLib,
        config.outputPath,
        trees,
        pkgRefs,
        pkgs,
      )
    } yield ()

  def writeDump(
      sdkVersion: String,
      damlScriptLib: String,
      targetDir: Path,
      trees: Seq[TransactionTree],
      pkgRefs: Set[PackageId],
      pkgs: Map[PackageId, (ByteString, Ast.Package)],
  ) = {
    val dir = Files.createDirectories(targetDir)
    Files.write(
      dir.resolve("Dump.daml"),
      Encode.encodeTransactionTreeStream(trees).render(80).getBytes(StandardCharsets.UTF_8),
    )
    val dars = pkgRefs.collect(Function.unlift(Dependencies.toDar(_, pkgs)))
    val deps = Files.createDirectory(dir.resolve("deps"))
    val depFiles = dars.zipWithIndex.map { case (dar, i) =>
      val file = deps.resolve(dar.main._3.metadata.fold(i.toString)(_.name) + ".dar")
      Dependencies.writeDar(sdkVersion, file, dar)
      file
    }
    Files.write(
      dir.resolve("daml.yaml"),
      s"""sdk-version: $sdkVersion
         |name: dump
         |version: 1.0.0
         |source: .
         |dependencies: [daml-stdlib, daml-prim, $damlScriptLib]
         |data-dependencies: [${depFiles.mkString(",")}]
         |""".stripMargin.getBytes(StandardCharsets.UTF_8),
    )
  }

  def filter(parties: List[String]): TransactionFilter =
    TransactionFilter(parties.map(p => p -> Filters()).toMap)

  val clientConfig: LedgerClientConfiguration = LedgerClientConfiguration(
    applicationId = "script-dump",
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
  )
}
