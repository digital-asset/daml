// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.nio.file.Path

import akka.actor.ActorSystem
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.lf.archive.DarReader
import scalaz.syntax.traverse._

import scala.concurrent.{ExecutionContext, Future}
import akka.stream.Materializer

import scala.util.control.NonFatal

object MigrationStep {

  trait Test {
    def execute(packageId: String, config: Config.Test)(implicit
        ec: ExecutionContext,
        esf: ExecutionSequencerFactory,
        mat: Materializer,
    ): Future[Unit]
  }

  private def readPackageId(path: Path): String =
    DarReader.readArchiveFromFile(path.toFile).toOption.get.map(_.pkgId).main

  def main(args: Array[String]): Unit = {
    val config = Config.parser.parse(args, Config.default).getOrElse(sys.exit(1))
    val packageId = readPackageId(config.dar)

    implicit val system: ActorSystem = ActorSystem(packageId)
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool(packageId)(system)
    implicit val ec: ExecutionContext = system.dispatcher

    val result = config.test.execute(packageId, config)

    result.failed.foreach { case NonFatal(e) => e.printStackTrace(System.err) }
    result.onComplete(_ => system.terminate())
  }
}
