// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import java.nio.file.{Files, Path}

import akka.actor.ActorSystem
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.lf.archive.DarReader
import scalaz.syntax.traverse._
import spray.json._
import JsonProtocol._

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

object MigrationStep {

  private def readPackageId(path: Path): String =
    DarReader().readArchiveFromFile(path.toFile).get.map(_._1.toString).main

  private val MigrationStep = "migration-step"
  def main(args: Array[String]): Unit = {
    val config =
      Config.parser.parse(args, Config.default).getOrElse(sys.exit(1))
    val packageId = readPackageId(config.dar)

    implicit val system: ActorSystem = ActorSystem(MigrationStep)
    implicit val sequencer: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool(MigrationStep)(system)
    implicit val ec: ExecutionContext = system.dispatcher

    val proposeAccept =
      new ProposeAccept(config.host, config.port, "propose-accept", packageId)

    proposeAccept
      .run(config.proposer, config.accepter, config.note)
      .onComplete {
        case Success(result) =>
          Files.write(config.outputFile, Seq(result.toJson.prettyPrint).asJava)
          system.terminate()
        case Failure(e) =>
          System.err.println(e)
          system.terminate()
      }
  }
}
