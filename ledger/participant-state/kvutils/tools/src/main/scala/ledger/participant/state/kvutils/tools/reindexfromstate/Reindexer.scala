// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.reindexfromstate

import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.participant.state.kvutils.`export`.ProtobufBasedLedgerDataImporter
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.IntegrityChecker.{
  CheckFailedException,
  CommitStrategySupportFactory,
}
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.{Config, color}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.{Failure, Success}

object Reindexer {
  def runAndExit[LogResult](
      args: Array[String],
      commitStrategySupportFactory: CommitStrategySupportFactory[LogResult],
  ): Unit = {
    val config = Config.parse(args).getOrElse {
      sys.exit(1)
    }
    println(s"Verifying integrity of ${config.exportFilePath}...")

    val actorSystem: ActorSystem = ActorSystem("integrity-checker")
    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())
    implicit val materializer: Materializer = Materializer(actorSystem)

    val importer = ProtobufBasedLedgerDataImporter(config.exportFilePath)
    new UpdatesIndexer(commitStrategySupportFactory(_, executionContext))
      .run(importer, config)
      .onComplete {
        case Success(_) =>
          sys.exit(0)
        case Failure(exception: CheckFailedException) =>
          println(exception.getMessage.red)
          sys.exit(1)
        case Failure(exception) =>
          exception.printStackTrace()
          sys.exit(1)
      }(DirectExecutionContext)
  }
}
