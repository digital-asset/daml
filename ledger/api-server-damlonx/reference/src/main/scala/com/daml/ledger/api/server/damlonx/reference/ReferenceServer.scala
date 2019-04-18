// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference

import java.io.{File, FileWriter}
import java.time.Instant
import java.util.zip.ZipFile

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.daml.ledger.api.server.damlonx.Server
import com.daml.ledger.participant.state.index.v1.impl.reference.ReferenceIndexService
import com.daml.ledger.participant.state.v1.impl.reference.Ledger
import com.daml.ledger.participant.state.v1.{ReadService, Offset, Update}
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.server.services.testing.TimeServiceBackend
import com.digitalasset.platform.services.time.TimeModel
import org.slf4j.LoggerFactory

import scala.util.Try

/** The reference server is a fully compliant DAML Ledger API server
  * backed by the in-memory reference index and participant state implementations.
  * Not meant for production, or even development use cases, but for serving as a blueprint
  * for other implementations.
  */
object ReferenceServer extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  final case class Config(
      port: Int,
      portFile: Option[File],
      archiveFiles: List[File],
      badServer: Boolean
  )

  val argParser = new scopt.OptionParser[Config]("reference-server") {
    head(
      "A fully compliant DAML Ledger API server backed by an in-memory store.\n" +
        "Due to its lack of persistence it is not meant for production, but to serve as a blueprint for other DAML Ledger API server implementations.")
    opt[Int]("port")
      .optional()
      .action((p, c) => c.copy(port = p))
      .text("Server port. If not set, a random port is allocated.")
    opt[File]("port-file")
      .optional()
      .action((f, c) => c.copy(portFile = Some(f)))
      .text("File to write the allocated port number to. Used to inform clients in CI about the allocated port.")
    opt[Unit]("bad-server")
      .optional()
      .action((_, c) => c.copy(badServer = true))
      .text("Simulate a badly behaving server that returns empty transactions. Defaults to false.")
    arg[File]("<archive>...")
      .unbounded()
      .action((f, c) => c.copy(archiveFiles = f :: c.archiveFiles))
      .text("DAR files to load. Scenarios are ignored. The servers starts with an empty ledger by default.")
  }
  val config = argParser.parse(args, Config(0, None, List.empty, false)).getOrElse(sys.exit(1))

  // Initialize Akka and log exceptions in flows.
  implicit val system = ActorSystem("ReferenceServer")
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy { e =>
        logger.error(s"Supervision caught exception: $e")
        Supervision.Stop
      })

  val timeModel = TimeModel.reasonableDefault
  val tsb = TimeServiceBackend.simple(Instant.EPOCH)
  val ledger = new Ledger(timeModel, tsb)

  def archivesFromDar(file: File): List[Archive] = {
    DarReader[Archive](x => Try(Archive.parseFrom(x)))
      .readArchive(new ZipFile(file))
      .fold(t => throw new RuntimeException(s"Failed to parse DAR from $file", t), dar => dar.all)
  }

  // Parse DAR archives given as command-line arguments and upload them
  // to the ledger using a side-channel.
  config.archiveFiles.foreach { f =>
    archivesFromDar(f).foreach { archive =>
      logger.info(s"Uploading archive ${archive.getHash}...")
      ledger.uploadArchive(archive)
    }
  }

  ledger.getLedgerInitialConditions.foreach { initialConditions =>
    val indexService = ReferenceIndexService(if (config.badServer) BadReadService(ledger) else ledger)

    val server = Server(
      serverPort = config.port,
      indexService = indexService,
      writeService = ledger,
      tsb
    )

    // If port file was provided, write out the allocated server port to it.
    config.portFile.foreach { f =>
      val w = new FileWriter(f)
      w.write(s"${server.port}\n")
      w.close
    }

    // Add a hook to close the server. Invoked when Ctrl-C is pressed.
    Runtime.getRuntime.addShutdownHook(new Thread(() => server.close()))
  }(DirectExecutionContext)
}

// simulate a bad read service by returning only
// empty transactions.
final case class BadReadService(ledger: Ledger) extends ReadService {
  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    ledger.stateUpdates(beginAfter).map {
      case (updateId, update) =>
        val updatePrime = update match {
          case tx: Update.TransactionAccepted =>
            tx.copy(transaction = GenTransaction(Map(), ImmArray.empty))
          case _ => update
        }
        (updateId, updatePrime)
    }
}
