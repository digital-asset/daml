// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference

import java.io.{File, FileWriter}
import java.time.Instant
import java.util.zip.ZipFile

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.daml.ledger.api.server.damlonx.Server
import com.daml.ledger.participant.state.index.v1.impl.reference.ReferenceIndexService
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.server.services.testing.TimeServiceBackend
import com.digitalasset.platform.services.time.TimeModel
import org.slf4j.LoggerFactory
import scala.util.Try

object ReferenceServer extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  final case class Config(
      port: Int,
      portFile: Option[File],
      archiveFiles: List[File],
  )

  val argParser = new scopt.OptionParser[Config]("reference-server") {
    head("DamlOnX Reference Server")
    opt[Int]("port")
      .optional()
      .action((p, c) => c.copy(port = p))
      .text("Server port. If not set, a random port is allocated.")
    opt[File]("port-file")
      .optional()
      .action((f, c) => c.copy(portFile = Some(f)))
      .text("File to write the allocated port number to.")
    arg[File]("<archive>...")
      .unbounded()
      .action((f, c) => c.copy(archiveFiles = f :: c.archiveFiles))
      .text("DAR files to load")
  }
  val config = argParser.parse(args, Config(0, None, List.empty)).getOrElse(sys.exit(1))

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
  val ledger =
    new com.daml.ledger.participant.state.v1.impl.reference.Ledger(timeModel, tsb)

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

  val indexService = ReferenceIndexService(ledger)

  // Block until the index service has been initialized, e.g. it has processed the
  // state initialization updates.
  indexService.waitUntilInitialized

  val server = Server(
    serverPort = config.port,
    indexService = indexService,
    writeService = ledger,
    tsb
  )

  config.portFile.foreach { f =>
    val w = new FileWriter(f)
    w.write(s"${server.port}\n")
    w.close
  }

  // Add a hook to close the server. Invoked when Ctrl-C is pressed.
  Runtime.getRuntime.addShutdownHook(new Thread(() => server.close()))
}
