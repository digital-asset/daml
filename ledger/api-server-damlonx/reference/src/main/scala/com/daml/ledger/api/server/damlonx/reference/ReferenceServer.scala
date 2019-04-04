package com.daml.ledger.api.server.damlonx.reference

import java.io.File
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

object ReferenceServer extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system = ActorSystem("ReferenceServer")
  //implicit val materializer = ActorMaterializer()(system)

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
    new DarReader[Archive](Archive.parseFrom)
      .readArchive(new ZipFile(file))
      .fold(t => throw new RuntimeException(s"Failed to parse DAR from $file", t), identity)
  }

  args.foreach { arg =>
    archivesFromDar(new File(arg)).foreach { archive =>
      logger.info(s"Uploading archive ${archive.getHash}...")
      ledger.uploadArchive(
        None,
        archive
      )
    }
  }

  val readService =
    new com.daml.ledger.participant.state.v1.impl.reference.ReferenceReadService(ledger)

  val writeService =
    new com.daml.ledger.participant.state.v1.impl.reference.ReferenceWriteService(ledger)

  val indexService = ReferenceIndexService(readService)

  indexService.waitUntilInitialized

  val server = Server(
    serverPort = 6865,
    indexService = indexService,
    writeService = writeService,
    tsb
  )
  Runtime.getRuntime.addShutdownHook(new Thread(() => server.close()))
}
