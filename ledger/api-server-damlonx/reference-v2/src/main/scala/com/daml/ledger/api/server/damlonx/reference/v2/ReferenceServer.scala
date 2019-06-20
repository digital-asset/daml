package com.daml.ledger.api.server.damlonx.reference.v2

import java.util.concurrent.atomic.AtomicBoolean
import java.util.zip.ZipFile

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.daml.ledger.participant.state.kvutils.InMemoryKVParticipantState
import com.daml.ledger.participant.state.kvutils.v2.ParticipantStateConversion
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.index.cli.Cli
import com.digitalasset.platform.index.{StandaloneIndexServer, StandaloneIndexerServer}
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.util.control.NonFatal

object ReferenceServer extends App {

  val logger = LoggerFactory.getLogger("indexed-kvutils")

  val config = Cli.parse(args).getOrElse(sys.exit(1))

  implicit val system: ActorSystem = ActorSystem("indexed-kvutils")
  implicit val materializer: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy { e =>
        logger.error(s"Supervision caught exception: $e")
        Supervision.Stop
      })

  val ledger = new InMemoryKVParticipantState()

  val readService = ParticipantStateConversion.V1ToV2Rread(ledger)
  val writeService = ParticipantStateConversion.V1ToV2Write(ledger)

  config.archiveFiles.foreach { file =>
    val archivesTry = for {
      zipFile <- Try(new ZipFile(file))
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }.readArchive(zipFile)
    } yield ledger.uploadPackages(dar.all, None)
  }

  val indexerServer = StandaloneIndexerServer(readService, config.jdbcUrl)
  val indexServer = StandaloneIndexServer(config, readService, writeService).start()

  val closed = new AtomicBoolean(false)

  def closeServer(): Unit = {
    if (closed.compareAndSet(false, true)) {
      indexServer.close()
      indexerServer.close()
      ledger.close()
      materializer.shutdown()
      val _ = system.terminate()
    }
  }

  try {
    Runtime.getRuntime.addShutdownHook(new Thread(() => closeServer()))
  } catch {
    case NonFatal(t) => {
      logger.error("Shutting down Sandbox application because of initialization error", t)
      closeServer()
    }
  }
}
