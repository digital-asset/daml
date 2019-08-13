// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference

import java.io.{File, FileWriter}

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.daml.ledger.api.server.damlonx.Server
import com.daml.ledger.participant.state.index.v1.impl.reference.ReferenceIndexService
import com.daml.ledger.participant.state.kvutils.InMemoryKVParticipantState
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.common.util.DirectExecutionContext
import org.slf4j.LoggerFactory

import scala.util.Try

/** The reference server is a fully compliant DAML Ledger API server
  * backed by the in-memory reference index and participant state implementations.
  * Not meant for production, or even development use cases, but for serving as a blueprint
  * for other implementations.
  */
object ReferenceServer extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  val config = Cli.parse(args).getOrElse(sys.exit(1))

  // Name of this participant
  // TODO: Pass this info in command-line (See issue #2025)
  val participantId: ParticipantId = Ref.LedgerString.assertFromString("in-memory-participant")

  // Initialize Akka and log exceptions in flows.
  implicit val system: ActorSystem = ActorSystem("ReferenceServer")
  implicit val materializer: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy { e =>
        logger.error(s"Supervision caught exception: $e")
        Supervision.Stop
      })

  val ledger = new InMemoryKVParticipantState(participantId)

  //val ledger = new Ledger(timeModel, tsb)
  def archivesFromDar(file: File): List[Archive] = {
    DarReader[Archive] { case (_, x) => Try(Archive.parseFrom(x)) }
      .readArchiveFromFile(file)
      .fold(t => throw new RuntimeException(s"Failed to parse DAR from $file", t), dar => dar.all)
  }

  // Parse DAR archives given as command-line arguments and upload them
  // to the ledger using a side-channel.
  config.archiveFiles.foreach { f =>
    val archives = archivesFromDar(f)
    archives.foreach { archive =>
      logger.info(s"Uploading package ${archive.getHash}...")
    }
    ledger.uploadPackages(archives, Some("uploaded on startup by participant"))
  }

  ledger.getLedgerInitialConditions
    .runWith(Sink.head)
    .foreach { initialConditions =>
      val indexService = ReferenceIndexService(
        participantReadService = if (config.badServer) BadReadService(ledger) else ledger,
        initialConditions = initialConditions,
        participantId = participantId
      )

      val server = Server(
        serverPort = config.port,
        sslContext = config.tlsConfig.flatMap(_.server),
        indexService = indexService,
        writeService = ledger,
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
final case class BadReadService(readService: ReadService) extends ReadService {
  override def getLedgerInitialConditions(): Source[LedgerInitialConditions, NotUsed] =
    readService.getLedgerInitialConditions

  override def stateUpdates(beginAfter: Option[Offset]): Source[(Offset, Update), NotUsed] =
    readService.stateUpdates(beginAfter).map {
      case (updateId, update) =>
        val updatePrime = update match {
          case tx: Update.TransactionAccepted =>
            tx.copy(transaction = GenTransaction(Map(), ImmArray.empty, Set.empty))
          case _ => update
        }
        (updateId, updatePrime)
    }
}
