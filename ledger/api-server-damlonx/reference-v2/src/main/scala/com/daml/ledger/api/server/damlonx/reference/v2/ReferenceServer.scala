// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.daml.ledger.participant.state.kvutils.InMemoryKVParticipantState
import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.platform.index.cli.Cli
import com.digitalasset.platform.index.{StandaloneIndexServer, StandaloneIndexerServer}
import org.slf4j.LoggerFactory

import scala.util.Try
import scala.util.control.NonFatal

object ReferenceServer extends App {

  val logger = LoggerFactory.getLogger("indexed-kvutils")

  val config =
    Cli
      .parse(
        args,
        "damlonx-reference-server",
        "A fully compliant DAML Ledger API server backed by an in-memory store.",
        allowExtraParticipants = true)
      .getOrElse(sys.exit(1))

  // Name of this participant
  val participantId: ParticipantId = config.participantId

  implicit val system: ActorSystem = ActorSystem("indexed-kvutils")
  implicit val materializer: ActorMaterializer = ActorMaterializer(
    ActorMaterializerSettings(system)
      .withSupervisionStrategy { e =>
        logger.error(s"Supervision caught exception: $e")
        Supervision.Stop
      })

  val ledger = new InMemoryKVParticipantState(participantId)

  val readService = ledger
  val writeService = ledger

  config.archiveFiles.foreach { file =>
    for {
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
        .readArchiveFromFile(file)
    } yield ledger.uploadPackages(dar.all, None)
  }

  val indexerServer = StandaloneIndexerServer(readService, config.jdbcUrl)
  val indexServer = StandaloneIndexServer(config, readService, writeService).start()

  val extraParticipants =
    for {
      (participantId, port, jdbcUrl) <- config.extraPartipants
    } yield {
      val extraIndexer = StandaloneIndexerServer(readService, jdbcUrl)
      val extraLedgerApiServer = StandaloneIndexServer(
        config.copy(
          port = port,
          participantId = participantId,
          jdbcUrl = jdbcUrl
        ),
        readService,
        writeService
      )
      (extraIndexer, extraLedgerApiServer.start())
    }

  val closed = new AtomicBoolean(false)

  def closeServer(): Unit = {
    if (closed.compareAndSet(false, true)) {
      indexServer.close()
      indexerServer.close()
      for ((extraIndexer, extraLedgerApi) <- extraParticipants) {
        extraIndexer.close()
        extraLedgerApi.close()
      }
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
