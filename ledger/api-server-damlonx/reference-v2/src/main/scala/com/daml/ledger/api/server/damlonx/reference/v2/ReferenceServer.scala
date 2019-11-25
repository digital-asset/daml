// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.api.server.damlonx.reference.v2.cli.Cli
import com.daml.ledger.participant.state.kvutils.InMemoryKVParticipantState
import com.daml.ledger.participant.state.v1.ParticipantId
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.AuthServiceWildcard
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.index.{StandaloneIndexServer, StandaloneIndexerServer}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
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
  implicit val ec: ExecutionContext = system.dispatcher

  val ledger = new InMemoryKVParticipantState(participantId)

  val readService = ledger
  val writeService = ledger
  val authService = AuthServiceWildcard

  config.archiveFiles.foreach { file =>
    for {
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
        .readArchiveFromFile(file)
    } yield ledger.uploadPackages(dar.all, None, UUID.randomUUID().toString)
  }

  val participantLoggerFactory = NamedLoggerFactory.forParticipant(participantId)
  val participantF: Future[(AutoCloseable, StandaloneIndexServer#SandboxState)] = for {
    indexerServer <- StandaloneIndexerServer(
      readService,
      config,
      participantLoggerFactory,
      SharedMetricRegistries.getOrCreate(s"indexer-$participantId"),
    )
    indexServer <- StandaloneIndexServer(
      config,
      readService,
      writeService,
      authService,
      participantLoggerFactory,
      SharedMetricRegistries.getOrCreate(s"ledger-api-server-$participantId"),
    ).start()
  } yield (indexerServer, indexServer)

  val extraParticipants =
    for {
      (extraParticipantId, port, jdbcUrl) <- config.extraParticipants
    } yield {
      val participantConfig = config.copy(
        port = port,
        participantId = extraParticipantId,
        jdbcUrl = jdbcUrl
      )
      val participantLoggerFactory =
        NamedLoggerFactory.forParticipant(participantConfig.participantId)
      for {
        extraIndexer <- StandaloneIndexerServer(
          readService,
          participantConfig,
          participantLoggerFactory,
          SharedMetricRegistries.getOrCreate(s"indexer-$extraParticipantId"))
        extraLedgerApiServer <- StandaloneIndexServer(
          participantConfig,
          readService,
          writeService,
          authService,
          participantLoggerFactory,
          SharedMetricRegistries.getOrCreate(s"ledger-api-server-$extraParticipantId")
        ).start()
      } yield (extraIndexer, extraLedgerApiServer)
    }

  val closed = new AtomicBoolean(false)

  def closeServer(): Unit = {
    if (closed.compareAndSet(false, true)) {
      participantF.foreach {
        case (indexer, indexServer) =>
          indexer.close()
          indexServer.close()
      }

      for (extraParticipantF <- extraParticipants) {
        extraParticipantF.foreach {
          case (indexer, indexServer) =>
            indexer.close()
            indexServer.close()
        }
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
