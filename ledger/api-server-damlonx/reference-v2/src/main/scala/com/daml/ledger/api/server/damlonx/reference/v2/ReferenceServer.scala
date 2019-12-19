// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.server.damlonx.reference.v2

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.participant.state.v1.SubmissionId
import com.daml.ledger.api.server.damlonx.reference.v2.cli.Cli
import com.daml.ledger.participant.state.kvutils.InMemoryKVParticipantState
import com.digitalasset.daml.lf.archive.DarReader
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.auth.AuthServiceWildcard
import com.digitalasset.platform.apiserver.{ApiServerConfig, StandaloneApiServer}
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.indexer.{IndexerConfig, StandaloneIndexerServer}
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
        binaryName = "damlonx-reference-server",
        description = "A fully compliant DAML Ledger API server backed by an in-memory store.",
      )
      .getOrElse(sys.exit(1))

  implicit val system: ActorSystem = ActorSystem("indexed-kvutils")
  implicit val materializer: Materializer = Materializer(system)
  implicit val ec: ExecutionContext = system.dispatcher

  val ledger = new InMemoryKVParticipantState(config.participantId)

  val readService = ledger
  val writeService = ledger
  val authService = AuthServiceWildcard

  config.archiveFiles.foreach { file =>
    val submissionId = SubmissionId.assertFromString(UUID.randomUUID().toString)
    for {
      dar <- DarReader { case (_, x) => Try(Archive.parseFrom(x)) }
        .readArchiveFromFile(file)
    } yield ledger.uploadPackages(submissionId, dar.all, None)
  }

  val participantF: Future[(AutoCloseable, AutoCloseable)] = for {
    indexer <- newIndexer(config)
    apiServer <- newApiServer(config).start()
  } yield (indexer, apiServer)

  val extraParticipants =
    for {
      (extraParticipantId, port, jdbcUrl) <- config.extraParticipants
    } yield {
      val participantConfig = config.copy(
        port = port,
        participantId = extraParticipantId,
        jdbcUrl = jdbcUrl,
      )
      for {
        extraIndexer <- newIndexer(participantConfig)
        extraLedgerApiServer <- newApiServer(participantConfig).start()
      } yield (extraIndexer, extraLedgerApiServer)
    }

  def newIndexer(config: Config) =
    StandaloneIndexerServer(
      readService,
      IndexerConfig(config.participantId, config.jdbcUrl, config.startupMode),
      NamedLoggerFactory.forParticipant(config.participantId),
      SharedMetricRegistries.getOrCreate(s"indexer-${config.participantId}"),
    )

  def newApiServer(config: Config) =
    new StandaloneApiServer(
      ApiServerConfig(
        config.participantId,
        config.archiveFiles,
        config.port,
        config.jdbcUrl,
        config.tlsConfig,
        config.timeProvider,
        config.maxInboundMessageSize,
        config.portFile,
      ),
      readService,
      writeService,
      authService,
      NamedLoggerFactory.forParticipant(config.participantId),
      SharedMetricRegistries.getOrCreate(s"ledger-api-server-${config.participantId}"),
    )

  val closed = new AtomicBoolean(false)

  def closeServer(): Unit = {
    if (closed.compareAndSet(false, true)) {
      participantF.foreach {
        case (indexer, apiServer) =>
          indexer.close()
          apiServer.close()
      }

      for (extraParticipantF <- extraParticipants) {
        extraParticipantF.foreach {
          case (indexer, apiServer) =>
            indexer.close()
            apiServer.close()
        }
      }
      ledger.close()
      materializer.shutdown()
      val _ = system.terminate()
    }
  }

  private def startupFailed(e: Throwable): Unit = {
    logger.error("Shutting down because of an initialization error.", e)
    closeServer()
  }

  participantF.failed.foreach(startupFailed)

  try {
    Runtime.getRuntime.addShutdownHook(new Thread(() => closeServer()))
  } catch {
    case NonFatal(e) =>
      startupFailed(e)
  }
}
