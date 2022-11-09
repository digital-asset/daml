// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.util.UUID
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.{ActorRef, ActorSystem, Scheduler}
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.Uri
import akka.util.Timeout
import ch.qos.logback.classic.Level
import com.daml.auth.middleware.api.{Client => AuthClient}
import com.daml.daml_lf_dev.DamlLf
import com.daml.dbutils.JdbcConfig
import com.daml.lf.archive.{Dar, DarReader}
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.engine.trigger.dao.DbTriggerDao
import com.daml.lf.speedy.Compiler
import com.daml.logging.ContextualizedLogger
import com.daml.ports.{Port, PortFiles}
import com.daml.runtime.JdbcDrivers
import com.daml.scalautil.Statement.discard
import scalaz.std.either._
import scalaz.std.list._
import scalaz.syntax.traverse._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.sys.ShutdownHookThread
import scala.util.{Failure, Success, Try}

object ServiceMain {

  // Timeout for serving binding
  implicit val timeout: Timeout = 30.seconds

  // Used by the test fixture
  def startServer(
      host: String,
      port: Int,
      maxAuthCallbacks: Int,
      authCallbackTimeout: FiniteDuration,
      maxHttpEntityUploadSize: Long,
      httpEntityUploadTimeout: FiniteDuration,
      authConfig: AuthConfig,
      authRedirectToLogin: AuthClient.RedirectToLogin,
      authCallback: Option[Uri],
      ledgerConfig: LedgerConfig,
      restartConfig: TriggerRestartConfig,
      encodedDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]],
      jdbcConfig: Option[JdbcConfig],
      allowExistingSchema: Boolean,
      compilerConfig: Compiler.Config,
      logTriggerStatus: (UUID, String) => Unit = (_, _) => (),
  ): Future[(ServerBinding, ActorSystem[Server.Message])] = {

    val system: ActorSystem[Server.Message] =
      ActorSystem(
        Server(
          host,
          port,
          maxAuthCallbacks,
          authCallbackTimeout,
          maxHttpEntityUploadSize,
          httpEntityUploadTimeout,
          authConfig,
          authRedirectToLogin,
          authCallback,
          ledgerConfig,
          restartConfig,
          encodedDars,
          jdbcConfig,
          allowExistingSchema,
          compilerConfig,
          logTriggerStatus,
        ),
        "TriggerService",
      )

    implicit val scheduler: Scheduler = system.scheduler
    implicit val ec: ExecutionContext = system.executionContext

    val serviceF: Future[ServerBinding] =
      system.ask((ref: ActorRef[ServerBinding]) => Server.GetServerBinding(ref))
    serviceF.map(server => (server, system))
  }

  def main(args: Array[String]): Unit = {
    Cli.parseConfig(
      args,
      DbTriggerDao.supportedJdbcDriverNames(JdbcDrivers.availableJdbcDriverNames),
    ) match {
      case None => sys.exit(1)
      case Some(config) =>
        config.rootLoggingLevel.foreach(setLoggingLevel)
        config.logEncoder match {
          case LogEncoder.Plain =>
          case LogEncoder.Json =>
            discard(System.setProperty("LOG_FORMAT_JSON", "true"))
        }

        val logger = ContextualizedLogger.get(this.getClass)
        val encodedDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]] =
          config.darPaths.traverse(p => DarReader.readArchiveFromFile(p.toFile)) match {
            case Left(err) => sys.error(s"Failed to read archive: $err")
            case Right(dars) => dars.map(_.map(p => p.pkgId -> p.proto))
          }
        val authConfig: AuthConfig =
          (config.authInternalUri, config.authExternalUri, config.authBothUri) match {
            case (None, None, None) => NoAuth
            case (None, None, Some(both)) => AuthMiddleware(both, both)
            case (Some(int), Some(ext), None) => AuthMiddleware(int, ext)
            case (int, ext, both) =>
              // Note that this should never happen, as it should be caucht by
              // the checkConfig part of our scopt configuration
              logger.withoutContext.error(
                s"Must specify either both --auth-internal and --auth-external or just --auth. Got: auth-internal: $int, auth-external: $ext, auth: $both."
              )
              sys.exit(1)
          }
        val ledgerConfig =
          LedgerConfig(
            config.ledgerHost,
            config.ledgerPort,
            config.timeProviderType,
            config.commandTtl,
            config.maxInboundMessageSize,
          )
        val restartConfig = TriggerRestartConfig(
          config.minRestartInterval,
          config.maxRestartInterval,
        )

        // Init db and exit immediately.
        if (config.init) {
          config.jdbcConfig match {
            case None =>
              logger.withoutContext.error("No JDBC configuration for database initialization.")
              sys.exit(1)
            case Some(c) =>
              Try(
                Await.result(
                  DbTriggerDao(c)(ExecutionContext.parasitic)
                    .initialize(config.allowExistingSchema)(ExecutionContext.parasitic),
                  Duration(30, SECONDS),
                )
              ) match {
                case Failure(exception) =>
                  logger.withoutContext.error(s"Failed to initialize database: $exception")
                  sys.exit(1)
                case Success(()) =>
                  logger.withoutContext.info("Successfully initialized database.")
                  sys.exit(0)
              }
          }
        }

        val system: ActorSystem[Server.Message] =
          ActorSystem(
            Server(
              config.address,
              config.httpPort,
              config.maxAuthCallbacks,
              config.authCallbackTimeout,
              config.maxHttpEntityUploadSize,
              config.httpEntityUploadTimeout,
              authConfig,
              config.authRedirectToLogin,
              config.authCallbackUri,
              ledgerConfig,
              restartConfig,
              encodedDars,
              config.jdbcConfig,
              config.allowExistingSchema,
              config.compilerConfig,
            ),
            "TriggerService",
          )

        implicit val scheduler: Scheduler = system.scheduler
        implicit val ec: ExecutionContext = system.executionContext

        // Shutdown gracefully on SIGINT.
        val serviceF: Future[ServerBinding] =
          system.ask((ref: ActorRef[ServerBinding]) => Server.GetServerBinding(ref))
        config.portFile.foreach(portFile =>
          serviceF.foreach(serverBinding =>
            PortFiles.write(portFile, Port(serverBinding.localAddress.getPort))
          )
        )
        val _: ShutdownHookThread = sys.addShutdownHook {
          system ! Server.Stop
          serviceF.onComplete {
            case Success(_) =>
              system.log.info("Server is offline, the system will now terminate")
            case Failure(ex) =>
              system.log.info("Failure encountered shutting down the server: " + ex.toString)
          }
          discard[serviceF.type](Await.ready(serviceF, 5.seconds))
        }
    }
  }

  private def setLoggingLevel(level: Level): Unit = {
    discard(System.setProperty("LOG_LEVEL_ROOT", level.levelStr))
  }
}
