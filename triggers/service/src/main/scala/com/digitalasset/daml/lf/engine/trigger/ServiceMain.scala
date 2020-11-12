// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import java.util.UUID

import akka.actor.typed.{ActorRef, ActorSystem, Scheduler}
import akka.actor.typed.scaladsl.AskPattern._
import akka.http.scaladsl.Http.ServerBinding
import akka.util.Timeout
import com.daml.daml_lf_dev.DamlLf
import com.daml.lf.archive.{Dar, DarReader}
import com.daml.lf.data.Ref.PackageId
import com.daml.ports.{Port, PortFiles}
import com.daml.scalautil.Statement.discard

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.sys.ShutdownHookThread
import scala.util.{Failure, Success}
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.std.either._

object ServiceMain {

  // Timeout for serving binding
  implicit val timeout: Timeout = 30.seconds

  // Used by the test fixture
  def startServer(
      host: String,
      port: Int,
      authConfig: AuthConfig,
      ledgerConfig: LedgerConfig,
      restartConfig: TriggerRestartConfig,
      encodedDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]],
      jdbcConfig: Option[JdbcConfig],
      logTriggerStatus: (UUID, String) => Unit = (_, _) => ()
  ): Future[(ServerBinding, ActorSystem[Message])] = {

    val system: ActorSystem[Message] =
      ActorSystem(
        Server(
          host,
          port,
          authConfig,
          ledgerConfig,
          restartConfig,
          encodedDars,
          jdbcConfig,
          initDb = false, // for tests we initialize the database in beforeEach clause
          logTriggerStatus
        ),
        "TriggerService"
      )

    implicit val scheduler: Scheduler = system.scheduler
    implicit val ec: ExecutionContext = system.executionContext

    val serviceF: Future[ServerBinding] =
      system.ask((ref: ActorRef[ServerBinding]) => GetServerBinding(ref))
    serviceF.map(server => (server, system))
  }

  def main(args: Array[String]): Unit = {
    ServiceConfig.parse(args) match {
      case None => sys.exit(1)
      case Some(config) =>
        val encodedDars: List[Dar[(PackageId, DamlLf.ArchivePayload)]] =
          config.darPaths.traverse(p => DarReader().readArchiveFromFile(p.toFile).toEither) match {
            case Left(err) => sys.error(s"Failed to read archive: $err")
            case Right(dar) => dar
          }
        val authConfig: AuthConfig = config.authUri match {
          case None => NoAuth
          case Some(uri) => AuthMiddleware(uri)
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
        val system: ActorSystem[Message] =
          ActorSystem(
            Server(
              config.address,
              config.httpPort,
              authConfig,
              ledgerConfig,
              restartConfig,
              encodedDars,
              config.jdbcConfig,
              config.init,
            ),
            "TriggerService"
          )

        implicit val scheduler: Scheduler = system.scheduler
        implicit val ec: ExecutionContext = system.executionContext

        // Shutdown gracefully on SIGINT.
        val serviceF: Future[ServerBinding] =
          system.ask((ref: ActorRef[ServerBinding]) => GetServerBinding(ref))
        config.portFile.foreach(portFile =>
          serviceF.foreach(serverBinding =>
            PortFiles.write(portFile, Port(serverBinding.localAddress.getPort))))
        val _: ShutdownHookThread = sys.addShutdownHook {
          system ! Stop
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
}
