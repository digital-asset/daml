// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.io.File
import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.Statement.discard
import com.daml.http.dbbackend.ContractDao
import com.daml.ledger.api.tls.TlsConfigurationCli
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.typesafe.scalalogging.StrictLogging
import scalaz.{-\/, \/, \/-}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.syntax.show._
import scalaz.syntax.tag._
import scopt.RenderingMode

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Main extends StrictLogging {

  object ErrorCodes {
    val Ok = 0
    val InvalidUsage = 100
    val StartupError = 101
  }

  def main(args: Array[String]): Unit =
    parseConfig(args) match {
      case Some(config) =>
        main(config)
      case None =>
        // error is printed out by scopt
        sys.exit(ErrorCodes.InvalidUsage)
    }

  private def main(config: Config): Unit = {
    logger.info(
      s"Config(ledgerHost=${config.ledgerHost: String}, ledgerPort=${config.ledgerPort: Int}" +
        s", address=${config.address: String}, httpPort=${config.httpPort: Int}" +
        s", portFile=${config.portFile: Option[Path]}" +
        s", applicationId=${config.applicationId.unwrap: String}" +
        s", packageReloadInterval=${config.packageReloadInterval.toString}" +
        s", maxInboundMessageSize=${config.maxInboundMessageSize: Int}" +
        s", tlsConfig=${config.tlsConfig}" +
        s", jdbcConfig=${config.jdbcConfig.shows}" +
        s", staticContentConfig=${config.staticContentConfig.shows}" +
        s", allowNonHttps=${config.allowNonHttps.shows}" +
        s", accessTokenFile=${config.accessTokenFile.toString}" +
        ")")

    implicit val asys: ActorSystem = ActorSystem("http-json-ledger-api")
    implicit val mat: Materializer = Materializer(asys)
    implicit val aesf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("clientPool")(asys)
    implicit val ec: ExecutionContext = asys.dispatcher

    def terminate(): Unit = discard { Await.result(asys.terminate(), 10.seconds) }

    val contractDao = config.jdbcConfig.map(c => ContractDao(c.driver, c.url, c.user, c.password))

    (contractDao, config.jdbcConfig) match {
      case (Some(dao), Some(c)) if c.createSchema =>
        logger.info("Creating DB schema...")
        Try(dao.transact(ContractDao.initialize(dao.logHandler)).unsafeRunSync()) match {
          case Success(()) =>
            logger.info("DB schema created. Terminating process...")
            terminate()
            System.exit(ErrorCodes.Ok)
          case Failure(e) =>
            logger.error("Failed creating DB schema", e)
            terminate()
            System.exit(ErrorCodes.StartupError)
        }
      case _ =>
    }

    val serviceF: Future[HttpService.Error \/ ServerBinding] =
      HttpService.start(
        startSettings = config,
        contractDao = contractDao,
      )

    discard {
      sys.addShutdownHook {
        HttpService
          .stop(serviceF)
          .onComplete { fa =>
            logFailure("Shutdown error", fa)
            terminate()
          }
      }
    }

    serviceF.onComplete {
      case Success(\/-(a)) =>
        logger.info(s"Started server: $a")
      case Success(-\/(e)) =>
        logger.error(s"Cannot start server: $e")
        terminate()
        System.exit(ErrorCodes.StartupError)
      case Failure(e) =>
        logger.error("Cannot start server", e)
        terminate()
        System.exit(ErrorCodes.StartupError)
    }
  }

  private def logFailure[A](msg: String, fa: Try[A]): Unit = fa match {
    case Failure(e) => logger.error(msg, e)
    case _ =>
  }

  private def parseConfig(args: Seq[String]): Option[Config] =
    configParser.parse(args, Config.Empty)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private val configParser: scopt.OptionParser[Config] =
    new scopt.OptionParser[Config]("http-json-binary") {

      override def renderingMode: RenderingMode = RenderingMode.OneColumn

      head("HTTP JSON API daemon")

      help("help").text("Print this usage text")

      opt[String]("ledger-host")
        .action((x, c) => c.copy(ledgerHost = x))
        .required()
        .text("Ledger host name or IP address")

      opt[Int]("ledger-port")
        .action((x, c) => c.copy(ledgerPort = x))
        .required()
        .text("Ledger port number")

      opt[String]("address")
        .action((x, c) => c.copy(address = x))
        .optional()
        .text(
          s"IP address that HTTP JSON API service listens on. Defaults to ${Config.Empty.address: String}.")

      opt[Int]("http-port")
        .action((x, c) => c.copy(httpPort = x))
        .required()
        .text(
          "HTTP JSON API service port number. " +
            "A port number of 0 will let the system pick an ephemeral port. " +
            "Consider specifying `--port-file` option with port number 0.")

      opt[File]("port-file")
        .action((x, c) => c.copy(portFile = Some(x.toPath)))
        .optional()
        .text(
          "Optional unique file name where to write the allocated HTTP port number. " +
            "If process terminates gracefully, this file will be deleted automatically. " +
            "Used to inform clients in CI about which port HTTP JSON API listens on. " +
            "Defaults to none, that is, no file gets created.")

      opt[String]("application-id")
        .action((x, c) => c.copy(applicationId = ApplicationId(x)))
        .optional()
        .text(
          s"Optional application ID to use for ledger registration. Defaults to ${Config.Empty.applicationId.unwrap: String}")

      TlsConfigurationCli.parse(this, colSpacer = "        ")((f, c) =>
        c copy (tlsConfig = f(c.tlsConfig)))

      opt[Duration]("package-reload-interval")
        .action((x, c) => c.copy(packageReloadInterval = FiniteDuration(x.length, x.unit)))
        .optional()
        .text(
          s"Optional interval to poll for package updates. Examples: 500ms, 5s, 10min, 1h, 1d. " +
            s"Defaults to ${Config.Empty.packageReloadInterval.toString}")

      opt[Int]("max-inbound-message-size")
        .action((x, c) => c.copy(maxInboundMessageSize = x))
        .optional()
        .text(
          s"Optional max inbound message size in bytes. Defaults to ${Config.Empty.maxInboundMessageSize: Int}")

      opt[Map[String, String]]("query-store-jdbc-config")
        .action((x, c) => c.copy(jdbcConfig = Some(JdbcConfig.createUnsafe(x))))
        .validate(JdbcConfig.validate)
        .optional()
        .valueName(JdbcConfig.usage)
        .text(s"Optional query store JDBC configuration string." +
          " Query store is a search index, use it if you need to query large active contract sets. " +
          JdbcConfig.help)

      opt[Map[String, String]]("static-content")
        .action((x, c) => c.copy(staticContentConfig = Some(StaticContentConfig.createUnsafe(x))))
        .validate(StaticContentConfig.validate)
        .optional()
        .valueName(StaticContentConfig.usage)
        .text(s"DEV MODE ONLY (not recommended for production). Optional static content configuration string. "
          + StaticContentConfig.help)

      opt[Unit]("allow-insecure-tokens")
        .action((_, c) => c copy (allowNonHttps = true))
        .text(
          "DEV MODE ONLY (not recommended for production). Allow connections without a reverse proxy providing HTTPS.")

      opt[String]("access-token-file")
        .text(
          s"provide the path from which the access token will be read, required to interact with an authenticated ledger, no default")
        .action((path, arguments) => arguments.copy(accessTokenFile = Some(Paths.get(path))))
        .optional()

      opt[Map[String, String]]("websocket-config")
        .action((x, c) => c.copy(wsConfig = Some(WebsocketConfig.createUnsafe(x))))
        .validate(WebsocketConfig.validate)
        .optional()
        .valueName(WebsocketConfig.usage)
        .text(s"Optional websocket configuration string. ${WebsocketConfig.help}")
    }
}
