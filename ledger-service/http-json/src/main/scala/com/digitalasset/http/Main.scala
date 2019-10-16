// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.ActorMaterializer
import com.digitalasset.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.digitalasset.ledger.api.refinements.ApiTypes.ApplicationId
import com.typesafe.scalalogging.StrictLogging
import scalaz.\/
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Main extends StrictLogging {

  object ErrorCodes {
    val InvalidUsage = 100
    val StartupError = 101
  }

  private final case class Config(
      ledgerHost: String,
      ledgerPort: Int,
      address: String = "0.0.0.0",
      httpPort: Int,
      applicationId: ApplicationId = ApplicationId("HTTP-JSON-API-Gateway"),
      packageReloadInterval: FiniteDuration = HttpService.DefaultPackageReloadInterval,
      maxInboundMessageSize: Int = HttpService.DefaultMaxInboundMessageSize,
      queryStoreJdbcUrl: Option[String] = None
  )

  private val EmptyConfig = Config(ledgerHost = "", ledgerPort = -1, httpPort = -1)

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
        s", applicationId=${config.applicationId.unwrap: String}" +
        s", packageReloadInterval=${config.packageReloadInterval.toString}" +
        s", maxInboundMessageSize=${config.maxInboundMessageSize: Int})" +
        s", queryStoreJdbcUrl=${config.queryStoreJdbcUrl.toString}")

    implicit val asys: ActorSystem = ActorSystem("http-json-ledger-api")
    implicit val mat: ActorMaterializer = ActorMaterializer()
    implicit val aesf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("clientPool")(asys)
    implicit val ec: ExecutionContext = asys.dispatcher

    val serviceF: Future[HttpService.Error \/ ServerBinding] =
      HttpService.start(
        config.ledgerHost,
        config.ledgerPort,
        config.applicationId,
        config.address,
        config.httpPort,
        config.packageReloadInterval,
        config.maxInboundMessageSize)

    sys.addShutdownHook {
      HttpService
        .stop(serviceF)
        .onComplete { fa =>
          logFailure("Shutdown error", fa)
          asys.terminate()
          ()
        }
    }

    serviceF.onComplete {
      case Success(_) =>
      case Failure(_) =>
        // no reason to log this failure, HttpService.start supposed to report it
        Await.result(asys.terminate(), 10.seconds)
        System.exit(ErrorCodes.StartupError)
    }
  }

  private def logFailure[A](msg: String, fa: Try[A]): Unit = fa match {
    case Failure(e) => logger.error(msg, e)
    case _ =>
  }

  private def parseConfig(args: Seq[String]): Option[Config] =
    configParser.parse(args, EmptyConfig)

  private val configParser = new scopt.OptionParser[Config]("http-json-binary") {
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
        s"IP address that HTTP JSON API service listens on. Defaults to ${EmptyConfig.address: String}.")

    opt[Int]("http-port")
      .action((x, c) => c.copy(httpPort = x))
      .required()
      .text("HTTP JSON API service port number")

    opt[String]("application-id")
      .action((x, c) => c.copy(applicationId = ApplicationId(x)))
      .optional()
      .text(
        s"Optional application ID to use for ledger registration. Defaults to ${EmptyConfig.applicationId.unwrap: String}")

    opt[Duration]("package-reload-interval")
      .action((x, c) => c.copy(packageReloadInterval = FiniteDuration(x.length, x.unit)))
      .optional()
      .text(s"Optional interval to poll for package updates. Examples: 500ms, 5s, 10min, 1h, 1d. " +
        s"Defaults to ${EmptyConfig.packageReloadInterval.toString}")

    opt[Int]("max-inbound-message-size")
      .action((x, c) => c.copy(maxInboundMessageSize = x))
      .optional()
      .text(
        s"Optional max inbound message size in bytes. Defaults to ${EmptyConfig.maxInboundMessageSize: Int}")

    opt[String]("query-store-jdbc-url")
      .action((x, c) => c.copy(queryStoreJdbcUrl = Some(x)))
      .optional()
      .text(s"Optional query store JDBC URL")
  }
}
