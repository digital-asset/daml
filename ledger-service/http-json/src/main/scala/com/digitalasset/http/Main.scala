// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Path

import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.scalautil.Statement.discard
import com.daml.http.dbbackend.ContractDao
import com.typesafe.scalalogging.StrictLogging
import scalaz.{-\/, \/, \/-}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.syntax.show._

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
    Cli.parseConfig(args) match {
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
        s", packageReloadInterval=${config.packageReloadInterval: FiniteDuration}" +
        s", packageMaxInboundMessageSize=${config.packageMaxInboundMessageSize: Option[Int]}" +
        s", maxInboundMessageSize=${config.maxInboundMessageSize: Int}" +
        s", tlsConfig=${config.tlsConfig}" +
        s", jdbcConfig=${config.jdbcConfig.shows}" +
        s", staticContentConfig=${config.staticContentConfig.shows}" +
        s", allowNonHttps=${config.allowNonHttps.shows}" +
        s", accessTokenFile=${config.accessTokenFile: Option[Path]}" +
        s", wsConfig=${config.wsConfig.shows}" +
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
}
