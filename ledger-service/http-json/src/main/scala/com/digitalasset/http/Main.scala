// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Path
import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.Materializer
import com.daml.cliopts.Logging.LogEncoder
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.runtime.JdbcDrivers
import com.daml.scalautil.Statement.discard
import com.daml.http.dbbackend.ContractDao
import scalaz.{-\/, \/, \/-}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.syntax.show._
import com.daml.cliopts.{GlobalLogLevel, Logging}
import com.daml.http.util.Logging.{InstanceUUID, instanceUUIDLogCtx}
import com.daml.ledger.resources.ResourceContext
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}

import com.daml.metrics.MetricsReporting

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object Main {

  private[this] val logger = ContextualizedLogger.get(getClass)

  object ErrorCodes {
    val Ok = 0
    val InvalidUsage = 100
    val StartupError = 101
  }

  def adjustAndReloadLoggingOptions(
      config: Config
  ): Unit = {
    // If the system property was explicitly set before application startup and the config option was provided,
    // the prior value will be overridden here.
    config.logEncoder match {
      case LogEncoder.Plain => () // This is the default
      case LogEncoder.Json =>
        Logging.setUseJsonLogEncoderSystemProp()
        Logging.reconfigure(getClass, "logback.xml")
    }
    // Here we set all things which are related to logging but not to
    // any env vars in the logback.xml file.
    config.logLevel.foreach(GlobalLogLevel.set("Ledger HTTP-JSON API"))
  }

  def main(args: Array[String]): Unit = {
    instanceUUIDLogCtx(implicit lc =>
      Cli.parseConfig(
        args,
        ContractDao.supportedJdbcDriverNames(JdbcDrivers.availableJdbcDriverNames),
      ) match {
        case Some(config) =>
          adjustAndReloadLoggingOptions(config)
          main(config)
        case None =>
          // error is printed out by scopt
          sys.exit(ErrorCodes.InvalidUsage)
      }
    )
  }

  private def main(config: Config)(implicit lc: LoggingContextOf[InstanceUUID]): Unit = {
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
        s", nonRepudiationCertificateFile=${config.nonRepudiation.certificateFile: Option[Path]}" +
        s", nonRepudiationPrivateKeyFile=${config.nonRepudiation.privateKeyFile: Option[Path]}" +
        s", nonRepudiationPrivateKeyAlgorithm=${config.nonRepudiation.privateKeyAlgorithm: Option[String]}" +
        ")"
    )

    implicit val asys: ActorSystem = ActorSystem("http-json-ledger-api")
    implicit val mat: Materializer = Materializer(asys)
    implicit val aesf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool("clientPool")(asys)
    implicit val ec: ExecutionContext = asys.dispatcher
    implicit val rc: ResourceContext = ResourceContext(ec)
    val metricsReporting = new MetricsReporting(
      getClass.getName,
      config.metricsReporter,
      config.metricsReportingInterval,
    )
    val metricsResource = metricsReporting.acquire()

    def terminate(): Unit = discard {
      Await.result(metricsResource.release(), 10.seconds)
      Await.result(asys.terminate(), 10.seconds)
    }

    val contractDao = config.jdbcConfig.map(c => ContractDao(c.driver, c.url, c.user, c.password))

    (contractDao, config.jdbcConfig) match {
      case (Some(dao), Some(c)) if c.createSchema =>
        logger.info("Creating DB schema...")
        import dao.{logHandler, jdbcDriver}
        Try(dao.transact(ContractDao.initialize).unsafeRunSync()) match {
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

    val serviceF: Future[HttpService.Error \/ ServerBinding] = {
      metricsResource.asFuture.flatMap(implicit metrics =>
        HttpService.start(
          startSettings = config,
          contractDao = contractDao,
        )
      )
    }

    discard {
      sys.addShutdownHook {
        metricsResource
          .release()
          .onComplete(fa => logFailure("Error releasing metricsResource", fa))
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

  private def logFailure[A](msg: String, fa: Try[A])(implicit
      lc: LoggingContextOf[InstanceUUID]
  ): Unit = fa match {
    case Failure(e) => logger.error(msg, e)
    case _ =>
  }
}
