// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.HttpServiceTestFixture.withHttpService
import com.daml.http.domain.LedgerId
import com.daml.http.perf.scenario.SimulationConfig
import com.daml.http.{EndpointsCompanion, HttpService}
import com.daml.jwt.domain.Jwt
import com.daml.scalautil.Statement.discard
import com.typesafe.scalalogging.StrictLogging
import scalaz.\/
import scalaz.syntax.tag._

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Success}

object Main extends StrictLogging {

  sealed abstract class ExitCode(val code: Int) extends Product with Serializable
  object ExitCode {
    case object Ok extends ExitCode(0)
    case object InvalidUsage extends ExitCode(100)
    case object StartupError extends ExitCode(101)
    case object InvalidScenario extends ExitCode(102)
    case object TimedOutScenario extends ExitCode(103)
    case object GatlingError extends ExitCode(104)
  }

  def main(args: Array[String]): Unit = {
    val name = "http-json-perf"
    val terminationTimeout: FiniteDuration = 30.seconds

    implicit val asys: ActorSystem = ActorSystem(name)
    implicit val mat: Materializer = Materializer(asys)
    implicit val aesf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool(poolName = name, terminationTimeout = terminationTimeout)
    implicit val ec: ExecutionContext = asys.dispatcher

    def terminate(): Unit = discard { Await.result(asys.terminate(), terminationTimeout) }

    val exitCode: ExitCode = Config.parseConfig(args) match {
      case Some(config) =>
        val exitCodeF: Future[ExitCode] = main(config)
        exitCodeF.onComplete {
          case Success(_) => logger.info(s"Scenario: ${config.scenario: String} completed")
          case Failure(e) => logger.error(s"Scenario: ${config.scenario: String} failed", e)
        }
        try {
          Await.result(exitCodeF, config.maxDuration.getOrElse(Duration.Inf))
        } catch {
          case e: TimeoutException =>
            logger.error(s"Scenario: ${config.scenario: String} failed", e)
            ExitCode.TimedOutScenario
        }
      case None =>
        // error is printed out by scopt
        ExitCode.InvalidUsage
    }

    terminate()
    sys.exit(exitCode.code)
  }

  private def main(config: Config)(
      implicit asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext
  ): Future[ExitCode] = {
    logger.info(s"$config")

    val ledgerId = getLedgerId(config.jwt)
      .getOrElse(throw new IllegalArgumentException("Cannot infer Ledger ID from JWT"))

    if (isValidScenario(config.scenario)) {
      withHttpService(ledgerId.unwrap, config.dars, None, None) { (uri, _, _, _) =>
        runGatlingScenario(config, uri.authority.host.address, uri.authority.port)
      }
    } else {
      logger.error(s"Invalid scenario: ${config.scenario}")
      Future.successful(ExitCode.InvalidScenario)
    }
  }

  private def isValidScenario(scenario: String): Boolean = {
    try {
      val klass: Class[_] = Class.forName(scenario)
      classOf[io.gatling.core.scenario.Simulation].isAssignableFrom(klass)
    } catch {
      case e: ClassCastException =>
        logger.error(s"Invalid Gatling scenario: '$scenario'", e)
        false
      case e: Throwable =>
        logger.error(s"Cannot find Gatling scenario: '$scenario'", e)
        false
    }
  }

  private def getLedgerId(jwt: Jwt): EndpointsCompanion.Unauthorized \/ LedgerId =
    EndpointsCompanion
      .decodeAndParsePayload(jwt, HttpService.decodeJwt)
      .map { case (_, payload) => payload.ledgerId }

  private def runGatlingScenario(config: Config, jsonApiHost: String, jsonApiPort: Int)(
      implicit ec: ExecutionContext): Future[ExitCode] = {
    import io.gatling.app
    import io.gatling.core.config.GatlingPropertiesBuilder

    val hostAndPort = s"${jsonApiHost: String}:${jsonApiPort: Int}"
    discard { System.setProperty(SimulationConfig.HostAndPortKey, hostAndPort) }
    discard { System.setProperty(SimulationConfig.JwtKey, config.jwt.value) }

    val configBuilder = new GatlingPropertiesBuilder()
      .simulationClass(config.scenario)
      .resultsDirectory("results-" + config.scenario)

    Future(app.Gatling.fromMap(configBuilder.build))
      .map(a => if (a == app.cli.StatusCode.Success.code) ExitCode.Ok else ExitCode.GatlingError)
  }
}
