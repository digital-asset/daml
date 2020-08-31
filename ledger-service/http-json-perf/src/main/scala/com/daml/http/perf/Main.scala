// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.HttpServiceTestFixture.withHttpService
import com.daml.scalautil.Statement.discard
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}
import scala.util.{Failure, Success}

object Main extends StrictLogging {

  type Scenario = Config => Future[Unit]

  sealed abstract class ExitCode(val code: Int) extends Product with Serializable
  object ExitCode {
    case object Ok extends ExitCode(0)
    case object InvalidUsage extends ExitCode(100)
    case object StartupError extends ExitCode(101)
    case object InvalidScenario extends ExitCode(102)
    case object TimedOutScenario extends ExitCode(103)
  }

  def main(args: Array[String]): Unit = {
    val name = "http-json-per"
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
    if (config.scenario == "commands") {
      withHttpService(config.scenario, config.dars, None, None) { (_, _, _, _) =>
        runScenarioSync(config, _ => Future.unit) // TODO(Leo): scenario runner goes here
      }.map(_ => ExitCode.Ok)
    } else {
      logger.error(s"Unsupported scenario: ${config.scenario}")
      Future.successful(ExitCode.InvalidScenario)
    }
  }

  private def runScenarioSync(config: Config, scenario: Scenario)(
      implicit ec: ExecutionContext): Future[Unit] = {
    scenario(config).map(_ => Thread.sleep(5000)) // XXX
  }
}
