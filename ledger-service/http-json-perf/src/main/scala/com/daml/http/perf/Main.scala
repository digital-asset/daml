// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import akka.actor.ActorSystem
import com.daml.scalautil.Statement.discard
import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object Main extends StrictLogging {

  type Scenario = Config => Future[Unit]

  sealed abstract class ExitCode(val code: Int) extends Product with Serializable
  object ExitCode {
    case object Ok extends ExitCode(0)
    case object InvalidUsage extends ExitCode(100)
    case object StartupError extends ExitCode(101)
    case object InvalidScenario extends ExitCode(102)
  }

  def main(args: Array[String]): Unit = {
    implicit val asys: ActorSystem = ActorSystem("http-json-perf")
    //    implicit val mat: Materializer = Materializer(asys)
    implicit val ec: ExecutionContext = asys.dispatcher

    def terminate(): Unit = discard { Await.result(asys.terminate(), 10.seconds) }

    val exitCode: ExitCode = Config.parseConfig(args) match {
      case Some(config) =>
        main(config)
      case None =>
        // error is printed out by scopt
        ExitCode.InvalidUsage
    }

    terminate()
    sys.exit(exitCode.code)
  }

  private def main(config: Config)(implicit ec: ExecutionContext): ExitCode = {
    logger.info(s"$config")
    if (config.scenario == "commands") {
      runScenarioSync(config, null) // XXX
      ExitCode.Ok
    } else {
      logger.error(s"Unsupported scenario: ${config.scenario}")
      ExitCode.InvalidScenario
    }
  }

  private def runScenarioSync(config: Config, scenario: Scenario)(
      implicit ec: ExecutionContext
  ): Unit = {
    val scenarioF: Future[Unit] = scenario(config)
    scenarioF.onComplete {
      case Success(_) => logger.info(s"Scenario: ${config.scenario} completed")
      case Failure(e) => logger.error(s"Scenario: ${config.scenario} failed", e)
    }
    Await.result(scenarioF, Duration.Inf)
  }
}
