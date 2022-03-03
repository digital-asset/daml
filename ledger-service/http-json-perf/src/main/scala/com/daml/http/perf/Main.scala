// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import java.nio.file.{Files, Path}
import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.gatling.stats.{SimulationLog, SimulationLogSyntax}
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.HttpServiceTestFixture.{withHttpService, withLedger}
import com.daml.http.perf.scenario.SimulationConfig
import com.daml.http.util.FutureUtil._
import com.daml.scalautil.Statement.discard
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.scenario.Simulation
import io.gatling.netty.util.Transports
import io.netty.channel.EventLoopGroup
import scalaz.std.scalaFuture._
import scalaz.std.string._
import scalaz.{-\/, \/, \/-}

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future, Promise, TimeoutException}
import scala.util.{Failure, Success, Try}

object Main extends StrictLogging {

  sealed abstract class ExitCode(val unwrap: Int) extends Product with Serializable
  object ExitCode {
    case object Ok extends ExitCode(0)
    case object InvalidUsage extends ExitCode(100)
    case object StartupError extends ExitCode(101)
    case object InvalidScenario extends ExitCode(102)
    case object TimedOutScenario extends ExitCode(103)
    case object GatlingError extends ExitCode(104)
  }

  private def resolveSimulationClass(str: String): Throwable \/ Class[_ <: Simulation] = {
    try {
      val klass: Class[_] = Class.forName(str)
      val simClass = klass.asSubclass(classOf[Simulation])
      \/-(simClass)
    } catch {
      case e: Throwable =>
        logger.error(s"Cannot resolve scenario: '$str'", e)
        -\/(e)
    }
  }

  private def runGatlingScenario(
      config: Config[String],
      jsonApiHost: String,
      jsonApiPort: Int,
  )(implicit
      sys: ActorSystem,
      ec: ExecutionContext,
      elg: EventLoopGroup,
  ): Future[(ExitCode, Path)] = {

    import io.gatling.app
    import io.gatling.core.config.GatlingPropertiesBuilder

    val hostAndPort = s"${jsonApiHost: String}:${jsonApiPort: Int}"
    discard { System.setProperty(SimulationConfig.HostAndPortKey, hostAndPort) }

    val configBuilder = new GatlingPropertiesBuilder()
      .simulationClass(config.scenario)
      .resultsDirectory(config.reportsDir.getAbsolutePath)

    Future
      .fromTry {
        app.CustomRunner.runWith(sys, elg, configBuilder.build, None)
      }
      .map { case (a, f) =>
        if (a == app.cli.StatusCode.Success.code) (ExitCode.Ok, f) else (ExitCode.GatlingError, f)
      }
  }

  private def generateReport(dir: Path): String \/ Unit = {
    import SimulationLogSyntax._

    require(Files.isDirectory(dir), s"input path $dir should be a directory")

    val jsDir = dir.resolve("js")
    val statsPath = jsDir.resolve("stats.json")
    val assertionsPath = jsDir.resolve("assertions.json")
    val simulationLog = SimulationLog.fromFiles(statsPath, assertionsPath)
    simulationLog.foreach { x =>
      x.writeSummaryCsv(dir)
      val summary = x.writeSummaryText(dir)
      logger.info(s"Report\n$summary")
    }
    simulationLog.map(_ => ())
  }

  private def waitForResult[A](fa: Future[Throwable \/ ExitCode], timeout: Duration): ExitCode =
    try {
      Await
        .result(fa, timeout)
        .valueOr(_ => ExitCode.GatlingError)
    } catch {
      case e: TimeoutException =>
        logger.error(s"Scenario failed", e)
        ExitCode.TimedOutScenario
    }

  private def logCompletion(
      fa: Future[Throwable \/ ExitCode]
  )(implicit ec: ExecutionContext): Future[Throwable \/ ExitCode] = {
    fa.transform { res =>
      res match {
        case Success(\/-(_)) => logger.info(s"Scenario completed")
        case Success(-\/(e)) => logger.error(s"Scenario failed", e)
        case Failure(e) => logger.error(s"Scenario failed", e)
      }
      res
    }
  }

  def main(args: Array[String]): Unit = {
    val name = "http-json-perf"
    val terminationTimeout: FiniteDuration = 30.seconds

    implicit val asys: ActorSystem = ActorSystem(name)
    implicit val mat: Materializer = Materializer(asys)
    implicit val aesf: ExecutionSequencerFactory =
      new AkkaExecutionSequencerPool(poolName = name, terminationTimeout = terminationTimeout)
    implicit val elg: EventLoopGroup = Transports.newEventLoopGroup(true, 0, "gatling")
    implicit val ec: ExecutionContext = asys.dispatcher

    def terminate(): Unit = {
      discard { Await.result(asys.terminate(), terminationTimeout) }
      val promise = Promise[Unit]()
      val future = elg.shutdownGracefully(0, terminationTimeout.length, terminationTimeout.unit)
      discard {
        future.addListener((f: io.netty.util.concurrent.Future[_]) =>
          discard { promise.complete(Try(f.get).map(_ => ())) }
        )
      }
      discard { Await.result(promise.future, terminationTimeout) }
    }

    val exitCode: ExitCode = Config.parseConfig(args) match {
      case None =>
        // error is printed out by scopt
        ExitCode.InvalidUsage
      case Some(config) =>
        logger.info(s"$config")
        val res = eitherT(resolveSimulationClass(config.scenario).traverse { _ =>
          withLedger(config.dars, name) { (ledgerPort, _, _) =>
            QueryStoreBracket.withJsonApiJdbcConfig(config.queryStoreIndex) { jsonApiJdbcConfig =>
              withHttpService(
                name,
                ledgerPort,
                jsonApiJdbcConfig,
                None,
              ) { (uri, _, _, _) =>
                runGatlingScenario(config, uri.authority.host.address, uri.authority.port)
                  .flatMap { case (exitCode, dir) =>
                    toFuture(generateReport(dir))
                      .map { _ =>
                        logger.info(s"Report directory: ${dir.toAbsolutePath}")
                        exitCode
                      }
                  }
              }
            }
          }
        })
        waitForResult(logCompletion(res.run), config.maxDuration.getOrElse(Duration.Inf))
    }
    terminate()
    sys.exit(exitCode.unwrap)
  }
}
