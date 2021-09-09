// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf

import java.nio.file.{Files, Path}

import akka.actor.ActorSystem
import akka.stream.Materializer
import com.daml.dbutils
import com.daml.gatling.stats.{SimulationLog, SimulationLogSyntax}
import com.daml.grpc.adapter.{AkkaExecutionSequencerPool, ExecutionSequencerFactory}
import com.daml.http.HttpServiceTestFixture.{withLedger, withHttpService}
import com.daml.http.domain.{JwtPayload, LedgerId}
import com.daml.http.perf.scenario.SimulationConfig
import com.daml.http.util.FutureUtil._
import com.daml.http.dbbackend.{DbStartupMode, JdbcConfig}
import com.daml.http.{EndpointsCompanion, HttpService}
import com.daml.jwt.domain.Jwt
import com.daml.scalautil.Statement.discard
import com.daml.testing.postgresql.PostgresDatabase
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.scenario.Simulation
import io.gatling.netty.util.Transports
import io.netty.channel.EventLoopGroup
import scalaz.std.scalaFuture._
import scalaz.std.string._
import scalaz.syntax.tag._
import scalaz.{-\/, EitherT, \/, \/-}

import Config.QueryStoreIndex

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, ExecutionContext, Future, Promise, TimeoutException}
import scala.util.{Failure, Success, Try}

object Main extends StrictLogging {

  private type ET[A] = EitherT[Future, Throwable, A]

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
        waitForResult(logCompletion(main1(config)), config.maxDuration.getOrElse(Duration.Inf))
    }

    terminate()
    sys.exit(exitCode.code)
  }

  private def logCompletion(fa: Future[Throwable \/ _])(implicit ec: ExecutionContext): fa.type = {
    fa.onComplete {
      case Success(\/-(_)) => logger.info(s"Scenario completed")
      case Success(-\/(e)) => logger.error(s"Scenario failed", e)
      case Failure(e) => logger.error(s"Scenario failed", e)
    }
    fa
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

  private def main1(config: Config[String])(implicit
      asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
      elg: EventLoopGroup,
  ): Future[Throwable \/ ExitCode] = {
    import scalaz.syntax.traverse._

    logger.info(s"$config")

    val et: ET[ExitCode] = for {
      ledgerId <- either(
        getLedgerId(config.jwt).leftMap(_ =>
          new IllegalArgumentException("Cannot infer Ledger ID from JWT")
        )
      ): ET[LedgerId]

      _ <- either(
        config.traverse(s => resolveSimulationClass(s))
      ): ET[Config[Class[_ <: Simulation]]]

      exitCode <- rightT(
        main2(ledgerId, config)
      ): ET[ExitCode]

    } yield exitCode

    et.run
  }

  private def main2(ledgerId: LedgerId, config: Config[String])(implicit
      asys: ActorSystem,
      mat: Materializer,
      aesf: ExecutionSequencerFactory,
      ec: ExecutionContext,
      elg: EventLoopGroup,
  ): Future[ExitCode] =
    withLedger(config.dars, ledgerId.unwrap) { (ledgerPort, _, _) =>
      withJsonApiJdbcConfig(config.queryStoreIndex) { jsonApiJdbcConfig =>
        withHttpService(
          ledgerId.unwrap,
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
            }: Future[ExitCode]
        }
      }
    }

  private def withJsonApiJdbcConfig[A](jsonApiQueryStoreEnabled: QueryStoreIndex)(
      fn: Option[JdbcConfig] => Future[A]
  )(implicit
      ec: ExecutionContext
  ): Future[A] = QueryStoreBracket lookup jsonApiQueryStoreEnabled match {
    case Some(b: QueryStoreBracket[s, d]) =>
      import b._
      for {
        dbInstance <- Future.successful(state())
        dbConfig <- toFuture(start(dbInstance))
        jsonApiDbConfig <- Future.successful(config(dbInstance, dbConfig))
        a <- fn(Some(jsonApiDbConfig))
        _ <- Future.successful(
          stop(dbInstance, dbConfig) // XXX ignores resulting Try
        ) // TODO: use something like `lf.data.TryOps.Bracket.bracket`
      } yield a

    case None => fn(None)
  }

  private[this] final case class QueryStoreBracket[S, D](
      state: () => S,
      start: S => Try[D],
      config: (S, D) => JdbcConfig,
      stop: (S, D) => Try[Unit],
  )
  private[this] object QueryStoreBracket {
    type T = QueryStoreBracket[_, _]
    val Postgres: T = QueryStoreBracket[PostgresRunner, PostgresDatabase](
      () => new PostgresRunner(),
      _.start(),
      (_, d) => jsonApiJdbcConfig(d),
      (r, _) => r.stop(),
    )

    import com.daml.testing.oracle, oracle.OracleAround
    val Oracle: T = QueryStoreBracket[OracleRunner, oracle.User](
      () => new OracleRunner,
      _.start(),
      _ jdbcConfig _,
      _.stop(_),
    )

    private[this] final class OracleRunner extends OracleAround {

      private val defaultUser = "ORACLE_USER"
      private val retainData = sys.env.get("RETAIN_DATA").exists(_ equalsIgnoreCase "true")
      private val useDefaultUser = sys.env.get("USE_DEFAULT_USER").exists(_ equalsIgnoreCase "true")
      type St = oracle.User

      def start() = Try {
        connectToOracle()
        if (useDefaultUser) createNewUser(defaultUser) else createNewRandomUser(): St
      }

      def jdbcConfig(user: St) = {
        import DbStartupMode._
        val startupMode: DbStartupMode = if (retainData) CreateIfNeededAndStart else CreateAndStart
        JdbcConfig(
          dbutils.JdbcConfig("oracle.jdbc.OracleDriver", oracleJdbcUrl, user.name, user.pwd),
          dbStartupMode = startupMode,
        )
      }

      def stop(user: St) = {
        if (retainData) Success((): Unit) else Try(dropUser(user.name))
      }
    }

    def lookup(q: QueryStoreIndex): Option[T] = q match {
      case QueryStoreIndex.No => None
      case QueryStoreIndex.Postgres => Some(Postgres)
      case QueryStoreIndex.Oracle => Some(Oracle)
    }
  }

  private def jsonApiJdbcConfig(c: PostgresDatabase): JdbcConfig =
    JdbcConfig(
      dbutils
        .JdbcConfig(driver = "org.postgresql.Driver", url = c.url, user = "test", password = ""),
      dbStartupMode = DbStartupMode.CreateOnly,
    )

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

  private def getLedgerId(jwt: Jwt): EndpointsCompanion.Unauthorized \/ LedgerId = {
    EndpointsCompanion
      .decodeAndParsePayload[JwtPayload](jwt, HttpService.decodeJwt)
      .map { case (_, payload) => payload.ledgerId }
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
    discard { System.setProperty(SimulationConfig.JwtKey, config.jwt.value) }

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
}
