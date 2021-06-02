// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http

import java.nio.file.Path
import akka.actor.ActorSystem
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.Materializer
import cats.{Applicative, Monad}
import cats.effect.concurrent.Deferred
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, IO, Resource, Timer}
import com.daml.cliopts.Logging.LogEncoder
import com.daml.grpc.adapter.AkkaExecutionSequencerPool
import com.daml.runtime.JdbcDrivers
import com.daml.http.dbbackend.ContractDao
import scalaz.{-\/, \/-}
import com.daml.metrics.Metrics
import com.daml.platform.sandbox.metrics.MetricsReporting
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.syntax.show._
import com.daml.cliopts.{GlobalLogLevel, Logging}
import com.daml.http.util.Logging.{InstanceUUID, instanceUUIDLogCtx}
import com.daml.logging.{ContextualizedLogger, LoggingContextOf}

import java.time.{Duration => JDuration}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
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

  def executeWithShutdownHookNet[F[_], A](
      program: Deferred[F, Unit] => F[A],
      shutdownHookTimeout: FiniteDuration = 10.seconds,
  )(implicit
      F: Applicative[F],
      globalConcurrentEffect: ConcurrentEffect[F],
      globalTimer: Timer[F],
      loggingCtx: LoggingContextOf[InstanceUUID],
  ): F[A] = {
    import cats.FlatMap.ops.toAllFlatMapOps
    for {
      _ <- F.pure(()): F[Unit]
      shutdownHookCalled <- Deferred[F, Unit](globalConcurrentEffect)
      didProgramAlreadyEnd <- Deferred[F, Unit](globalConcurrentEffect)
      _ = sys.addShutdownHook {
        globalConcurrentEffect.toIO(shutdownHookCalled.complete(())).unsafeRunSync()

        logger.debug(
          s"Shutdown hook will wait $shutdownHookTimeout for the application to exit gracefully otherwise it will forcefully shutdown"
        )
        globalConcurrentEffect
          .toIO(Concurrent.timeoutTo(didProgramAlreadyEnd.get, shutdownHookTimeout, F.unit))
          .unsafeRunSync()
      }
      result <- program(shutdownHookCalled)
      _ <- didProgramAlreadyEnd.complete(())
    } yield result
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

    def program(shutdownHookCalled: Deferred[IO, Unit]) = {
      import cats.FlatMap.ops.toAllFlatMapOps
      val waitTime = 5.seconds
      config.metricsReporter.foreach(reporter =>
        logger.info(s"Using the $reporter metrics reporter")
      )
      val metricsReporting = new MetricsReporting(
        getClass.getName,
        config.metricsReporter,
        JDuration.ofNanos(config.metricsReportingInterval.toNanos),
      )
      def createActor[F[_]](name: String)(implicit F: Monad[F]): Resource[F, ActorSystem] =
        Resource.make(F.pure(ActorSystem(name)))(asys =>
          for {
            _ <- F.pure(logger.debug(s"Terminating actor system (waiting up to $waitTime)"))
            terminateFuture <- F.pure(asys.terminate())
            _ <- F.pure(Await.ready(terminateFuture, waitTime))
            _ = logger.debug("Actor system terminated")
          } yield ()
        )
      def createMaterializer[F[_]](asys: ActorSystem)(implicit
          F: Monad[F]
      ): Resource[F, Materializer] =
        Resource.make(F.pure(Materializer(asys)))(mat =>
          F.pure(mat).flatMap(mat => F.pure(mat.shutdown()))
        )
      val resources =
        for {
          asys <- createActor[IO]("http-json-ledger-api")
          _ = logger.debug("Started actor system")
          mat <- createMaterializer[IO](asys)
          metrics <- metricsReporting.acquire[IO]()
          _ = logger.debug("Acquired metrics")
        } yield (asys, mat, metrics)
      resources
        .use { case (asys, mat, metrics) =>
          implicit val asysInstance: ActorSystem = asys
          implicit val matInstance: Materializer = mat
          implicit val sequencePool: AkkaExecutionSequencerPool = new AkkaExecutionSequencerPool(
            "clientPool"
          )(asys)
          implicit val ec: ExecutionContextExecutor = asys.dispatcher
          implicit val metricsI: Metrics = metrics
          implicit val contextShift: ContextShift[IO] = IO.contextShift(ec)
          implicit val timer: Timer[IO] = IO.timer(ec)
          val contractDao =
            config.jdbcConfig.map(c => ContractDao(c.driver, c.url, c.user, c.password))

          (contractDao, config.jdbcConfig) match {
            case (Some(dao), Some(c)) if c.createSchema =>
              logger.info("Creating DB schema...")
              import dao.{jdbcDriver, logHandler}
              Try(dao.transact(ContractDao.initialize).unsafeRunSync()) match {
                case Success(()) =>
                  logger.info("DB schema created. Terminating process...")
                  IO(ErrorCodes.Ok)
                case Failure(e) =>
                  logger.error("Failed creating DB schema", e)
                  IO(ErrorCodes.StartupError)
              }
            case _ =>
              def shutdownServer(serverBinding: ServerBinding) = {
                IO(logger.info(s"Shutting down server (waiting up to $waitTime")) *>
                  IO.fromFuture(IO(serverBinding.terminate(waitTime)))
                    .attempt
                    .map {
                      case Left(fa) => logger.error("Shutdown error", fa)
                      case _ => logger.trace("Server stopped")
                    }
              }
              logger.debug(
                "No DB schema had to be created, proceeding with starting the server"
              )
              for {
                serverStarted <- Deferred[IO, ServerBinding]
                _ = logger.debug("Initialized serverStarted deferred")

                // Here we _try_ to shutdown the server, because we could still be in the startup of the server
                // thus having no server binding available.
                // If there is no server binding available the application will be forcefully shutdown,
                // because Scala futures sadly cannot be cancelled.
                handleShutdownHookCalled =
                  for {
                    _ <- shutdownHookCalled.get
                    _ <- IO(
                      logger.info(
                        s"Shutdown hook called, trying to retrieve server binding (waiting up to $waitTime)"
                      )
                    )
                    res <- serverStarted.get
                      .map(Some(_))
                      .timeoutTo(waitTime, IO(None))
                      .flatMap {
                        case Some(serverBinding) =>
                          shutdownServer(serverBinding) *> IO(ErrorCodes.Ok)
                        // No server binding means no graceful shutdown of the server
                        case None =>
                          IO(
                            logger.info(
                              "No server binding available, the shutdown hook will run into a timeout and do a force shutdown"
                            )
                          ) *> IO(ErrorCodes.StartupError)
                      }
                  } yield res
                serverRes =
                  IO.fromFuture(
                    IO(
                      HttpService.start(
                        startSettings = config,
                        contractDao = contractDao,
                      )
                    )
                  ).attempt
                    .flatMap {
                      case Right(\/-(serverBinding)) =>
                        logger.info(s"Started server: $serverBinding")
                        serverStarted.complete(serverBinding) *> IO.never *> IO(ErrorCodes.Ok)
                      case Right(-\/(e)) =>
                        logger.error(s"Cannot start server: $e")
                        IO(ErrorCodes.StartupError)
                      case Left(e) =>
                        logger.error("Cannot start server", e)
                        IO(ErrorCodes.StartupError)
                    }
                res <- IO
                  .race(handleShutdownHookCalled, serverRes)
                  .map(_.merge)
              } yield res
          }
        }
    }

    val globalEc = scala.concurrent.ExecutionContext.global
    val globalCs = IO.contextShift(globalEc)
    val globalConcurrent = IO.ioConcurrentEffect(globalCs)
    val globalTimer = IO.timer(globalEc)
    System.exit(
      executeWithShutdownHookNet(
        program(_: Deferred[IO, Unit]).attempt
          .map(
            _.fold(
              exception => {
                logger.error("Startup failed", exception)
                ErrorCodes.StartupError
              },
              identity,
            )
          )
      )(
        implicitly[Applicative[IO]],
        globalConcurrent,
        globalTimer,
        lc,
      ).unsafeRunSync()
    )
  }
}
