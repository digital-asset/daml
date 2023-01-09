// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import java.util.concurrent._

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.daml.ledger.api.benchtool.config.WorkflowConfig.{
  FibonacciSubmissionConfig,
  FooSubmissionConfig,
}
import com.daml.ledger.api.benchtool.config.{Config, ConfigMaker, WorkflowConfig}
import com.daml.ledger.api.benchtool.metrics.MetricsManager.NoOpMetricsManager
import com.daml.ledger.api.benchtool.metrics.{BenchmarkResult, LatencyMetric, MetricsManager}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.submission._
import com.daml.ledger.api.benchtool.submission.foo.RandomPartySelecting
import com.daml.ledger.api.benchtool.util.TypedActorSystemResourceOwner
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.metrics.OpenTelemetryMetricsFactoryOwner
import com.daml.metrics.api.MetricHandle.Factory
import com.daml.platform.localstore.api.UserManagementStore
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

/** Runs a submission step followed by a benchmark step.
  * Either step is optional.
  *
  * Uses "benchtool" ([[Names.benchtoolApplicationId]]) applicationId for both steps.
  */
object LedgerApiBenchTool {
  private val printer = pprint.PPrinter.BlackWhite

  private[benchtool] val logger: Logger = LoggerFactory.getLogger(getClass)
  private[benchtool] def prettyPrint(x: Any): String = printer(x).toString()

  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    ConfigMaker.make(args) match {
      case Left(error) =>
        logger.error(s"Configuration error: ${error.details}")
        sys.exit(1)
      case Right(config) =>
        logger.info(s"Starting benchmark with configuration:\n${prettyPrint(config)}")
        val result = LedgerApiBenchTool(config)
          .run()(ExecutionContext.Implicits.global)
          .map {
            case Right(()) =>
              logger.info(s"Benchmark finished successfully.")
            case Left(error) =>
              logger.info(s"Benchmark failed: $error")
          }
          .recover { case ex =>
            logger.error(s"ledger-api-bench-tool failure: ${ex.getMessage}", ex)
            sys.exit(1)
          }(scala.concurrent.ExecutionContext.Implicits.global)
        Await.result(result, atMost = Duration.Inf)
        ()
    }
  }

  def apply(config: Config): LedgerApiBenchTool = {
    new LedgerApiBenchTool(
      names = new Names,
      authorizationHelper = config.authorizationTokenSecret.map(new AuthorizationHelper(_)),
      config = config,
    )
  }

}

class LedgerApiBenchTool(
    names: Names,
    authorizationHelper: Option[AuthorizationHelper],
    config: Config,
) {

  import LedgerApiBenchTool.{logger, prettyPrint}

  def run()(implicit ec: ExecutionContext): Future[Either[String, Unit]] = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    val resources: ResourceOwner[
      (
          String => LedgerApiServices,
          ActorSystem[SpawnProtocol.Command],
          Factory,
      )
    ] = for {
      servicesForUserId <- apiServicesOwner(config, authorizationHelper)
      system <- TypedActorSystemResourceOwner.owner()
      factory <- OpenTelemetryMetricsFactoryOwner(config.metricsReporter)
    } yield (servicesForUserId, system, factory)

    resources.use { case (servicesForUserId, actorSystem, metricsFactory) =>
      val adminServices = servicesForUserId(UserManagementStore.DefaultParticipantAdminUserId)
      val regularUserServices = servicesForUserId(names.benchtoolUserId)

      val partyAllocating = new PartyAllocating(
        names = names,
        adminServices = adminServices,
      )
      for {
        _ <- regularUserSetupStep(adminServices)
        (allocatedParties, benchtoolTestsPackageInfo) <- {
          config.workflow.submission match {
            case None =>
              logger.info("No submission config found; skipping the command submission step")
              for {
                allocatedParties <- SubmittedDataAnalyzing.determineAllocatedParties(
                  config.workflow,
                  partyAllocating,
                )
                benchtoolDamlPackageInfo <- SubmittedDataAnalyzing.determineBenchtoolTestsPackageId(
                  regularUserServices.packageService
                )
              } yield {
                (allocatedParties, benchtoolDamlPackageInfo)
              }
            case Some(submissionConfig) =>
              logger.info("Submission config found; command submission will be performed")
              submissionStep(
                regularUserServices = regularUserServices,
                adminServices = adminServices,
                submissionConfig = submissionConfig,
                partyAllocating = partyAllocating,
                metricsFactory = metricsFactory,
              )
                .map(_ -> BenchtoolTestsPackageInfo.StaticDefault)
                .map { v =>
                  // We manually execute a 'VACUUM ANALYZE' at the end of the submission step (if IndexDB is on Postgresql),
                  // to make sure query planner statistics, visibility map, etc.. are all up-to-date.
                  config.ledger.indexDbJdbcUrlO.foreach { indexDbJdbcUrl =>
                    if (indexDbJdbcUrl.startsWith("jdbc:postgresql:")) {
                      PostgresUtils.invokeVacuumAnalyze(indexDbJdbcUrl)
                    }
                  }
                  v
                }
          }
        }

        configEnricher = new ConfigEnricher(allocatedParties, benchtoolTestsPackageInfo)
        updatedStreamConfigs = config.workflow.streams.map(streamsConfig =>
          configEnricher.enrichStreamConfig(streamsConfig)
        )

        _ = logger.info(
          s"Stream configs adapted after the submission step: ${prettyPrint(updatedStreamConfigs)}"
        )
        benchmarkResult <-
          if (config.latencyTest) {
            benchmarkLatency(
              regularUserServices = regularUserServices,
              adminServices = adminServices,
              submissionConfigO = config.workflow.submission,
              metricsFactory = metricsFactory,
              allocatedParties = allocatedParties,
              actorSystem = actorSystem,
              maxLatencyObjectiveMillis = config.maxLatencyObjectiveMillis,
            )
          } else {
            benchmarkStreams(
              regularUserServices = regularUserServices,
              streamConfigs = updatedStreamConfigs,
              metricsFactory = metricsFactory,
              actorSystem = actorSystem,
            )
          }
      } yield benchmarkResult
    }
  }

  private def regularUserSetupStep(
      adminServices: LedgerApiServices
  )(implicit ec: ExecutionContext): Future[Unit] =
    (config.authorizationTokenSecret, config.workflow.submission) match {
      case (Some(_), Some(submissionConfig)) =>
        // We only need to setup the user when the UserManagementService is used and we're going to submit transactions
        // The submission config is necessary to establish a set of rights that will be granted to the user.
        logger.info(
          s"Setting up the regular '${names.benchtoolUserId}' user prior to the submission phase."
        )
        adminServices.userManagementService.createUserOrGrantRightsToExisting(
          userId = names.benchtoolUserId,
          observerPartyNames = names.observerPartyNames(
            submissionConfig.numberOfObservers,
            submissionConfig.uniqueParties,
          ),
          signatoryPartyName = names.signatoryPartyName,
        )
      case _ =>
        Future.successful(
          logger.info(
            s"The '${names.benchtoolUserId}' user is going to be used for authentication."
          )
        )
    }

  private def benchmarkStreams(
      regularUserServices: LedgerApiServices,
      streamConfigs: List[WorkflowConfig.StreamConfig],
      metricsFactory: Factory,
      actorSystem: ActorSystem[SpawnProtocol.Command],
  )(implicit ec: ExecutionContext): Future[Either[String, Unit]] =
    if (streamConfigs.isEmpty) {
      logger.info(s"No streams defined. Skipping the benchmark step.")
      Future.successful(Right(()))
    } else
      Benchmark
        .run(
          streamConfigs = streamConfigs,
          reportingPeriod = config.reportingPeriod,
          apiServices = regularUserServices,
          metricsFactory = metricsFactory,
          system = actorSystem,
        )

  private def benchmarkLatency(
      regularUserServices: LedgerApiServices,
      adminServices: LedgerApiServices,
      submissionConfigO: Option[WorkflowConfig.SubmissionConfig],
      metricsFactory: Factory,
      allocatedParties: AllocatedParties,
      actorSystem: ActorSystem[SpawnProtocol.Command],
      maxLatencyObjectiveMillis: Long,
  )(implicit ec: ExecutionContext): Future[Either[String, Unit]] =
    submissionConfigO match {
      case Some(submissionConfig: FooSubmissionConfig) =>
        val generator: CommandGenerator = new FooCommandGenerator(
          config = submissionConfig,
          divulgeesToDivulgerKeyMap = Map.empty,
          names = names,
          allocatedParties = allocatedParties,
          partySelecting = new RandomPartySelecting(
            config = submissionConfig,
            allocatedParties = allocatedParties,
          ),
        )
        for {
          metricsManager <- MetricsManager(
            observedMetric = "submit-and-wait-latency",
            logInterval = config.reportingPeriod,
            metrics = List(LatencyMetric.empty(maxLatencyObjectiveMillis)),
            exposedMetrics = None,
          )(actorSystem, ec)
          submitter = CommandSubmitter(
            names = names,
            benchtoolUserServices = regularUserServices,
            adminServices = adminServices,
            metricsFactory = metricsFactory,
            metricsManager = metricsManager,
            waitForSubmission = true,
            partyAllocating = new PartyAllocating(
              names = names,
              adminServices = adminServices,
            ),
          )
          result <- submitter
            .generateAndSubmit(
              generator = generator,
              config = submissionConfig,
              baseActAs = List(allocatedParties.signatory),
              maxInFlightCommands = config.maxInFlightCommands,
              submissionBatchSize = config.submissionBatchSize,
            )
            .flatMap(_ => metricsManager.result())
            .map {
              case BenchmarkResult.ObjectivesViolated =>
                Left("Metrics objectives not met.")
              case BenchmarkResult.Ok =>
                Right(())
            }
            .recoverWith { case NonFatal(e) =>
              Future.successful(Left(e.getMessage))
            }
        } yield result
      case Some(other) =>
        Future.failed(
          new RuntimeException(s"Unsupported submission config for latency benchmarking: $other")
        )
      case None =>
        Future.failed(
          new RuntimeException("Submission config cannot be empty for latency benchmarking")
        )
    }

  def submissionStep(
      regularUserServices: LedgerApiServices,
      adminServices: LedgerApiServices,
      submissionConfig: WorkflowConfig.SubmissionConfig,
      partyAllocating: PartyAllocating,
      metricsFactory: Factory,
  )(implicit
      ec: ExecutionContext
  ): Future[AllocatedParties] = {

    val submitter = CommandSubmitter(
      names = names,
      benchtoolUserServices = regularUserServices,
      adminServices = adminServices,
      metricsManager = NoOpMetricsManager(),
      waitForSubmission = submissionConfig.waitForSubmission,
      partyAllocating = partyAllocating,
      metricsFactory = metricsFactory,
    )
    for {
      allocatedParties <- submitter.prepare(
        submissionConfig
      )
      _ <-
        submissionConfig match {
          case submissionConfig: FooSubmissionConfig =>
            new FooSubmission(
              submitter = submitter,
              maxInFlightCommands = config.maxInFlightCommands,
              submissionBatchSize = config.submissionBatchSize,
              submissionConfig = submissionConfig,
              allocatedParties = allocatedParties,
              names = names,
            ).performSubmission()
          case submissionConfig: FibonacciSubmissionConfig =>
            val generator: CommandGenerator = new FibonacciCommandGenerator(
              signatory = allocatedParties.signatory,
              config = submissionConfig,
              names = names,
            )
            for {
              _ <- submitter
                .generateAndSubmit(
                  generator = generator,
                  config = submissionConfig,
                  baseActAs = List(allocatedParties.signatory) ++ allocatedParties.divulgees,
                  maxInFlightCommands = config.maxInFlightCommands,
                  submissionBatchSize = config.submissionBatchSize,
                )
            } yield ()
        }
    } yield allocatedParties
  }

  private def apiServicesOwner(
      config: Config,
      authorizationHelper: Option[AuthorizationHelper],
  )(implicit ec: ExecutionContext): ResourceOwner[String => LedgerApiServices] =
    for {
      executorService <- threadPoolExecutorOwner(config.concurrency)
      channel <- channelOwner(config.ledger, config.tls, executorService)
      servicesForUserId <- ResourceOwner.forFuture(() =>
        LedgerApiServices.forChannel(
          channel = channel,
          authorizationHelper = authorizationHelper,
        )
      )
    } yield servicesForUserId

  private def channelOwner(
      ledger: Config.Ledger,
      tls: TlsConfiguration,
      executor: Executor,
  ): ResourceOwner[Channel] = {
    logger.info(
      s"Setting up a managed channel to a ledger at: ${ledger.hostname}:${ledger.port}..."
    )
    val MessageChannelSizeBytes: Int = 32 * 1024 * 1024 // 32 MiB
    val ShutdownTimeout: FiniteDuration = 5.seconds

    val channelBuilder = NettyChannelBuilder
      .forAddress(ledger.hostname, ledger.port)
      .executor(executor)
      .maxInboundMessageSize(MessageChannelSizeBytes)
      .usePlaintext()

    if (tls.enabled) {
      tls.client().map { sslContext =>
        logger.info(s"Setting up a managed channel with transport security...")
        channelBuilder
          .useTransportSecurity()
          .sslContext(sslContext)
          .negotiationType(NegotiationType.TLS)
      }
    }

    ResourceOwner.forChannel(channelBuilder, ShutdownTimeout)
  }

  private def threadPoolExecutorOwner(
      config: Config.Concurrency
  ): ResourceOwner[ThreadPoolExecutor] =
    ResourceOwner.forExecutorService(() =>
      new ThreadPoolExecutor(
        config.corePoolSize,
        config.maxPoolSize,
        config.keepAliveTime,
        TimeUnit.SECONDS,
        if (config.maxQueueLength == 0) new SynchronousQueue[Runnable]()
        else new ArrayBlockingQueue[Runnable](config.maxQueueLength),
      )
    )
}
