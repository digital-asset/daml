// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.benchtool.config.{Config, ConfigMaker, WorkflowConfig}
import com.daml.ledger.api.benchtool.metrics.MetricsManager.NoOpMetricsManager
import com.daml.ledger.api.benchtool.metrics.{
  BenchmarkResult,
  LatencyMetric,
  MetricRegistryOwner,
  MetricsManager,
}
import com.daml.ledger.api.benchtool.services.LedgerApiServices
import com.daml.ledger.api.benchtool.submission.{CommandSubmitter, Names}
import com.daml.ledger.api.benchtool.util.TypedActorSystemResourceOwner
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.client
import com.daml.ledger.client.binding.Primitive
import com.daml.ledger.participant.state.index.v2.UserManagementStore
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent._
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

/** Runs a submission step followed by a benchmark step.
  * Either step is optional.
  *
  * Uses "benchtool" ([[Names.benchtoolApplicationId]]) applicationId for both steps.
  */
object LedgerApiBenchTool {
  def main(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    ConfigMaker.make(args) match {
      case Left(error) =>
        logger.error(s"Configuration error: ${error.details}")
      case Right(config) =>
        logger.info(s"Starting benchmark with configuration:\n${prettyPrint(config)}")
        val result = run(config)(ExecutionContext.Implicits.global)
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

  private def run(config: Config)(implicit ec: ExecutionContext): Future[Either[String, Unit]] = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    val names = new Names
    val authorizationHelper = config.authorizationTokenSecret.map(new AuthorizationHelper(_))

    def regularUserSetupStep(adminServices: LedgerApiServices): Future[Unit] =
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

    def benchmarkStreams(
        regularUserServices: LedgerApiServices,
        streamConfigs: List[WorkflowConfig.StreamConfig],
        metricRegistry: MetricRegistry,
        actorSystem: ActorSystem[SpawnProtocol.Command],
    ): Future[Either[String, Unit]] =
      if (streamConfigs.isEmpty) {
        logger.info(s"No streams defined. Skipping the benchmark step.")
        Future.successful(Right(()))
      } else
        Benchmark
          .run(
            streamConfigs = streamConfigs,
            reportingPeriod = config.reportingPeriod,
            apiServices = regularUserServices,
            metricRegistry = metricRegistry,
            system = actorSystem,
          )

    def benchmarkLatency(
        regularUserServices: LedgerApiServices,
        adminServices: LedgerApiServices,
        submissionConfigO: Option[WorkflowConfig.SubmissionConfig],
        metricRegistry: MetricRegistry,
        signatory: client.binding.Primitive.Party,
        actorSystem: ActorSystem[SpawnProtocol.Command],
        maxLatencyObjectiveMillis: Long,
    ): Future[Either[String, Unit]] =
      submissionConfigO match {
        case Some(submissionConfig) =>
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
              metricRegistry = metricRegistry,
              metricsManager = metricsManager,
            )
            result <- submitter
              .submit(
                config = submissionConfig,
                signatory = signatory,
                observers = Nil,
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
        case None =>
          Future.failed(
            new RuntimeException("Submission config cannot be empty for latency benchmarking")
          )
      }

    def submissionStep(
        regularUserServices: LedgerApiServices,
        adminServices: LedgerApiServices,
        submissionConfig: Option[WorkflowConfig.SubmissionConfig],
        metricRegistry: MetricRegistry,
    )(implicit ec: ExecutionContext): Future[
      Option[(Primitive.Party, CommandSubmitter.SubmissionSummary)]
    ] =
      submissionConfig match {
        case None =>
          logger.info(s"No submission defined. Skipping.")
          Future.successful(None)
        case Some(submissionConfig) =>
          val submitter = CommandSubmitter(
            names = names,
            benchtoolUserServices = regularUserServices,
            adminServices = adminServices,
            metricRegistry = metricRegistry,
            metricsManager = NoOpMetricsManager(),
          )
          for {
            (signatory, observers) <- submitter.prepare(submissionConfig)
            result <- submitter
              .submit(
                config = submissionConfig,
                signatory = signatory,
                observers = observers,
                maxInFlightCommands = config.maxInFlightCommands,
                submissionBatchSize = config.submissionBatchSize,
              )
          } yield Some((signatory, result))
      }

    val resources = for {
      servicesForUserId <- apiServicesOwner(config, authorizationHelper)
      system <- TypedActorSystemResourceOwner.owner()
      metricRegistry <- new MetricRegistryOwner(
        reporter = config.metricsReporter,
        reportingInterval = config.reportingPeriod,
        logger = logger,
      )
    } yield (servicesForUserId, system, metricRegistry)

    resources.use { case (servicesForUserId, actorSystem, metricRegistry) =>
      val adminServices = servicesForUserId(UserManagementStore.DefaultParticipantAdminUserId)
      val regularUserServices = servicesForUserId(names.benchtoolUserId)

      for {
        _ <- regularUserSetupStep(adminServices)
        submissionStepProducts <- submissionStep(
          regularUserServices = regularUserServices,
          adminServices = adminServices,
          submissionConfig = config.workflow.submission,
          metricRegistry = metricRegistry,
        )
        streams = config.workflow.streams.map(
          ConfigEnricher.enrichStreamConfig(_, submissionStepProducts.map(_._2))
        )
        _ = logger.info(
          s"Stream configs adapted after the submission step: ${prettyPrint(streams)}"
        )
        benchmarkResult <-
          if (config.latencyTest) {
            val signatory = submissionStepProducts
              .map(_._1)
              .getOrElse(
                throw new RuntimeException("Signatory cannot be empty for latency benchmark")
              )
            benchmarkLatency(
              regularUserServices,
              adminServices,
              config.workflow.submission,
              metricRegistry,
              signatory,
              actorSystem,
              config.maxLatencyObjectiveMillis,
            )
          } else benchmarkStreams(regularUserServices, streams, metricRegistry, actorSystem)
      } yield benchmarkResult
    }
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

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val printer = pprint.PPrinter.BlackWhite
  private def prettyPrint(x: Any): String = printer(x).toString()
}
