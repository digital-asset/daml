// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.metrics.{
  MetricRegistryOwner,
  MetricsCollector,
  MetricsSet,
  StreamMetrics,
}
import com.daml.ledger.api.benchtool.services.{
  ActiveContractsService,
  CommandCompletionService,
  LedgerIdentityService,
  TransactionService,
}
import com.daml.ledger.api.benchtool.util.TypedActorSystemResourceOwner
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{
  ArrayBlockingQueue,
  Executor,
  SynchronousQueue,
  ThreadPoolExecutor,
  TimeUnit,
}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

object LedgerApiBenchTool {
  def main(args: Array[String]): Unit = {
    Cli.config(args) match {
      case Some(config) =>
        val benchmark = runBenchmark(config)(ExecutionContext.Implicits.global)
          .recover { case ex =>
            println(s"Error: ${ex.getMessage}")
            sys.exit(1)
          }(scala.concurrent.ExecutionContext.Implicits.global)
        Await.result(benchmark, atMost = Duration.Inf)
        ()
      case _ =>
        logger.error("Invalid configuration arguments.")
    }
  }

  private def runBenchmark(config: Config)(implicit ec: ExecutionContext): Future[Unit] = {
    val printer = pprint.PPrinter(200, 1000)
    logger.info(s"Starting benchmark with configuration:\n${printer(config).toString()}")

    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    val resources = for {
      executorService <- threadPoolExecutorOwner(config.concurrency)
      channel <- channelOwner(config.ledger, config.tls, executorService)
      system <- TypedActorSystemResourceOwner.owner()
      registry <- new MetricRegistryOwner(
        reporter = config.metricsReporter,
        reportingInterval = config.reportingPeriod,
        logger = logger,
      )
    } yield (channel, system, registry)

    resources.use { case (channel, system, registry) =>
      val ledgerIdentityService: LedgerIdentityService = new LedgerIdentityService(channel)
      val ledgerId: String = ledgerIdentityService.fetchLedgerId()
      val transactionService = new TransactionService(channel, ledgerId)
      val activeContractsService = new ActiveContractsService(channel, ledgerId)
      val commandCompletionService = new CommandCompletionService(channel, ledgerId)
      Future
        .traverse(config.streams) {
          case streamConfig: Config.StreamConfig.TransactionsStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = config.reportingPeriod,
                metrics = MetricsSet.transactionMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet
                    .transactionExposedMetrics(streamConfig.name, registry, config.reportingPeriod)
                ),
              )(system, ec)
              .flatMap { observer =>
                transactionService.transactions(streamConfig, observer)
              }
          case streamConfig: Config.StreamConfig.TransactionTreesStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = config.reportingPeriod,
                metrics = MetricsSet.transactionTreesMetrics(streamConfig.objectives),
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet.transactionTreesExposedMetrics(
                    streamConfig.name,
                    registry,
                    config.reportingPeriod,
                  )
                ),
              )(system, ec)
              .flatMap { observer =>
                transactionService.transactionTrees(streamConfig, observer)
              }
          case streamConfig: Config.StreamConfig.ActiveContractsStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = config.reportingPeriod,
                metrics = MetricsSet.activeContractsMetrics,
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet.activeContractsExposedMetrics(
                    streamConfig.name,
                    registry,
                    config.reportingPeriod,
                  )
                ),
              )(system, ec)
              .flatMap { observer =>
                activeContractsService.getActiveContracts(streamConfig, observer)
              }
          case streamConfig: Config.StreamConfig.CompletionsStreamConfig =>
            StreamMetrics
              .observer(
                streamName = streamConfig.name,
                logInterval = config.reportingPeriod,
                metrics = MetricsSet.completionsMetrics,
                logger = logger,
                exposedMetrics = Some(
                  MetricsSet
                    .completionsExposedMetrics(streamConfig.name, registry, config.reportingPeriod)
                ),
              )(system, ec)
              .flatMap { observer =>
                commandCompletionService.completions(streamConfig, observer)
              }
        }
        .transform {
          case Success(results) =>
            if (results.contains(MetricsCollector.Message.MetricsResult.ObjectivesViolated))
              Failure(new RuntimeException("Metrics objectives not met."))
            else Success(())
          case Failure(ex) =>
            Failure(ex)
        }
    }
  }

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
}
