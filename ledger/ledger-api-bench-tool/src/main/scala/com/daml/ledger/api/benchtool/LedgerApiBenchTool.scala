// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.config.{Config, ConfigMaker, WorkflowConfig}
import com.daml.ledger.api.benchtool.submission.CommandSubmitter
import com.daml.ledger.api.benchtool.services._
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import org.slf4j.{Logger, LoggerFactory}
import pprint.PPrinter

import java.util.concurrent.{
  ArrayBlockingQueue,
  Executor,
  SynchronousQueue,
  ThreadPoolExecutor,
  TimeUnit,
}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

object LedgerApiBenchTool {
  def main(args: Array[String]): Unit = {
    ConfigMaker.make(args) match {
      case Left(error) =>
        logger.error(s"Configuration error: ${error.details}")
      case Right(config) =>
        logger.info(s"Starting benchmark with configuration:\n${prettyPrint(config)}")
        val result = run(config)(ExecutionContext.Implicits.global)
          .recover { case ex =>
            println(s"Error: ${ex.getMessage}")
            sys.exit(1)
          }(scala.concurrent.ExecutionContext.Implicits.global)
        Await.result(result, atMost = Duration.Inf)
        ()
    }
  }

  private def run(config: Config)(implicit ec: ExecutionContext): Future[Unit] = {
    implicit val resourceContext: ResourceContext = ResourceContext(ec)
    apiServicesOwner(config).use { apiServices =>
      def benchmarkStep(streams: List[WorkflowConfig.StreamConfig]): Future[Unit] =
        if (streams.isEmpty) {
          Future.successful(logger.info(s"No streams defined. Skipping the benchmark step."))
        } else {
          Benchmark.run(
            streams = streams,
            reportingPeriod = config.reportingPeriod,
            apiServices = apiServices,
            metricsReporter = config.metricsReporter,
          )
        }

      def submissionStep(
          submissionConfig: Option[WorkflowConfig.SubmissionConfig]
      ): Future[Option[CommandSubmitter.SubmissionSummary]] =
        submissionConfig match {
          case None =>
            logger.info(s"No submission defined. Skipping.")
            Future.successful(None)
          case Some(submissionConfig) =>
            CommandSubmitter(apiServices)
              .submit(
                config = submissionConfig,
                maxInFlightCommands = config.maxInFlightCommands,
                submissionBatchSize = config.submissionBatchSize,
              )
              .map(Some(_))
        }

      for {
        summary <- submissionStep(config.workflow.submission)
        streams = config.workflow.streams.map(
          ConfigEnricher.enrichedStreamConfig(_, summary)
        )
        _ = logger.info(
          s"Stream configs adapted after the submission step: ${prettyPrint(streams)}"
        )
        _ <- benchmarkStep(streams)
      } yield ()
    }
  }

  private def apiServicesOwner(
      config: Config
  )(implicit ec: ExecutionContext): ResourceOwner[LedgerApiServices] =
    for {
      executorService <- threadPoolExecutorOwner(config.concurrency)
      channel <- channelOwner(config.ledger, config.tls, executorService)
      services <- ResourceOwner.forFuture(() => LedgerApiServices.forChannel(channel))
    } yield services

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
  private val printer: PPrinter = pprint.PPrinter(200, 1000)
  private def prettyPrint(x: Any): String = printer(x).toString()
}
