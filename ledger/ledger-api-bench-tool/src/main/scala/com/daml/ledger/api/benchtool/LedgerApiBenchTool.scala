// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.Config.StreamConfig
import com.daml.ledger.api.benchtool.submission.CommandSubmitter
import com.daml.ledger.api.benchtool.services._
import com.daml.ledger.api.benchtool.util.SimpleFileReader
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.ledger.test.model.Foo.{Foo1, Foo2, Foo3}
import io.grpc.Channel
import io.grpc.netty.{NegotiationType, NettyChannelBuilder}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.util.concurrent.{
  ArrayBlockingQueue,
  Executor,
  SynchronousQueue,
  ThreadPoolExecutor,
  TimeUnit,
}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object LedgerApiBenchTool {
  def main(args: Array[String]): Unit = {
    Cli.config(args) match {
      case Some(config) =>
        val result = run(config)(ExecutionContext.Implicits.global)
          .recover { case ex =>
            println(s"Error: ${ex.getMessage}")
            sys.exit(1)
          }(scala.concurrent.ExecutionContext.Implicits.global)
        Await.result(result, atMost = Duration.Inf)
        ()
      case _ =>
        logger.error("Invalid configuration arguments.")
    }
  }

  private def run(config: Config)(implicit ec: ExecutionContext): Future[Unit] = {
    val printer = pprint.PPrinter(200, 1000)
    logger.info(s"Starting benchmark with configuration:\n${printer(config).toString()}")

    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    apiServicesOwner(config).use { apiServices =>
      def benchmarkStep(): Future[Unit] =
        if (config.streams.isEmpty) {
          Future.successful(logger.info(s"No streams defined. Skipping the benchmark step."))
        } else {
          Benchmark.run(
            streams = config.streams,
            reportingPeriod = config.reportingPeriod,
            apiServices = apiServices,
            metricsReporter = config.metricsReporter,
          )
        }

      def benchmarkFromDescriptorStep(
          streams: List[StreamDescriptor],
          submissionSummary: CommandSubmitter.SubmissionSummary,
      ): Future[Unit] = {
        def toConfig(descriptor: StreamDescriptor): StreamConfig = {
          import scalaz.syntax.tag._
          def templateStringToId(template: String) = template match {
            case "Foo1" => Foo1.id.unwrap
            case "Foo2" => Foo2.id.unwrap
            case "Foo3" => Foo3.id.unwrap
            case invalid => throw new RuntimeException(s"Invalid template: $invalid")
          }
          def partyFromObservers(party: String): String =
            submissionSummary.observers
              .map(_.unwrap)
              .find(_.contains(party))
              .getOrElse(throw new RuntimeException(s"Observer not found: $party"))

          val filters = descriptor.filters.map { filter =>
            partyFromObservers(filter.party) -> Some(filter.templates.map(templateStringToId))
          }.toMap

          descriptor.streamType match {
            case StreamDescriptor.StreamType.ActiveContracts =>
              Config.StreamConfig.ActiveContractsStreamConfig(
                name = descriptor.name,
                filters = filters,
              )
            case invalid =>
              throw new RuntimeException(s"Invalid stream type: $invalid")
          }
        }
        Benchmark.run(
          streams = streams.map(toConfig),
          reportingPeriod = config.reportingPeriod,
          apiServices = apiServices,
          metricsReporter = config.metricsReporter,
        )
      }

      config.contractSetDescriptorFile match {
        case None => benchmarkStep()
        case Some(descriptorFile) =>
          for {
            descriptor <- Future.fromTry(parseDescriptor(descriptorFile))
            summary <- CommandSubmitter(apiServices).submit(
              descriptor.submission,
              config.maxInFlightCommands,
              config.submissionBatchSize,
            )
            _ <- benchmarkFromDescriptorStep(descriptor.streams, summary)
          } yield ()
      }
    }
  }

  private def parseDescriptor(descriptorFile: File): Try[WorkflowDescriptor] =
    SimpleFileReader.readFile(descriptorFile)(WorkflowParser.parse).flatMap {
      case Left(err: WorkflowParser.ParserError) =>
        val message = s"Workflow parsing error. Details: ${err.details}"
        logger.error(message)
        Failure(CommandSubmitter.CommandSubmitterError(message))
      case Right(descriptor) =>
        logger.info(s"Descriptor parsed: $descriptor")
        Success(descriptor)
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
}
