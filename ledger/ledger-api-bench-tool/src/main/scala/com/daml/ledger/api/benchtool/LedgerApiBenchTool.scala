// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import akka.actor.typed.{ActorSystem, SpawnProtocol}
import com.daml.ledger.api.benchtool.metrics.{
  Creator,
  MeteredStreamObserver,
  MetricsManager,
  TransactionMetrics,
}
import com.daml.ledger.api.benchtool.services.{LedgerIdentityService, TransactionService}
import com.daml.ledger.api.benchtool.util.TypedActorSystemResourceOwner
import com.daml.ledger.api.v1.transaction_service.{
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import io.grpc.Channel
import io.grpc.netty.NettyChannelBuilder
import org.slf4j.LoggerFactory

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
      channel <- channelOwner(config.ledger, executorService)
      system <- actorSystemResourceOwner()
    } yield (channel, system)

    resources.use { case (channel, system) =>
      val ledgerIdentityService: LedgerIdentityService = new LedgerIdentityService(channel)
      val ledgerId: String = ledgerIdentityService.fetchLedgerId()
      val transactionService = new TransactionService(channel, ledgerId)
      Future
        .traverse(config.streams) { streamConfig =>
          streamConfig.streamType match {
            case Config.StreamConfig.StreamType.Transactions =>
              TransactionMetrics
                .transactionsMetricsManager(
                  streamConfig.name,
                  config.reportingPeriod,
                  streamConfig.objectives,
                )(system)
                .flatMap { manager =>
                  val observer: MeteredStreamObserver[GetTransactionsResponse] =
                    new MeteredStreamObserver[GetTransactionsResponse](
                      streamConfig.name,
                      logger,
                      manager,
                    )(system)
                  transactionService.transactions(streamConfig, observer)
                }
            case Config.StreamConfig.StreamType.TransactionTrees =>
              TransactionMetrics
                .transactionTreesMetricsManager(
                  streamConfig.name,
                  config.reportingPeriod,
                  streamConfig.objectives,
                )(system)
                .flatMap { manager =>
                  val observer =
                    new MeteredStreamObserver[GetTransactionTreesResponse](
                      streamConfig.name,
                      logger,
                      manager,
                    )(system)
                  transactionService.transactionTrees(streamConfig, observer)
                }
          }
        }
        .transform {
          case Success(results) =>
            if (results.contains(MetricsManager.Message.MetricsResult.ObjectivesViolated))
              Failure(new RuntimeException("Metrics objectives not met."))
            else Success(())
          case Failure(ex) =>
            Failure(ex)
        }
    }
  }

  // TODO: add TLS compatible with the ledger-api-test-tool
  private def channelOwner(ledger: Config.Ledger, executor: Executor): ResourceOwner[Channel] = {
    val MessageChannelSizeBytes: Int = 32 * 1024 * 1024 // 32 MiB
    val ShutdownTimeout: FiniteDuration = 5.seconds

    val channelBuilder = NettyChannelBuilder
      .forAddress(ledger.hostname, ledger.port)
      .executor(executor)
      .maxInboundMessageSize(MessageChannelSizeBytes)
      .usePlaintext()

    ResourceOwner.forChannel(channelBuilder, ShutdownTimeout)
  }

  private def actorSystemResourceOwner(): ResourceOwner[ActorSystem[SpawnProtocol.Command]] =
    new TypedActorSystemResourceOwner[SpawnProtocol.Command](() =>
      ActorSystem(Creator(), "Creator")
    )

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

  private val logger = LoggerFactory.getLogger(getClass)
}
