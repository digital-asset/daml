// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.benchtool.services.{LedgerIdentityService, TransactionService}
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
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

object LedgerApiBenchTool {
  def main(args: Array[String]): Unit = {
    Cli.config(args) match {
      case Some(config) =>
        val benchmark = runBenchmark(config)(ExecutionContext.Implicits.global)
        Await.result(benchmark, atMost = Duration.Inf)
      case _ =>
        logger.error("Invalid configuration arguments.")
    }
  }

  private def runBenchmark(config: Config)(implicit ec: ExecutionContext): Future[Unit] = {
    logger.info(s"Starting benchmark")
    logger.info(config.toString) //TODO: print a nice representation

    implicit val resourceContext: ResourceContext = ResourceContext(ec)

    val channel = for {
      executorService <- threadPoolExecutorOwner(config.concurrency)
      channel <- channelOwner(config.ledger, executorService)
    } yield channel

    channel.use { channel =>
      Future {
        val ledgerIdentityService: LedgerIdentityService = new LedgerIdentityService(channel)
        val ledgerId: String = ledgerIdentityService.fetchLedgerId()
        val transactionService = new TransactionService(channel, ledgerId)
        config.streamConfig.foreach { streamConfig =>
          streamConfig.streamType match {
            case Config.StreamConfig.StreamType.Transactions =>
              transactionService.transactions(streamConfig.party)
              ()
            case Config.StreamConfig.StreamType.TransactionTrees =>
              transactionService.transactionTrees(streamConfig.party)
              ()
          }
        }
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
