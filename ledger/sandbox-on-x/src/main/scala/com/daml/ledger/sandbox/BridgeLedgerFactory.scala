// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import akka.stream.Materializer
import com.daml.ledger.participant.state.kvutils.app.{
  Config,
  ConfigProvider,
  LedgerFactory,
  ParticipantConfig,
  ReadWriteServiceFactory,
}
import com.daml.ledger.participant.state.v2.{ReadService, WritePackagesService, WriteService}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import scopt.OptionParser

import scala.concurrent.ExecutionContext

case class BridgeConfig(maxDedupSeconds: Int, submissionBufferSize: Int)

object BridgeLedgerFactory extends LedgerFactory[BridgeConfig] {

  override def readWriteServiceFactoryOwner(
      config: Config[BridgeConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
      metrics: Metrics,
  )(implicit
      materializer: Materializer,
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): ResourceOwner[ReadWriteServiceFactory] = {
    ResourceOwner
      .forCloseable(() =>
        ReadWriteServiceBridge(
          participantId = participantConfig.participantId,
          ledgerId = config.ledgerId,
          maxDedupSeconds = config.extra.maxDedupSeconds,
          submissionBufferSize = config.extra.submissionBufferSize,
        )
      )
      .map(readWriteService => {
        new ReadWriteServiceFactory {
          override def readService(): ReadService = readWriteService

          override def writePackageService(): WritePackagesService = readWriteService

          override def writeService(): WriteService = readWriteService
        }
      })
  }

}
object BridgeConfigProvider extends ConfigProvider[BridgeConfig] {

  override def extraConfigParser(parser: OptionParser[Config[BridgeConfig]]): Unit = {
    parser
      .opt[Int]("sandbox-on-x-bridge-max-dedup-seconds")
      .text("Maximum deduplication time in seconds. Defaults to 30.")
      .action((p, c) => c.copy(extra = c.extra.copy(maxDedupSeconds = p)))

    parser
      .opt[Int]("sandbox-on-x-bridge-submission-buffer-size")
      .text("Submission buffer size. Defaults to 200.")
      .action((p, c) => c.copy(extra = c.extra.copy(submissionBufferSize = p)))

    parser
      .opt[Unit]("sandbox-on-x-bridge")
      .text("Placeholder for the configuration turning on the sandbox-on-x bridge.")

    ()
  }

  override val defaultExtraConfig: BridgeConfig = BridgeConfig(
    maxDedupSeconds = 30,
    submissionBufferSize = 500,
  )
}
