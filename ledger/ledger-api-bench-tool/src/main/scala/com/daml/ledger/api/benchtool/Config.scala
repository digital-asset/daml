// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.value.Identifier
import com.daml.metrics.MetricsReporter

import java.io.File
import scala.concurrent.duration._

case class Config(
    ledger: Config.Ledger,
    concurrency: Config.Concurrency,
    tls: TlsConfiguration,
    streams: List[Config.StreamConfig],
    reportingPeriod: FiniteDuration,
    contractSetDescriptorFile: Option[File],
    maxInFlightCommands: Int,
    metricsReporter: MetricsReporter,
)

object Config {
  trait StreamConfig {
    def name: String
    def party: String
  }

  object StreamConfig {
    case class TransactionsStreamConfig(
        name: String,
        party: String,
        templateIds: Option[List[Identifier]],
        beginOffset: Option[LedgerOffset],
        endOffset: Option[LedgerOffset],
        objectives: StreamConfig.Objectives,
    ) extends StreamConfig

    case class TransactionTreesStreamConfig(
        name: String,
        party: String,
        templateIds: Option[List[Identifier]],
        beginOffset: Option[LedgerOffset],
        endOffset: Option[LedgerOffset],
        objectives: StreamConfig.Objectives,
    ) extends StreamConfig

    case class ActiveContractsStreamConfig(
        name: String,
        party: String,
        templateIds: Option[List[Identifier]],
    ) extends StreamConfig

    case class CompletionsStreamConfig(
        name: String,
        party: String,
        applicationId: String,
        beginOffset: Option[LedgerOffset],
    ) extends StreamConfig

    case class Objectives(
        maxDelaySeconds: Option[Long],
        minConsumptionSpeed: Option[Double],
    )
  }

  case class Ledger(
      hostname: String,
      port: Int,
  )

  case class Concurrency(
      corePoolSize: Int,
      maxPoolSize: Int,
      keepAliveTime: Long,
      maxQueueLength: Int,
  )

  val Default: Config =
    Config(
      ledger = Config.Ledger(
        hostname = "localhost",
        port = 6865,
      ),
      concurrency = Config.Concurrency(
        corePoolSize = 2,
        maxPoolSize = 8,
        keepAliveTime = 30,
        maxQueueLength = 10000,
      ),
      tls = TlsConfiguration.Empty.copy(enabled = false),
      streams = List.empty[Config.StreamConfig],
      reportingPeriod = 5.seconds,
      contractSetDescriptorFile = None,
      maxInFlightCommands = 100,
      metricsReporter = MetricsReporter.Console,
    )
}
