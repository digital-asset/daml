// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.config

import com.digitalasset.canton.config.TlsClientConfig

import java.io.File
import scala.concurrent.duration.*

final case class Config(
    ledger: Config.Ledger,
    concurrency: Config.Concurrency,
    tls: Option[TlsClientConfig],
    workflow: WorkflowConfig,
    reportingPeriod: FiniteDuration,
    workflowConfigFile: Option[File],
    maxInFlightCommands: Int,
    submissionBatchSize: Int,
    authorizationTokenSecret: Option[String],
    latencyTest: Boolean,
    maxLatencyObjectiveMillis: Long,
) {
  def withLedgerConfig(f: Config.Ledger => Config.Ledger): Config = copy(ledger = f(ledger))
}

object Config {
  final case class Ledger(
      hostname: String,
      port: Int,
      indexDbJdbcUrlO: Option[String] = None,
  )

  final case class Concurrency(
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
      tls = None,
      workflow = WorkflowConfig(),
      reportingPeriod = 5.seconds,
      workflowConfigFile = None,
      maxInFlightCommands = 100,
      submissionBatchSize = 100,
      authorizationTokenSecret = None,
      latencyTest = false,
      maxLatencyObjectiveMillis = 1000L,
    )
}
