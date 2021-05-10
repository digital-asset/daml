// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import scala.concurrent.duration._

case class Config(
    ledger: Config.Ledger,
    concurrency: Config.Concurrency,
    streamConfig: Option[Config.StreamConfig],
    reportingPeriod: Duration,
)

object Config {
  case class StreamConfig(
      name: String,
      streamType: Config.StreamConfig.StreamType,
      party: String,
  )

  object StreamConfig {
    sealed trait StreamType
    object StreamType {
      case object Transactions extends StreamType
      case object TransactionTrees extends StreamType
    }
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
      streamConfig = None,
      reportingPeriod = 5.seconds,
    )
}
