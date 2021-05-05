// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

case class Config(
    streamType: Config.StreamType,
    ledger: Config.Ledger,
    concurrency: Config.Concurrency,
)

object Config {
  sealed trait StreamType
  object StreamType {
    case object Transactions extends StreamType
    case object TransactionTrees extends StreamType
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

}
