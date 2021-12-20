// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.participant.state.kvutils.app.{Config, ConfigProvider}
import scopt.OptionParser

case class BridgeConfig(
    conflictCheckingEnabled: Boolean,
    maxDedupSeconds: Int,
    submissionBufferSize: Int,
    bridgeThreadPoolSize: Int,
)

object BridgeConfigProvider extends ConfigProvider[BridgeConfig] {
  override def extraConfigParser(parser: OptionParser[Config[BridgeConfig]]): Unit = {
    parser
      .opt[Int]("bridge-max-dedup-seconds")
      .text("Maximum deduplication time in seconds. Defaults to 30.")
      .action((p, c) => c.copy(extra = c.extra.copy(maxDedupSeconds = p)))

    parser
      .opt[Int]("bridge-submission-buffer-size")
      .text("Submission buffer size. Defaults to 500.")
      .action((p, c) => c.copy(extra = c.extra.copy(submissionBufferSize = p)))

    parser
      .opt[Unit]("enable-conflict-checking")
      .text("Enables the ledger-side submission conflict checking.")
      .action((_, c) => c.copy(extra = c.extra.copy(conflictCheckingEnabled = true)))

    parser
      .opt[Int]("bridge-threadpool-size")
      .text("The thread-pool size used for the ledger-side conflict checking")
      .action((p, c) => c.copy(extra = c.extra.copy(bridgeThreadPoolSize = p)))

    ()
  }

  override val defaultExtraConfig: BridgeConfig = BridgeConfig(
    // TODO SoX: Enabled by default
    conflictCheckingEnabled = false,
    maxDedupSeconds = 30,
    submissionBufferSize = 500,
    bridgeThreadPoolSize = 8,
  )
}
