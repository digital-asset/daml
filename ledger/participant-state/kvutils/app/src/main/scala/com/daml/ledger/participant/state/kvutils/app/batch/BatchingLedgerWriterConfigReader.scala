// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app.batch

import com.daml.ledger.participant.state.kvutils.api.BatchingLedgerWriterConfig
import com.daml.ledger.participant.state.kvutils.api.BatchingLedgerWriterConfig.reasonableDefault
import scopt.Read

import scala.concurrent.duration.{Duration, MILLISECONDS}

/**
  * Provides an options reader for parsing configuration settings for batching submissions.
  * Example usage:
  * {{{
  *  import com.daml.ledger.participant.state.kvutils.api.BatchingLedgerWriterConfig
  *  import com.daml.ledger.participant.state.kvutils.app.batch.BatchingLedgerWriterConfigReader
  *  import com.daml.ledger.participant.state.kvutils.app.batch.BatchingLedgerWriterConfigReader.optionsReader
  *
  *  val parser = ...
  *  parser
  *    .opt[BatchingLedgerWriterConfig]("batching")
  *    .optional()
  *    .text(BatchingLedgerWriterConfigReader.UsageText)
  *    .action {
  *           case (parsedBatchingConfig, config) =>
  *             config.copy(
  *               extra = config.extra.copy(
  *                 batchingConfig = parsedBatchingConfig
  *               )
  *             )
  *         }
  * }}}
  *
  */
object BatchingLedgerWriterConfigReader {
  lazy val UsageText: String = {
    val default = BatchingLedgerWriterConfig.reasonableDefault
    "Configuration for batching of submissions. The optional comma-separated key-value pairs with their defaults are: [" +
      List(
        s"enable=${default.enableBatching}",
        s"max-queue-size=${default.maxBatchQueueSize}",
        s"max-batch-size-bytes=${default.maxBatchSizeBytes}",
        s"max-wait-millis=${default.maxBatchWaitDuration.toMillis}",
        s"max-concurrent-commits=${default.maxBatchConcurrentCommits}"
      ).mkString(", ") + "]"
  }

  implicit val optionsReader: Read[BatchingLedgerWriterConfig] = Read.mapRead[String, String].map {
    _.foldLeft[BatchingLedgerWriterConfig](reasonableDefault) {
      case (config, (key, value)) =>
        key match {
          case "enable" => config.copy(enableBatching = Read.booleanRead.reads(value))
          case "max-queue-size" => config.copy(maxBatchQueueSize = Read.intRead.reads(value))
          case "max-batch-size-bytes" => config.copy(maxBatchSizeBytes = Read.longRead.reads(value))
          case "max-wait-millis" =>
            config.copy(maxBatchWaitDuration = Duration(Read.longRead.reads(value), MILLISECONDS))
          case "max-concurrent-commits" =>
            config.copy(maxBatchConcurrentCommits = Read.intRead.reads(value))
          case _ => config
        }
    }
  }
}
