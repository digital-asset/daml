package com.daml.ledger.participant.state.kvutils.app.batch

import scopt.Read

import scala.concurrent.duration._

/** Configuration for the batching ledger writer.
  *
  * @param enableBatching Enables batching if true.
  * @param maxBatchQueueSize The maximum number of submission to queue before dropping.
  * @param maxBatchSizeBytes Maximum size threshold after which batch is immediately emitted.
  * @param maxBatchWaitDuration The maximum duration to wait before emitting the batch.
  * @param maxBatchConcurrentCommits Maximum number of concurrent calls to commit.
  */
final case class BatchingLedgerWriterConfig(
    enableBatching: Boolean,
    maxBatchQueueSize: Int,
    maxBatchSizeBytes: Long,
    maxBatchWaitDuration: FiniteDuration,
    maxBatchConcurrentCommits: Int
)

object BatchingLedgerWriterConfig {
  val reasonableDefault: BatchingLedgerWriterConfig =
    BatchingLedgerWriterConfig(
      enableBatching = false,
      maxBatchQueueSize = 200,
      maxBatchSizeBytes = 4L * 1024L * 1024L /* 4MB */,
      maxBatchWaitDuration = 100.millis,
      maxBatchConcurrentCommits = 5
    )

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
