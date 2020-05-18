package com.daml.ledger.participant.state.kvutils.app.batch

import org.junit.runner.RunWith
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._

class BatchingLedgerWriterConfigSpec extends WordSpec with Matchers {
  "optionsReader" should {
    import BatchingLedgerWriterConfig.optionsReader

    "return defaults for options not specified" in {
      val actual = optionsReader.reads("enable=true")
      actual shouldBe BatchingLedgerWriterConfig.reasonableDefault.copy(enableBatching = true)
    }

    "ignore unknown options" in {
      val actual = optionsReader.reads("some=value")
      actual shouldBe BatchingLedgerWriterConfig.reasonableDefault
    }

    "parse options specified" in {
      val actual = optionsReader.reads(
        "enable=true,max-queue-size=1,max-batch-size-bytes=10,max-wait-millis=100,max-concurrent-commits=1000")
      actual shouldBe BatchingLedgerWriterConfig(
        enableBatching = true,
        maxBatchQueueSize = 1,
        maxBatchSizeBytes = 10L,
        maxBatchWaitDuration = Duration(100, MILLISECONDS),
        maxBatchConcurrentCommits = 1000)
    }
  }
}
