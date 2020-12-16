// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app.batch

import com.daml.ledger.participant.state.kvutils.api.BatchingLedgerWriterConfig
import com.daml.ledger.participant.state.kvutils.app.batch.BatchingLedgerWriterConfigReader.optionsReader
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class BatchingLedgerWriterConfigReaderSpec extends AnyWordSpec with Matchers {
  "optionsReader" should {

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
