// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import com.daml.metrics.api.MetricHandle.{Counter, Factory, Histogram, Timer}
import com.daml.metrics.api.MetricName

class BridgeMetrics(factory: Factory) {

  val prefix: MetricName = MetricName.Daml :+ "sandbox_ledger_bridge"

  val threadpool: MetricName = prefix :+ "threadpool"

  object Stages {
    val prefix: MetricName = BridgeMetrics.this.prefix :+ "stages"

    case class StageMetrics(stageName: String) {
      protected val prefix: MetricName = Stages.this.prefix :+ stageName
      val timer: Timer = factory.timer(prefix :+ "timer")
      val bufferBefore: Counter = factory.counter(prefix :+ "buffer")
    }

    object PrepareSubmission extends StageMetrics("prepare_submission")
    object TagWithLedgerEnd extends StageMetrics("tag_with_ledger_end")
    object ConflictCheckWithCommitted extends StageMetrics("conflict_check_with_committed")
    object Sequence extends StageMetrics("sequence") {
      val statePrefix: MetricName = prefix :+ "state"
      val keyStateSize: Histogram = factory.histogram(statePrefix :+ "keys")
      val consumedContractsStateSize: Histogram =
        factory.histogram(statePrefix :+ "consumed_contracts")
      val sequencerQueueLength: Histogram = factory.histogram(statePrefix :+ "queue")
      val deduplicationQueueLength: Histogram =
        factory.histogram(statePrefix :+ "deduplication_queue")
    }
  }

  object BridgeInputQueue {
    val prefix: MetricName = BridgeMetrics.this.prefix :+ "input_queue"

    val conflictQueueCapacity: Counter = factory.counter(prefix :+ "capacity")
    val conflictQueueLength: Counter = factory.counter(prefix :+ "length")
    val conflictQueueDelay: Timer = factory.timer(prefix :+ "delay")
  }
}
