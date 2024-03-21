// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import com.daml.metrics.{MetricName, Metrics}
import com.codahale.metrics.{Counter, Histogram, MetricRegistry, Timer}

class BridgeMetrics(metrics: Metrics) {
  val registry: MetricRegistry = metrics.registry

  val Prefix: MetricName = MetricName.Daml :+ "sandbox_ledger_bridge"

  val threadpool: MetricName = Prefix :+ "threadpool"

  object Stages {
    val Prefix: MetricName = BridgeMetrics.this.Prefix :+ "stages"

    case class StageMetrics(stageName: String) {
      protected val prefix: MetricName = Stages.this.Prefix :+ stageName
      val timer: Timer = registry.timer(prefix :+ "timer")
      val bufferBefore: Counter = registry.counter(prefix :+ "buffer")
    }

    object PrepareSubmission extends StageMetrics("prepare_submission")
    object TagWithLedgerEnd extends StageMetrics("tag_with_ledger_end")
    object ConflictCheckWithCommitted extends StageMetrics("conflict_check_with_committed")
    object Sequence extends StageMetrics("sequence") {
      val statePrefix: MetricName = prefix :+ "state"
      val keyStateSize: Histogram = registry.histogram(statePrefix :+ "keys")
      val consumedContractsStateSize: Histogram =
        registry.histogram(statePrefix :+ "consumed_contracts")
      val sequencerQueueLength: Histogram = registry.histogram(statePrefix :+ "queue")
      val deduplicationQueueLength: Histogram =
        registry.histogram(statePrefix :+ "deduplication_queue")
    }
  }

  object BridgeInputQueue {
    val Prefix: MetricName = BridgeMetrics.this.Prefix :+ "input_queue"

    val conflictQueueCapacity: Counter = registry.counter(Prefix :+ "capacity")
    val conflictQueueLength: Counter = registry.counter(Prefix :+ "length")
    val conflictQueueDelay: Timer = registry.timer(Prefix :+ "delay")
  }
}
