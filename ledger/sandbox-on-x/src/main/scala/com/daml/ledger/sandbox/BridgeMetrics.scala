// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.codahale.metrics.{Counter, Histogram, Timer}
import com.daml.metrics.{MetricName, Metrics}

class BridgeMetrics(metrics: Metrics) {
  private val registry = metrics.registry

  val Prefix: MetricName = MetricName.Daml :+ "ledger"

  val threadpool: MetricName = Prefix :+ "threadpool"
  val conflictCheckingDelay: Timer = registry.timer(Prefix :+ "conflict_checking_delay")

  object Stages {
    val Prefix: MetricName = BridgeMetrics.this.Prefix :+ "stages"

    val precomputeTransactionOutputs: Timer =
      registry.timer(Prefix :+ "precompute_transaction_outputs")
    val conflictCheckWithCommitted: Timer =
      registry.timer(Prefix :+ "conflict_check_with_committed")
    val conflictCheckWithDelta: Timer = registry.timer(Prefix :+ "conflict_check_with_delta")
  }

  object SequencerState {
    val Prefix: MetricName = BridgeMetrics.this.Prefix :+ "sequencer_state"

    val keyStateSize: Histogram = registry.histogram(Prefix :+ "keys")
    val consumedContractsStateSize: Histogram = registry.histogram(Prefix :+ "consumed_contracts")
    val sequencerQueueLength: Histogram = registry.histogram(Prefix :+ "queue")
  }

  object InputQueue {
    val Prefix: MetricName = BridgeMetrics.this.Prefix :+ "input_queue"

    val conflictQueueCapacity: Counter = registry.counter(Prefix :+ "capacity")
    val conflictQueueLength: Counter = registry.counter(Prefix :+ "length")
    val conflictQueueDelay: Timer = registry.timer(Prefix :+ "delay")
  }
}
