// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import com.codahale.metrics.MetricRegistry
import com.daml.metrics.Metrics
import com.daml.metrics.api.MetricHandle.{Counter, Histogram, Timer}
import com.daml.metrics.api.MetricName
import com.daml.metrics.api.dropwizard.DropwizardFactory

class BridgeMetrics(metrics: Metrics) extends DropwizardFactory {

  override val registry: MetricRegistry = metrics.registry

  val prefix: MetricName = MetricName.Daml :+ "sandbox_ledger_bridge"

  val threadpool: MetricName = prefix :+ "threadpool"

  object Stages {
    val prefix: MetricName = BridgeMetrics.this.prefix :+ "stages"

    case class StageMetrics(stageName: String) {
      protected val prefix: MetricName = Stages.this.prefix :+ stageName
      val timer: Timer = BridgeMetrics.this.timer(prefix :+ "timer")
      val bufferBefore: Counter = counter(prefix :+ "buffer")
    }

    object PrepareSubmission extends StageMetrics("prepare_submission")
    object TagWithLedgerEnd extends StageMetrics("tag_with_ledger_end")
    object ConflictCheckWithCommitted extends StageMetrics("conflict_check_with_committed")
    object Sequence extends StageMetrics("sequence") {
      val statePrefix: MetricName = prefix :+ "state"
      val keyStateSize: Histogram = histogram(statePrefix :+ "keys")
      val consumedContractsStateSize: Histogram =
        histogram(statePrefix :+ "consumed_contracts")
      val sequencerQueueLength: Histogram = histogram(statePrefix :+ "queue")
      val deduplicationQueueLength: Histogram =
        histogram(statePrefix :+ "deduplication_queue")
    }
  }

  object BridgeInputQueue {
    val prefix: MetricName = BridgeMetrics.this.prefix :+ "input_queue"

    val conflictQueueCapacity: Counter = counter(prefix :+ "capacity")
    val conflictQueueLength: Counter = counter(prefix :+ "length")
    val conflictQueueDelay: Timer = timer(prefix :+ "delay")
  }
}
