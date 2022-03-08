// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox.bridge

import com.daml.metrics.{MetricName}
import io.prometheus.client
import com.daml.metrics.{gauge, summary}
import io.prometheus.client.{Gauge, Summary}

class BridgeMetrics() {
//  val registry: MetricRegistry = metrics.registry

  val Prefix: MetricName = MetricName.Daml :+ "sandbox_ledger_bridge"

  val threadpool: MetricName = Prefix :+ "threadpool"

  object Stages {
    val Prefix: MetricName = BridgeMetrics.this.Prefix :+ "stages"

    case class StageMetrics(stageName: String) {
      protected val prefix: MetricName = Stages.this.Prefix :+ stageName
      val timer: Summary = summary(prefix :+ "timer")
      val bufferBefore: Gauge = gauge(prefix :+ "buffer")
    }

    object PrepareSubmission extends StageMetrics("prepare_submission")
    object TagWithLedgerEnd extends StageMetrics("tag_with_ledger_end")
    object ConflictCheckWithCommitted extends StageMetrics("conflict_check_with_committed")
    object Sequence extends StageMetrics("sequence") {
      val statePrefix: MetricName = prefix :+ "state"
      val keyStateSize: Summary = summary(statePrefix :+ "keys")
      val consumedContractsStateSize: Summary =
        summary(statePrefix :+ "consumed_contracts")
      val sequencerQueueLength: Summary = summary(statePrefix :+ "queue")
      val deduplicationQueueLength: Summary =
        summary(statePrefix :+ "deduplication_queue")
    }
  }

  object BridgeInputQueue {
    val Prefix: MetricName = BridgeMetrics.this.Prefix :+ "input_queue"

    val conflictQueueCapacity: client.Gauge = gauge(Prefix :+ "capacity")
    val conflictQueueLength: client.Gauge = gauge(Prefix :+ "length")
    val conflictQueueDelay: client.Summary = summary(Prefix :+ "delay")
  }
}
