// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.MetricHandle.{Counter, Histogram, Meter, Timer}

import com.codahale.metrics.MetricRegistry

class ExecutionMetrics(override val prefix: MetricName, override val registry: MetricRegistry)
    extends MetricHandle.Factory {
  val lookupActiveContract: Timer = timer(prefix :+ "lookup_active_contract")
  val lookupActiveContractPerExecution: Timer =
    timer(prefix :+ "lookup_active_contract_per_execution")
  val lookupActiveContractCountPerExecution: Histogram =
    histogram(prefix :+ "lookup_active_contract_count_per_execution")
  val lookupContractKey: Timer = timer(prefix :+ "lookup_contract_key")
  val lookupContractKeyPerExecution: Timer =
    timer(prefix :+ "lookup_contract_key_per_execution")
  val lookupContractKeyCountPerExecution: Histogram =
    histogram(prefix :+ "lookup_contract_key_count_per_execution")
  val getLfPackage: Timer = timer(prefix :+ "get_lf_package")
  val retry: Meter = meter(prefix :+ "retry")

  // Total time for command execution (including data fetching)
  val total: Timer = timer(prefix :+ "total")
  val totalRunning: Meter = meter(prefix :+ "total_running")

  // Commands being executed by the engine (not currently fetching data)
  val engine: Timer = timer(prefix :+ "engine")
  val engineRunning: Meter = meter(prefix :+ "engine_running")

  object cache extends MetricHandle.Factory {
    override val prefix: MetricName = ExecutionMetrics.this.prefix :+ "cache"
    override val registry = ExecutionMetrics.this.registry

    val keyState: CacheMetrics = new CacheMetrics(prefix :+ "key_state", registry)
    val contractState: CacheMetrics = new CacheMetrics(prefix :+ "contract_state", registry)

    val registerCacheUpdate: Timer = timer(prefix :+ "register_update")

    val resolveDivulgenceLookup: Counter =
      counter(prefix :+ "resolve_divulgence_lookup")

    val resolveFullLookup: Counter =
      counter(prefix :+ "resolve_full_lookup")

    val readThroughNotFound: Counter = counter(prefix :+ "read_through_not_found")
  }
}
