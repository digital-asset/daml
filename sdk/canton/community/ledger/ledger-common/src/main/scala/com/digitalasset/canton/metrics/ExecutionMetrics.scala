// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.CacheMetrics
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.{HistogramInventory, MetricInfo, MetricName, MetricQualification}
import com.daml.metrics.api.HistogramInventory.Item

class ExecutionHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {

  private[metrics] val lookupActiveContract: Item = Item(
    prefix :+ "lookup_active_contract",
    summary = "The time to lookup individual active contracts during interpretation.",
    description = """The interpretation of a command in the ledger api server might require
                        |fetching multiple active contracts. This metric exposes the time to lookup
                        |individual active contracts.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val lookupActiveContractPerExecution: Item = Item(
    prefix :+ "lookup_active_contract_per_execution",
    summary = "The compound time to lookup all active contracts in a single Daml command.",
    description = """The interpretation of a command in the ledger api server might require
                        |fetching multiple active contracts. This metric exposes the compound time to
                        |lookup all the active contracts in a single Daml command.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val lookupActiveContractCountPerExecution: Item = Item(
    prefix :+ "lookup_active_contract_count_per_execution",
    summary = "The number of the active contracts looked up per Daml command.",
    description = """The interpretation of a command in the ledger api server might require
                        |fetching multiple active contracts. This metric exposes the number of active
                        |contracts that must be looked up to process a Daml command.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val lookupContractKey: Item = Item(
    prefix :+ "lookup_contract_key",
    summary = "The time to lookup individual contract keys during interpretation.",
    description = """The interpretation of a command in the ledger api server might require
                      |fetching multiple contract keys. This metric exposes the time needed to lookup
                      |individual contract keys.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val lookupContractKeyPerExecution: Item = Item(
    prefix :+ "lookup_contract_key_per_execution",
    summary = "The compound time to lookup all contract keys in a single Daml command.",
    description = """The interpretation of a command in the ledger api server might require
                        |fetching multiple contract keys. This metric exposes the compound time needed
                        |to lookup all the contract keys in a single Daml command.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val lookupContractKeyCountPerExecution: Item = Item(
    prefix :+ "lookup_contract_key_count_per_execution",
    summary = "The number of contract keys looked up per Daml command.",
    description = """The interpretation of a command in the ledger api server might require
                        |fetching multiple contract keys. This metric exposes the number of contract
                        |keys that must be looked up to process a Daml command.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val getLfPackage: Item = Item(
    prefix :+ "get_lf_package",
    summary = "The time to fetch individual Daml code packages during interpretation.",
    description = """The interpretation of a command in the ledger api server might require
                      |fetching multiple Daml packages. This metric exposes the time needed to fetch
                      |the packages that are necessary for interpretation.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val total: Item = Item(
    prefix :+ "total",
    summary = "The overall time spent interpreting a Daml command.",
    description = """The time spent interpreting a Daml command in the ledger api server (includes
                      |executing Daml and fetching data).""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val engine: Item = Item(
    prefix :+ "engine",
    summary = "The time spent executing a Daml command.",
    description = """The time spent by the Daml engine executing a Daml command (excluding fetching
          |data).""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val cachePrefix: MetricName = ExecutionHistograms.this.prefix :+ "cache"

  private[metrics] val registerKeyStateCacheUpdate: Item = Item(
    cachePrefix :+ "key_state" :+ "register_update",
    summary = "The time spent to update the cache.",
    description = """The total time spent in sequential update steps of the contract state caches
                |updating logic. This metric is created with debugging purposes in mind.""",
    qualification = MetricQualification.Debug,
  )

  private[metrics] val registerContractStateCacheUpdate: Item = Item(
    cachePrefix :+ "contract_state" :+ "register_update",
    summary = "The time spent to update the cache.",
    description = """The total time spent in sequential update steps of the contract state caches
                |updating logic. This metric is created with debugging purposes in mind.""",
    qualification = MetricQualification.Debug,
  )

}

class ExecutionMetrics(
    inventory: ExecutionHistograms,
    openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

  import com.daml.metrics.api.MetricsContext.Implicits.empty
  private val prefix = inventory.prefix

  val lookupActiveContract: Timer =
    openTelemetryMetricsFactory.timer(inventory.lookupActiveContract.info)

  val lookupActiveContractPerExecution: Timer =
    openTelemetryMetricsFactory.timer(inventory.lookupActiveContractPerExecution.info)

  val lookupActiveContractCountPerExecution: Histogram =
    openTelemetryMetricsFactory.histogram(inventory.lookupActiveContractCountPerExecution.info)

  val lookupContractKey: Timer = openTelemetryMetricsFactory.timer(inventory.lookupContractKey.info)

  val lookupContractKeyPerExecution: Timer =
    openTelemetryMetricsFactory.timer(inventory.lookupContractKeyPerExecution.info)

  val lookupContractKeyCountPerExecution: Histogram =
    openTelemetryMetricsFactory.histogram(inventory.lookupContractKeyCountPerExecution.info)

  val getLfPackage: Timer = openTelemetryMetricsFactory.timer(inventory.getLfPackage.info)

  val retry: Meter = openTelemetryMetricsFactory.meter(
    MetricInfo(
      prefix :+ "retry",
      summary = "The number of the interpretation retries.",
      description =
        """The total number of interpretation retries attempted due to mismatching ledger
                    |effective time in this ledger api server process.""",
      qualification = MetricQualification.Debug,
    )
  )

  val total: Timer = openTelemetryMetricsFactory.timer(inventory.total.info)

  val totalRunning: Counter = openTelemetryMetricsFactory.counter(
    MetricInfo(
      prefix :+ "total_running",
      summary = "The number of Daml commands currently being interpreted.",
      description = """The number of the commands that are currently being interpreted (includes
                    |executing Daml code and fetching data).""",
      qualification = MetricQualification.Debug,
    )
  )

  val engine: Timer = openTelemetryMetricsFactory.timer(inventory.engine.info)

  val engineRunning: Counter = openTelemetryMetricsFactory.counter(
    MetricInfo(
      prefix :+ "engine_running",
      summary = "The number of Daml commands currently being executed.",
      description = """The number of the commands that are currently being executed by the Daml
                    |engine (excluding fetching data).""",
      qualification = MetricQualification.Debug,
    )
  )

  object cache {
    val prefix: MetricName = inventory.cachePrefix

    object keyState {
      val stateCache: CacheMetrics =
        new CacheMetrics(prefix :+ "key_state", openTelemetryMetricsFactory)

      val registerCacheUpdate: Timer =
        openTelemetryMetricsFactory.timer(
          MetricInfo(
            prefix :+ "key_state" :+ "register_update",
            summary = "The time spent to update the cache.",
            description =
              """The total time spent in sequential update steps of the contract state caches
              |updating logic. This metric is created with debugging purposes in mind.""",
            qualification = MetricQualification.Debug,
          )
        )
    }

    object contractState {
      val stateCache: CacheMetrics =
        new CacheMetrics(prefix :+ "contract_state", openTelemetryMetricsFactory)

      val registerCacheUpdate: Timer =
        openTelemetryMetricsFactory.timer(
          MetricInfo(
            prefix :+ "contract_state" :+ "register_update",
            summary = "The time spent to update the cache.",
            description =
              """The total time spent in sequential update steps of the contract state caches
              |updating logic. This metric is created with debugging purposes in mind.""",
            qualification = MetricQualification.Debug,
          )
        )
    }

  }
}
