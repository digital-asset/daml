// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.CacheMetrics
import com.daml.metrics.api.MetricDoc.MetricQualification.Debug
import com.daml.metrics.api.MetricHandle.*
import com.daml.metrics.api.{MetricDoc, MetricName}

class ExecutionMetrics(
    prefix: MetricName,
    dropWizardMetricsFactory: LabeledMetricsFactory,
    openTelemetryMetricsFactory: LabeledMetricsFactory,
) {

  @MetricDoc.Tag(
    summary = "The time to lookup individual active contracts during interpretation.",
    description = """The interpretation of a command in the ledger api server might require
                    |fetching multiple active contracts. This metric exposes the time to lookup
                    |individual active contracts.""",
    qualification = Debug,
  )
  val lookupActiveContract: Timer =
    dropWizardMetricsFactory.timer(prefix :+ "lookup_active_contract")

  @MetricDoc.Tag(
    summary = "The compound time to lookup all active contracts in a single Daml command.",
    description = """The interpretation of a command in the ledger api server might require
                    |fetching multiple active contracts. This metric exposes the compound time to
                    |lookup all the active contracts in a single Daml command.""",
    qualification = Debug,
  )
  val lookupActiveContractPerExecution: Timer =
    dropWizardMetricsFactory.timer(prefix :+ "lookup_active_contract_per_execution")

  @MetricDoc.Tag(
    summary = "The number of the active contracts looked up per Daml command.",
    description = """The interpretation of a command in the ledger api server might require
                    |fetching multiple active contracts. This metric exposes the number of active
                    |contracts that must be looked up to process a Daml command.""",
    qualification = Debug,
  )
  val lookupActiveContractCountPerExecution: Histogram =
    dropWizardMetricsFactory.histogram(prefix :+ "lookup_active_contract_count_per_execution")

  @MetricDoc.Tag(
    summary = "The time to lookup individual contract keys during interpretation.",
    description = """The interpretation of a command in the ledger api server might require
                    |fetching multiple contract keys. This metric exposes the time needed to lookup
                    |individual contract keys.""",
    qualification = Debug,
  )
  val lookupContractKey: Timer = dropWizardMetricsFactory.timer(prefix :+ "lookup_contract_key")

  @MetricDoc.Tag(
    summary = "The compound time to lookup all contract keys in a single Daml command.",
    description = """The interpretation of a command in the ledger api server might require
                    |fetching multiple contract keys. This metric exposes the compound time needed
                    |to lookup all the contract keys in a single Daml command.""",
    qualification = Debug,
  )
  val lookupContractKeyPerExecution: Timer =
    dropWizardMetricsFactory.timer(prefix :+ "lookup_contract_key_per_execution")

  @MetricDoc.Tag(
    summary = "The number of contract keys looked up per Daml command.",
    description = """The interpretation of a command in the ledger api server might require
                    |fetching multiple contract keys. This metric exposes the number of contract
                    |keys that must be looked up to process a Daml command.""",
    qualification = Debug,
  )
  val lookupContractKeyCountPerExecution: Histogram =
    dropWizardMetricsFactory.histogram(prefix :+ "lookup_contract_key_count_per_execution")

  @MetricDoc.Tag(
    summary = "The time to fetch individual Daml code packages during interpretation.",
    description = """The interpretation of a command in the ledger api server might require
                    |fetching multiple Daml packages. This metric exposes the time needed to fetch
                    |the packages that are necessary for interpretation.""",
    qualification = Debug,
  )
  val getLfPackage: Timer = dropWizardMetricsFactory.timer(prefix :+ "get_lf_package")

  @MetricDoc.Tag(
    summary = "The number of the interpretation retries.",
    description = """The total number of interpretation retries attempted due to mismatching ledger
                    |effective time in this ledger api server process.""",
    qualification = Debug,
  )
  val retry: Meter = dropWizardMetricsFactory.meter(prefix :+ "retry")

  @MetricDoc.Tag(
    summary = "The overall time spent interpreting a Daml command.",
    description = """The time spent interpreting a Daml command in the ledger api server (includes
                    |executing Daml and fetching data).""",
    qualification = Debug,
  )
  val total: Timer = dropWizardMetricsFactory.timer(prefix :+ "total")

  @MetricDoc.Tag(
    summary = "The number of Daml commands currently being interpreted.",
    description = """The number of the commands that are currently being interpreted (includes
                    |executing Daml code and fetching data).""",
    qualification = Debug,
  )
  val totalRunning: Counter = dropWizardMetricsFactory.counter(prefix :+ "total_running")

  @MetricDoc.Tag(
    summary = "The time spent executing a Daml command.",
    description = """The time spent by the Daml engine executing a Daml command (excluding fetching
                    |data).""",
    qualification = Debug,
  )
  val engine: Timer = dropWizardMetricsFactory.timer(prefix :+ "engine")

  @MetricDoc.Tag(
    summary = "The number of Daml commands currently being executed.",
    description = """The number of the commands that are currently being executed by the Daml
                    |engine (excluding fetching data).""",
    qualification = Debug,
  )
  val engineRunning: Counter = dropWizardMetricsFactory.counter(prefix :+ "engine_running")

  object cache {
    val prefix: MetricName = ExecutionMetrics.this.prefix :+ "cache"

    object keyState {
      val stateCache: CacheMetrics =
        new CacheMetrics(prefix :+ "key_state", openTelemetryMetricsFactory)

      @MetricDoc.Tag(
        summary = "The time spent to update the cache.",
        description =
          """The total time spent in sequential update steps of the contract state caches
                        |updating logic. This metric is created with debugging purposes in mind.""",
        qualification = Debug,
      )
      val registerCacheUpdate: Timer =
        dropWizardMetricsFactory.timer(prefix :+ "key_state" :+ "register_update")
    }

    object contractState {
      val stateCache: CacheMetrics =
        new CacheMetrics(prefix :+ "contract_state", openTelemetryMetricsFactory)

      @MetricDoc.Tag(
        summary = "The time spent to update the cache.",
        description =
          """The total time spent in sequential update steps of the contract state caches
                        |updating logic. This metric is created with debugging purposes in mind.""",
        qualification = Debug,
      )
      val registerCacheUpdate: Timer =
        dropWizardMetricsFactory.timer(prefix :+ "contract_state" :+ "register_update")
    }

    @MetricDoc.Tag(
      summary =
        "The number of lookups trying to resolve divulged contracts on active contracts cache hits.",
      description = """Divulged contracts are not cached in the contract state caches. On active
                      |contract cache hits, where stakeholders are not within the submission readers,
                      |a contract activeness lookup is performed against the Index database. On such
                      |lookups, this counter is incremented.""",
      qualification = Debug,
    )
    val resolveDivulgenceLookup: Counter =
      dropWizardMetricsFactory.counter(prefix :+ "resolve_divulgence_lookup")

    @MetricDoc.Tag(
      summary =
        "The number of lookups trying to resolve divulged contracts on archived contracts cache hits.",
      description = """Divulged contracts are not cached in the contract state caches. On archived
                      |contract cache hits, where stakeholders are not within the submission readers,
                      |a full contract activeness lookup (including fetching contract arguments) is
                      |performed against the Index database. On such lookups, this counter is incremented.""",
      qualification = Debug,
    )
    val resolveFullLookup: Counter =
      dropWizardMetricsFactory.counter(prefix :+ "resolve_full_lookup")

    @MetricDoc.Tag(
      summary = "The number of cache read-throughs resulting in not found contracts.",
      description =
        """On cache misses, a read-through query is performed against the Index database.
          |When the contract is not found (as result of this query), this counter is
          |incrmented.""",
      qualification = Debug,
    )
    val readThroughNotFound: Counter =
      dropWizardMetricsFactory.counter(prefix :+ "read_through_not_found")
  }
}
