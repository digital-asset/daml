// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.HistogramInventory.Item
import com.daml.metrics.api.MetricHandle.{LabeledMetricsFactory, Timer}
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricQualification}

class ContractStoreHistograms(val prefix: MetricName)(implicit
    inventory: HistogramInventory
) {
  val lookupPersisted: Item = Item(
    prefix :+ "lookup_persisted",
    summary = "The time to lookup persisted contract by LF contract id.",
    description =
      "The time to enqueue and execute the lookup for persisted contract by LF contract id.",
    qualification = MetricQualification.Debug,
  )
  val lookupBatched: Item = Item(
    prefix :+ "lookup_batched",
    summary = "The time to execute batched contract lookup.",
    description = "The time to enqueue and execute batched contract lookup.",
    qualification = MetricQualification.Debug,
  )
  val lookupBatchedContractIds: Item = Item(
    prefix :+ "lookup_batched_contract_ids",
    summary = "The time to execute batched contract id lookup.",
    description = "The time to enqueue and execute batched contract id lookup.",
    qualification = MetricQualification.Debug,
  )
  val lookupBatchedInternalIds: Item = Item(
    prefix :+ "lookup_batched_internal_ids",
    summary = "The time to execute batched internal id lookup.",
    description = "The time to enqueue and execute batched internal id lookup.",
    qualification = MetricQualification.Debug,
  )
}

class ContractStoreMetrics private[metrics] (
    inventory: ContractStoreHistograms,
    factory: LabeledMetricsFactory,
) {
  val lookupPersisted: Timer = factory.timer(inventory.lookupPersisted.info)
  val lookupBatched: Timer = factory.timer(inventory.lookupBatched.info)
  val lookupBatchedContractIds: Timer = factory.timer(inventory.lookupBatchedContractIds.info)
  val lookupBatchedInternalIds: Timer = factory.timer(inventory.lookupBatchedInternalIds.info)
}
