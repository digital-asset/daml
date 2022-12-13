// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.metrics.api.MetricName
import com.daml.metrics.api.MetricHandle.Factory

class PruningMetrics(prefix: MetricName, factory: Factory) {
  // Using a meter, which can keep track of how many time the operation was executed, even if the
  // operation is fully executed between 2 metric fetches by the monitoring.
  // With a (boolean) gauge, there is a large risk that some operation invocation would be missed.
  val pruneStarted = factory.meter(prefix :+ "prune" :+ "started")
  val pruneCompleted = factory.meter(prefix :+ "prune" :+ "completed")
}
