// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.metrics

import com.daml.metrics.api.MetricHandle.Factory
import com.daml.metrics.http.DamlHttpMetrics

case class TriggerServiceMetrics(metricsFactory: Factory) {

  val http = new DamlHttpMetrics(metricsFactory, "trigger-service")

}
