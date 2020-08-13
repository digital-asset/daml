// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.codahale.metrics.MetricRegistry.MetricSupplier

package object metrics {

  private[metrics] def registerGauge[T](
      name: MetricName,
      gaugeSupplier: MetricSupplier[Gauge[_]],
      registry: MetricRegistry,
  ): Gauge[T] =
    registry.synchronized {
      registry.remove(name)
      registry.gauge(name, gaugeSupplier).asInstanceOf[Gauge[T]]
    }

}
