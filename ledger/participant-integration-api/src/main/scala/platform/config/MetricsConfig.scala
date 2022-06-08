// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.config

import com.daml.metrics.MetricsReporter
import com.daml.platform.config.MetricsConfig.MetricRegistryType

import scala.concurrent.duration.{Duration, _}

final case class MetricsConfig(
    reporter: Option[MetricsReporter] = None,
    reportingInterval: Duration = 10.seconds,
    registryType: MetricRegistryType = MetricRegistryType.JvmShared,
)

object MetricsConfig {
  sealed trait MetricRegistryType
  object MetricRegistryType {
    case object JvmShared extends MetricRegistryType
    case object New extends MetricRegistryType
  }
  val DefaultMetricsConfig: MetricsConfig = MetricsConfig()
}
