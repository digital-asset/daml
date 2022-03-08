// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}

// TODO Prometheus metrics: notes:
// - no slf4j reporter
// - reporting to the global registry as in best practices
// - JMX reporter to implement
final class MetricsReporting(
    extraMetricsReporter: Option[MetricsReporter]
) extends ResourceOwner[Metrics] {
  def acquire()(implicit context: ResourceContext): Resource[Metrics] = {
    JvmMetrics.initialize()
    for {
      _ <- extraMetricsReporter.fold(Resource.unit) { reporter =>
        MetricsReporter.owner(reporter).acquire()
      }
    } yield new Metrics(null) // TODO Prometheus metrics
  }
}
