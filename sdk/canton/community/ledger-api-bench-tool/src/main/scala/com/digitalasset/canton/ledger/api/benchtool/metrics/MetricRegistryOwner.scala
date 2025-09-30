// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.benchtool.metrics

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import io.opentelemetry.api.metrics.MeterProvider

class MetricRegistryOwner() extends ResourceOwner[MeterProvider] {
  override def acquire()(implicit
      context: ResourceContext
  ): Resource[MeterProvider] =
    ResourceOwner.forCloseable(() => metricOwner).acquire()

  private def metricOwner =
    throw new NotImplementedError(
      "LogReporter only exists in canton, not in observablilty. Need to move it up first"
    )

}
