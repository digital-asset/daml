// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.testing.InMemoryMetricsFactory

import scala.collection.concurrent.TrieMap

class ScopedInMemoryMetricsFactory extends MetricsFactoryProvider {

  private val factories = TrieMap[MetricsContext, InMemoryMetricsFactory]()

  override def generateMetricsFactory(metricsContext: MetricsContext): InMemoryMetricsFactory = {
    factories.getOrElseUpdate(metricsContext, new InMemoryMetricsFactory)
  }

  def findSingle(condition: MetricsContext => Boolean): InMemoryMetricsFactory = {
    val filteredFactories = factories.toMap.view.filter { case (context, _) =>
      condition(context)
    }.values
    if (filteredFactories.sizeIs == 1) filteredFactories.head
    else
      throw new IllegalArgumentException(
        s"Cannot find factory for $factories. Filtered down to $filteredFactories"
      )
  }

}
